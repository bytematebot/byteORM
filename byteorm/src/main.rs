use byteorm_lib::*;
use clap::{CommandFactory, Parser, ValueEnum};
use clap_complete::{Shell, generate};
use std::path::{Path, PathBuf};
use std::{env, fs};

include!(concat!(env!("OUT_DIR"), "/embedded_macros.rs"));

#[derive(Parser)]
#[command(name = "byte")]
#[command(about = "ByteORM CLI - Database schema management", long_about = None)]
struct Cli {
    /// Path to byteorm.toml
    #[arg(long, global = true)]
    config: Option<PathBuf>,
    /// Use a single schema file
    #[arg(long, global = true)]
    schema: Option<PathBuf>,
    /// Use all .bo files from a schema directory
    #[arg(long = "schema-dir", global = true)]
    schema_dir: Option<PathBuf>,
    /// Generate byteorm-client into this directory
    #[arg(long, global = true)]
    output: Option<PathBuf>,
    #[command(subcommand)]
    command: Option<Commands>,
}

#[derive(clap::Subcommand)]
enum Commands {
    /// Initialize ByteORM config and starter files
    Init,
    /// Push schema to database, run migrations, and generate the byteorm-client crate
    Push {
        /// Print migration SQL and generate the client without executing SQL
        #[arg(long)]
        dry_run: bool,
        /// Allow destructive changes such as dropping tables, columns, or enums
        #[arg(long)]
        accept_data_loss: bool,
    },
    /// Drop all database tables and reset state (dangerous!)
    Reset,
    /// Update ByteORM to the latest version from GitHub
    SelfUpdate,
    /// Generate byteorm-client crate from schema without DB connection
    Generate,
    /// Show resolved config, schema files, and output paths
    Doctor,
    /// Repair database: add missing constraints (unique, indexes) from schema
    Repair,
    /// Generate shell autocomplete scripts
    Completions {
        /// Shell to generate completions for, or "install" to install for the detected shell
        #[arg(value_enum)]
        target: Option<CompletionTarget>,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
enum CompletionTarget {
    Bash,
    Zsh,
    Fish,
    Powershell,
    Elvish,
    Install,
}

impl CompletionTarget {
    fn as_clap_shell(self) -> Option<Shell> {
        match self {
            Self::Bash => Some(Shell::Bash),
            Self::Zsh => Some(Shell::Zsh),
            Self::Fish => Some(Shell::Fish),
            Self::Powershell => Some(Shell::PowerShell),
            Self::Elvish => Some(Shell::Elvish),
            Self::Install => None,
        }
    }

    fn as_name(self) -> &'static str {
        match self {
            Self::Bash => "bash",
            Self::Zsh => "zsh",
            Self::Fish => "fish",
            Self::Powershell => "powershell",
            Self::Elvish => "elvish",
            Self::Install => "install",
        }
    }
}

#[derive(Debug, Clone, Default)]
struct ProjectConfig {
    schema: SchemaConfig,
    client: ClientConfig,
}

#[derive(Debug, Clone, Default)]
struct SchemaConfig {
    path: Option<PathBuf>,
    directory: Option<PathBuf>,
}

#[derive(Debug, Clone, Default)]
struct ClientConfig {
    output: Option<PathBuf>,
    crate_name: Option<String>,
    dependency_source: Option<DependencySource>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DependencySource {
    Vendored,
}

#[derive(Debug, Clone)]
struct ProjectPaths {
    root: PathBuf,
    config_path: Option<PathBuf>,
    schema_files: Vec<PathBuf>,
    schema_source: String,
    client_output: PathBuf,
    crate_name: String,
    dependency_source: DependencySource,
}

#[tokio::main]
async fn main() {
    dotenvy::dotenv().ok();

    let cli = Cli::parse();

    match cli.command {
        Some(Commands::Init) => {
            if let Err(e) = init_project(&cli) {
                eprintln!("Error initializing ByteORM: {}", e);
            }
        }
        Some(Commands::Push {
            dry_run,
            accept_data_loss,
        }) => {
            println!("Pushing schema...");

            let project = match resolve_project(&cli) {
                Ok(project) => project,
                Err(e) => {
                    eprintln!("Configuration error: {}", e);
                    print_schema_help();
                    return;
                }
            };

            if project.schema_files.is_empty() {
                eprintln!("No schema files found!");
                print_schema_help();
                return;
            }

            println!("Using schema: {}", project.schema_source);
            println!("Found {} schema file(s):", project.schema_files.len());
            for file in &project.schema_files {
                println!("   - {}", file.display());
            }

            let schema = match load_and_merge_schemas(&project.schema_files) {
                Ok(s) => s,
                Err(e) => {
                    eprintln!("Error loading schemas: {}", e);
                    return;
                }
            };

            println!("Loaded {} models", schema.models.len());

            let client = match db::connect().await {
                Ok(c) => c,
                Err(e) => {
                    eprintln!("Database connection error: {}", e);
                    return;
                }
            };

            if let Err(e) = snapshot::init_snapshot_table(&client).await {
                eprintln!("Error initializing snapshot table: {}", e);
                return;
            }

            let existing_tables = match db::get_existing_tables(&client).await {
                Ok(tables) => tables,
                Err(e) => {
                    eprintln!("Error getting existing tables: {}", e);
                    vec![]
                }
            };

            let previous = match snapshot::load_snapshot(&client).await.unwrap_or(None) {
                Some(mut prev_schema) => {
                    let before_count = prev_schema.models.len();
                    prev_schema.models.retain(|m| {
                        existing_tables
                            .iter()
                            .any(|t| t.eq_ignore_ascii_case(&m.name))
                    });
                    let removed = before_count - prev_schema.models.len();
                    if removed > 0 {
                        println!(
                            "⚠️  Removed {} model(s) from snapshot (tables don't exist in DB)",
                            removed
                        );
                    }
                    Some(prev_schema)
                }
                None => None,
            };

            let changes = diff::diff_schemas(previous.as_ref(), &schema);

            println!("\nChanges detected:");
            for change in &changes {
                println!("  - {:?}", change);
            }

            if !accept_data_loss && changes.iter().any(is_destructive_change) {
                eprintln!("\nDestructive changes detected.");
                eprintln!("Re-run with --accept-data-loss to drop tables, columns, or enum types.");
                return;
            }

            let sql = codegen::generate_migration_sql(&changes);
            println!("\nGenerated SQL:\n{}", sql);

            if dry_run {
                println!("\nDry run enabled; SQL was not executed and snapshot was not updated.");
                println!("\nGenerating byteorm-client crate...");
                if let Err(e) = generate_client_package(&schema, &project) {
                    eprintln!("Error generating client: {}", e);
                }
                return;
            }

            println!("\n⚙️  Executing SQL...");
            let executed = match db::execute_sql(&client, &sql).await {
                Ok(_) => true,
                Err(e) => {
                    eprintln!("❌ Error: {}", e);
                    // Try to downcast to tokio_postgres::Error to get more details
                    if let Some(db_err) = e.downcast_ref::<tokio_postgres::Error>() {
                        if let Some(sql_state) = db_err.code() {
                            eprintln!("  📍 Error details:");
                            eprintln!("     Code: {:?}", sql_state);
                            eprintln!("     Message: {}", db_err);
                        }
                    }
                    eprintln!("Error executing SQL: {}", e);
                    eprintln!("Continuing to generate client from schema only.");
                    false
                }
            };

            if executed {
                if let Err(e) = snapshot::save_snapshot(&client, &schema).await {
                    eprintln!("Error saving snapshot: {}", e);
                    return;
                }
            }

            println!("\nGenerating byteorm-client crate...");
            if let Err(e) = generate_client_package(&schema, &project) {
                eprintln!("Error generating client: {}", e);
                return;
            }

            println!("\nPush complete!");
            println!("Add to your Cargo.toml:");
            println!("   [dependencies]");
            println!(
                "   {} = {{ path = \"{}\" }}",
                project.crate_name,
                project.client_output.display()
            );
        }
        Some(Commands::Reset) => {
            println!("Resetting database...");

            let client = match db::connect().await {
                Ok(c) => c,
                Err(e) => {
                    eprintln!("Database connection error: {}", e);
                    return;
                }
            };

            let project = match resolve_project(&cli) {
                Ok(project) => project,
                Err(e) => {
                    eprintln!("Configuration error: {}", e);
                    print_schema_help();
                    return;
                }
            };
            if project.schema_files.is_empty() {
                eprintln!("No schema files found!");
                print_schema_help();
                return;
            }
            let schema = match load_and_merge_schemas(&project.schema_files) {
                Ok(s) => s,
                Err(e) => {
                    eprintln!("Error loading schemas: {}", e);
                    return;
                }
            };

            // Drop tables
            if let Err(e) = db::reset_database(&client, &schema).await {
                eprintln!("Error resetting database: {}", e);
                return;
            }

            println!("Database reset complete!");
        }
        Some(Commands::SelfUpdate) => {
            if let Err(e) = self_update().await {
                eprintln!("Update failed: {}", e);
                return;
            }
        }
        Some(Commands::Generate) => {
            println!("🔍 Generating client code...");
            let project = match resolve_project(&cli) {
                Ok(project) => project,
                Err(e) => {
                    eprintln!("Configuration error: {}", e);
                    print_schema_help();
                    return;
                }
            };

            if project.schema_files.is_empty() {
                eprintln!("No schema files found!");
                print_schema_help();
                return;
            }

            println!("Using schema: {}", project.schema_source);
            println!("Generating client in: {}", project.client_output.display());

            let schema = match load_and_merge_schemas(&project.schema_files) {
                Ok(s) => s,
                Err(e) => {
                    eprintln!("Error loading schemas: {}", e);
                    return;
                }
            };

            if let Err(e) = generate_client_package(&schema, &project) {
                eprintln!("Error generating client: {}", e);
                return;
            }
            println!("✅ byteorm-client generated successfully!");
        }
        Some(Commands::Doctor) => match resolve_project(&cli) {
            Ok(project) => print_doctor(&project),
            Err(e) => {
                eprintln!("Configuration error: {}", e);
                print_schema_help();
            }
        },
        Some(Commands::Completions { target }) => {
            if let Err(e) = handle_completions(target) {
                eprintln!("Autocomplete error: {}", e);
            }
        }
        Some(Commands::Repair) => {
            println!("🔧 Repairing database...");

            let project = match resolve_project(&cli) {
                Ok(project) => project,
                Err(e) => {
                    eprintln!("Configuration error: {}", e);
                    print_schema_help();
                    return;
                }
            };
            if project.schema_files.is_empty() {
                eprintln!("No schema files found!");
                print_schema_help();
                return;
            }

            let schema = match load_and_merge_schemas(&project.schema_files) {
                Ok(s) => s,
                Err(e) => {
                    eprintln!("Error loading schemas: {}", e);
                    return;
                }
            };

            let client = match db::connect().await {
                Ok(c) => c,
                Err(e) => {
                    eprintln!("Database connection error: {}", e);
                    return;
                }
            };

            let existing_constraints: Vec<String> = match client
                .query(
                    "SELECT conname FROM pg_constraint WHERE connamespace = (SELECT oid FROM pg_namespace WHERE nspname = 'public')",
                    &[],
                )
                .await
            {
                Ok(rows) => rows.iter().map(|r| r.get::<_, String>(0)).collect(),
                Err(e) => {
                    eprintln!("Error querying constraints: {}", e);
                    return;
                }
            };

            let existing_indexes: Vec<String> = match client
                .query(
                    "SELECT indexname FROM pg_indexes WHERE schemaname = 'public'",
                    &[],
                )
                .await
            {
                Ok(rows) => rows.iter().map(|r| r.get::<_, String>(0)).collect(),
                Err(e) => {
                    eprintln!("Error querying indexes: {}", e);
                    return;
                }
            };

            let mut sql_statements: Vec<String> = Vec::new();

            for model in &schema.models {
                let table_name = model.name.to_lowercase();

                for field in &model.fields {
                    if field.modifiers.iter().any(|m| matches!(m, Modifier::Index)) {
                        let field_name = field.name.to_lowercase();
                        let index_name = format!("idx_{}_{}", table_name, field_name);
                        if !existing_indexes.iter().any(|i| i == &index_name) {
                            sql_statements.push(format!(
                                "CREATE INDEX IF NOT EXISTS {} ON {} ({});",
                                index_name, table_name, field_name
                            ));
                        }
                    }
                    if field
                        .modifiers
                        .iter()
                        .any(|m| matches!(m, Modifier::Unique))
                    {
                        let constraint_name =
                            format!("{}_{}_key", table_name, field.name.to_lowercase());
                        if !existing_constraints.iter().any(|c| c == &constraint_name) {
                            sql_statements.push(format!(
                                "ALTER TABLE {} ADD CONSTRAINT {} UNIQUE ({});",
                                table_name, constraint_name, field.name
                            ));
                        }
                    }
                }

                for attr in &model.model_attributes {
                    if let Some(args) = &attr.args {
                        let args_content =
                            args.trim_start_matches('(').trim_end_matches(')').trim();
                        if !(args_content.starts_with('[') && args_content.ends_with(']')) {
                            continue;
                        }
                        let fields: Vec<&str> = args_content[1..args_content.len() - 1]
                            .split(',')
                            .map(|s| s.trim())
                            .collect();
                        let fields_str = fields.join("_");

                        match attr.name.as_str() {
                            "unique" => {
                                let constraint_name = format!("uq_{}_{}", table_name, fields_str);
                                if !existing_constraints.iter().any(|c| c == &constraint_name) {
                                    let fields_sql = fields.join(", ");
                                    sql_statements.push(format!(
                                        "ALTER TABLE {} ADD CONSTRAINT {} UNIQUE ({});",
                                        table_name, constraint_name, fields_sql
                                    ));
                                }
                            }
                            "index" => {
                                let index_name = format!("idx_{}_{}", table_name, fields_str);
                                if !existing_indexes.iter().any(|i| i == &index_name) {
                                    let fields_sql = fields.join(", ");
                                    sql_statements.push(format!(
                                        "CREATE INDEX IF NOT EXISTS {} ON {} ({});",
                                        index_name, table_name, fields_sql
                                    ));
                                }
                            }
                            _ => {}
                        }
                    }
                }
            }

            if sql_statements.is_empty() {
                println!("✅ Database is in sync — no missing constraints or indexes.");
            } else {
                println!(
                    "Found {} missing constraint(s)/index(es):",
                    sql_statements.len()
                );
                for stmt in &sql_statements {
                    println!("  → {}", stmt);
                }
                let sql = sql_statements.join(" ");
                match db::execute_sql(&client, &sql).await {
                    Ok(_) => {
                        println!("✅ Repair complete!");
                    }
                    Err(e) => {
                        eprintln!("❌ Repair failed: {}", e);
                    }
                }
            }

            if let Err(e) = snapshot::init_snapshot_table(&client).await {
                eprintln!("Error initializing snapshot table: {}", e);
                return;
            }
            if let Err(e) = snapshot::save_snapshot(&client, &schema).await {
                eprintln!("Error saving snapshot: {}", e);
            }
        }
        None => {
            println!("ByteORM CLI v0.1.0");
            println!("Commands:");
            println!("  init        - Initialize byteorm.toml and starter schema files");
            println!(
                "  push        - Push schema, run migrations, and generate byteorm-client crate"
            );
            println!("  generate    - Generate byteorm-client crate from schema");
            println!("  reset       - Drop all database tables and reset state (dangerous!)");
            println!("  doctor      - Show resolved config, schema files, and output paths");
            println!("  completions - Generate shell autocomplete scripts");
            println!("  repair      - Add missing constraints and indexes from schema to database");
            println!("  self-update - Update ByteORM to the latest version from GitHub");
            println!("\nUsage:");
            println!("  Single schema:  schema.bo");
            println!("  Multi schema:   byteorm/*.bo or [schema].directory in byteorm.toml");
            println!("\nUse --help for more information");
        }
    }
}

fn resolve_project(cli: &Cli) -> Result<ProjectPaths, Box<dyn std::error::Error>> {
    let root = env::current_dir()?;
    let (config, config_path) = load_project_config(&root, cli.config.as_ref())?;

    let (schema_files, schema_source) = resolve_schema_files(&root, cli, &config)?;
    let client_output = resolve_client_output(&root, cli, &config);
    let crate_name = config
        .client
        .crate_name
        .clone()
        .unwrap_or_else(|| "byteorm-client".to_string());
    let dependency_source = config
        .client
        .dependency_source
        .unwrap_or(DependencySource::Vendored);

    Ok(ProjectPaths {
        root,
        config_path,
        schema_files,
        schema_source,
        client_output,
        crate_name,
        dependency_source,
    })
}

fn handle_completions(target: Option<CompletionTarget>) -> Result<(), Box<dyn std::error::Error>> {
    match target {
        Some(CompletionTarget::Install) => install_detected_completions(),
        Some(shell) => {
            generate_completions(shell);
            Ok(())
        }
        None => {
            print_completion_status();
            Ok(())
        }
    }
}

fn generate_completions(shell: CompletionTarget) {
    let Some(clap_shell) = shell.as_clap_shell() else {
        return;
    };
    let mut command = Cli::command();
    generate(clap_shell, &mut command, "byteorm", &mut std::io::stdout());
}

fn render_completions(shell: CompletionTarget) -> Vec<u8> {
    let mut buffer = Vec::new();
    if let Some(clap_shell) = shell.as_clap_shell() {
        let mut command = Cli::command();
        generate(clap_shell, &mut command, "byteorm", &mut buffer);
    }
    buffer
}

fn print_completion_status() {
    match detect_completion_shell() {
        Some(shell) => {
            println!("Detected shell: {}", shell.as_name());
            println!("Install autocomplete with:");
            println!("  byteorm completions install");
            println!();
            println!("Generate the script manually with:");
            println!("  byteorm completions {}", shell.as_name());
            if shell == CompletionTarget::Bash {
                println!();
                println!("Enable it for the current bash session with:");
                println!("  source <(byteorm completions bash)");
            }
        }
        None => {
            println!("Could not detect a supported shell.");
            println!("Generate manually with:");
            println!("  byteorm completions <bash|zsh|fish|powershell|elvish>");
        }
    }
}

fn install_detected_completions() -> Result<(), Box<dyn std::error::Error>> {
    let shell = detect_completion_shell()
        .ok_or("Could not detect a supported shell. Use byteorm completions <shell> manually.")?;
    let home = home_dir().ok_or("Could not determine the home directory.")?;

    println!("Detected shell: {}", shell.as_name());

    match shell {
        CompletionTarget::Bash => {
            let base = env::var_os("XDG_DATA_HOME")
                .map(PathBuf::from)
                .unwrap_or_else(|| home.join(".local/share"));
            let path = base.join("bash-completion/completions/byteorm");
            write_completion_file(&path, shell)?;
            println!("Installed bash autocomplete: {}", path.display());
            println!("Restart your shell, or enable it now with:");
            println!("  source {}", path.display());
        }
        CompletionTarget::Zsh => {
            let dir = home.join(".byteorm/completions");
            let path = dir.join("_byteorm");
            write_completion_file(&path, shell)?;
            let zshrc = env::var_os("ZDOTDIR")
                .map(PathBuf::from)
                .unwrap_or_else(|| home.clone())
                .join(".zshrc");
            append_once(
                &zshrc,
                "# ByteORM autocomplete",
                &format!(
                    "fpath=(\"{}\" $fpath)\nautoload -Uz compinit\ncompinit",
                    dir.display()
                ),
            )?;
            println!("Installed zsh autocomplete: {}", path.display());
            println!("Restart your shell if Tab completion is not active.");
        }
        CompletionTarget::Fish => {
            let base = env::var_os("XDG_CONFIG_HOME")
                .map(PathBuf::from)
                .unwrap_or_else(|| home.join(".config"));
            let path = base.join("fish/completions/byteorm.fish");
            write_completion_file(&path, shell)?;
            println!("Installed fish autocomplete: {}", path.display());
        }
        CompletionTarget::Powershell => {
            let path = home.join(".byteorm/completions/byteorm.ps1");
            write_completion_file(&path, shell)?;
            println!(
                "Installed PowerShell autocomplete script: {}",
                path.display()
            );
            println!("Add this line to your PowerShell profile:");
            println!("  . \"{}\"", path.display());
        }
        CompletionTarget::Elvish => {
            let base = env::var_os("XDG_CONFIG_HOME")
                .map(PathBuf::from)
                .unwrap_or_else(|| home.join(".config"));
            let path = base.join("elvish/lib/byteorm-completions.elv");
            write_completion_file(&path, shell)?;
            let rc = base.join("elvish/rc.elv");
            append_once(&rc, "# ByteORM autocomplete", "use byteorm-completions")?;
            println!("Installed elvish autocomplete: {}", path.display());
            println!("Restart your shell if Tab completion is not active.");
        }
        CompletionTarget::Install => {}
    }

    Ok(())
}

fn write_completion_file(
    path: &Path,
    shell: CompletionTarget,
) -> Result<(), Box<dyn std::error::Error>> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    fs::write(path, render_completions(shell))?;
    Ok(())
}

fn append_once(path: &Path, marker: &str, content: &str) -> Result<(), Box<dyn std::error::Error>> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }

    let current = fs::read_to_string(path).unwrap_or_default();
    if current.contains(marker) {
        return Ok(());
    }

    let mut next = current;
    if !next.is_empty() && !next.ends_with('\n') {
        next.push('\n');
    }
    next.push('\n');
    next.push_str(marker);
    next.push('\n');
    next.push_str(content);
    next.push('\n');
    fs::write(path, next)?;
    Ok(())
}

fn detect_completion_shell() -> Option<CompletionTarget> {
    #[cfg(windows)]
    {
        return Some(CompletionTarget::Powershell);
    }

    #[cfg(not(windows))]
    {
        let shell = env::var("SHELL").ok()?;
        let name = Path::new(&shell)
            .file_name()
            .and_then(|s| s.to_str())
            .unwrap_or(&shell)
            .to_ascii_lowercase();

        match name.as_str() {
            "bash" => Some(CompletionTarget::Bash),
            "zsh" => Some(CompletionTarget::Zsh),
            "fish" => Some(CompletionTarget::Fish),
            "pwsh" | "powershell" => Some(CompletionTarget::Powershell),
            "elvish" => Some(CompletionTarget::Elvish),
            _ => None,
        }
    }
}

fn home_dir() -> Option<PathBuf> {
    env::var_os("HOME")
        .map(PathBuf::from)
        .or_else(|| env::var_os("USERPROFILE").map(PathBuf::from))
}

fn load_project_config(
    root: &Path,
    explicit_path: Option<&PathBuf>,
) -> Result<(ProjectConfig, Option<PathBuf>), Box<dyn std::error::Error>> {
    let path = explicit_path
        .cloned()
        .unwrap_or_else(|| root.join("byteorm.toml"));

    if !path.exists() {
        if explicit_path.is_some() {
            return Err(format!("Config file not found: {}", path.display()).into());
        }
        return Ok((ProjectConfig::default(), None));
    }

    let content = fs::read_to_string(&path)?;
    let config = parse_project_config(&content)?;
    Ok((config, Some(path)))
}

fn parse_project_config(content: &str) -> Result<ProjectConfig, Box<dyn std::error::Error>> {
    let mut config = ProjectConfig::default();
    let mut section = String::new();

    for (idx, raw_line) in content.lines().enumerate() {
        let line = strip_toml_comment(raw_line).trim().to_string();
        if line.is_empty() {
            continue;
        }

        if line.starts_with('[') && line.ends_with(']') {
            section = line[1..line.len() - 1].trim().to_string();
            continue;
        }

        let Some((key, value)) = line.split_once('=') else {
            return Err(format!("Invalid byteorm.toml line {}: {}", idx + 1, raw_line).into());
        };
        let key = key.trim();
        let value = parse_toml_string(value.trim())
            .ok_or_else(|| format!("Expected string value on byteorm.toml line {}", idx + 1))?;

        match (section.as_str(), key) {
            ("schema", "path") => config.schema.path = Some(PathBuf::from(value)),
            ("schema", "directory") | ("schema", "dir") => {
                config.schema.directory = Some(PathBuf::from(value))
            }
            ("client", "output") => config.client.output = Some(PathBuf::from(value)),
            ("client", "crate_name") | ("client", "crate-name") => {
                config.client.crate_name = Some(value)
            }
            ("client", "dependency_source") | ("client", "dependency-source") => {
                config.client.dependency_source = Some(parse_dependency_source(&value)?)
            }
            _ => {}
        }
    }

    if config.schema.path.is_some() && config.schema.directory.is_some() {
        return Err("[schema] can define either path or directory, not both".into());
    }

    Ok(config)
}

fn parse_dependency_source(value: &str) -> Result<DependencySource, Box<dyn std::error::Error>> {
    match value {
        "vendored" => Ok(DependencySource::Vendored),
        "path" | "git" | "registry" => Err(format!(
            "client.dependency_source = {:?} is reserved for a future ByteORM release; v0.1.x only supports \"vendored\"",
            value
        )
        .into()),
        _ => Err(format!(
            "Unknown client.dependency_source {:?}; supported value is \"vendored\"",
            value
        )
        .into()),
    }
}

fn strip_toml_comment(line: &str) -> String {
    let mut in_string = false;
    let mut escaped = false;

    for (idx, ch) in line.char_indices() {
        if escaped {
            escaped = false;
            continue;
        }
        if ch == '\\' && in_string {
            escaped = true;
            continue;
        }
        if ch == '"' {
            in_string = !in_string;
            continue;
        }
        if ch == '#' && !in_string {
            return line[..idx].to_string();
        }
    }

    line.to_string()
}

fn parse_toml_string(value: &str) -> Option<String> {
    let value = value.trim();
    if value.starts_with('"') && value.ends_with('"') && value.len() >= 2 {
        Some(value[1..value.len() - 1].replace("\\\"", "\""))
    } else if value.is_empty() {
        None
    } else {
        Some(value.to_string())
    }
}

fn resolve_schema_files(
    root: &Path,
    cli: &Cli,
    config: &ProjectConfig,
) -> Result<(Vec<PathBuf>, String), Box<dyn std::error::Error>> {
    if cli.schema.is_some() && cli.schema_dir.is_some() {
        return Err("Use either --schema or --schema-dir, not both".into());
    }

    if let Some(path) = &cli.schema {
        return Ok((
            single_schema_file(root, path),
            format!("--schema {}", path.display()),
        ));
    }

    if let Some(dir) = &cli.schema_dir {
        return Ok((
            schema_files_from_dir(root, dir),
            format!("--schema-dir {}", dir.display()),
        ));
    }

    if let Some(path) = &config.schema.path {
        return Ok((
            single_schema_file(root, path),
            format!("byteorm.toml [schema].path = {}", path.display()),
        ));
    }

    if let Some(dir) = &config.schema.directory {
        return Ok((
            schema_files_from_dir(root, dir),
            format!("byteorm.toml [schema].directory = {}", dir.display()),
        ));
    }

    let byteorm_dir = root.join("byteorm");
    if byteorm_dir.exists() && byteorm_dir.is_dir() {
        return Ok((
            schema_files_from_dir(root, &PathBuf::from("byteorm")),
            "legacy byteorm/*.bo".to_string(),
        ));
    }

    let single_schema = root.join("schema.bo");
    if single_schema.exists() {
        return Ok((vec![single_schema], "legacy schema.bo".to_string()));
    }

    Ok((Vec::new(), "no schema resolved".to_string()))
}

fn single_schema_file(root: &Path, path: &Path) -> Vec<PathBuf> {
    let full_path = if path.is_absolute() {
        path.to_path_buf()
    } else {
        root.join(path)
    };

    if full_path.exists() {
        vec![full_path]
    } else {
        Vec::new()
    }
}

fn schema_files_from_dir(root: &Path, dir: &Path) -> Vec<PathBuf> {
    let full_dir = if dir.is_absolute() {
        dir.to_path_buf()
    } else {
        root.join(dir)
    };

    let mut schema_files = Vec::new();
    if let Ok(entries) = fs::read_dir(&full_dir) {
        for entry in entries.flatten() {
            let path = entry.path();
            if path.extension().and_then(|s| s.to_str()) == Some("bo") {
                schema_files.push(path);
            }
        }
    }
    schema_files.sort();
    schema_files
}

fn resolve_client_output(root: &Path, cli: &Cli, config: &ProjectConfig) -> PathBuf {
    if let Some(output) = &cli.output {
        return output.clone();
    }

    if let Some(output) = &config.client.output {
        return output.clone();
    }

    if root.join("generated").join("Cargo.toml").exists() || cargo_toml_mentions_generated(root) {
        return PathBuf::from("generated");
    }

    PathBuf::from("generated")
}

fn cargo_toml_mentions_generated(root: &Path) -> bool {
    let cargo_toml = root.join("Cargo.toml");
    fs::read_to_string(cargo_toml)
        .map(|content| content.contains("\"generated\"") || content.contains("../generated"))
        .unwrap_or(false)
}

fn init_project(cli: &Cli) -> Result<(), Box<dyn std::error::Error>> {
    let root = env::current_dir()?;
    let config_path = cli
        .config
        .clone()
        .unwrap_or_else(|| root.join("byteorm.toml"));

    let use_directory = cli.schema_dir.clone().or_else(|| {
        let legacy_dir = root.join("byteorm");
        if legacy_dir.exists() {
            Some(PathBuf::from("byteorm"))
        } else {
            None
        }
    });

    let schema_path = cli
        .schema
        .clone()
        .unwrap_or_else(|| PathBuf::from("schema.bo"));
    let output = cli.output.clone().unwrap_or_else(|| {
        if root.join("generated").exists() || cargo_toml_mentions_generated(&root) {
            PathBuf::from("generated")
        } else {
            PathBuf::from(".byteorm/client")
        }
    });

    if !config_path.exists() {
        let config = if let Some(dir) = &use_directory {
            format!(
                "[schema]\ndirectory = \"{}\"\n\n[client]\noutput = \"{}\"\ncrate_name = \"byteorm-client\"\ndependency_source = \"vendored\"\n",
                normalize_path_for_config(dir),
                normalize_path_for_config(&output)
            )
        } else {
            format!(
                "[schema]\npath = \"{}\"\n\n[client]\noutput = \"{}\"\ncrate_name = \"byteorm-client\"\ndependency_source = \"vendored\"\n",
                normalize_path_for_config(&schema_path),
                normalize_path_for_config(&output)
            )
        };
        fs::write(&config_path, config)?;
        println!("Created {}", config_path.display());
    } else {
        println!("Keeping existing {}", config_path.display());
    }

    if let Some(dir) = use_directory {
        let full_dir = root.join(&dir);
        fs::create_dir_all(&full_dir)?;
        if schema_files_from_dir(&root, &dir).is_empty() {
            let starter = full_dir.join("schema.bo");
            fs::write(&starter, starter_schema())?;
            println!("Created {}", starter.display());
        }
    } else {
        let full_schema_path = root.join(&schema_path);
        if !full_schema_path.exists() {
            if let Some(parent) = full_schema_path.parent() {
                fs::create_dir_all(parent)?;
            }
            fs::write(&full_schema_path, starter_schema())?;
            println!("Created {}", full_schema_path.display());
        }
    }

    ensure_gitignore_entry(&root, &output)?;

    println!("\nByteORM initialized.");
    println!("Generate the client with:");
    println!("   byteorm generate");
    println!("\nAdd to your Cargo.toml:");
    println!("   byteorm-client = {{ path = \"{}\" }}", output.display());

    Ok(())
}

fn normalize_path_for_config(path: &Path) -> String {
    path.display().to_string().replace('\\', "/")
}

fn starter_schema() -> &'static str {
    r#"model User {
    id         BigInt      PrimaryKey
    email      String      Unique
    created_at TimestamptZ @default(now())
}
"#
}

fn ensure_gitignore_entry(root: &Path, output: &Path) -> Result<(), Box<dyn std::error::Error>> {
    let gitignore = root.join(".gitignore");
    let entry = output
        .components()
        .next()
        .map(|c| c.as_os_str().to_string_lossy().to_string())
        .unwrap_or_else(|| output.display().to_string());
    let entry = if entry.ends_with('/') {
        entry
    } else {
        format!("{}/", entry)
    };

    let current = fs::read_to_string(&gitignore).unwrap_or_default();
    if current
        .lines()
        .any(|line| line.trim() == entry.trim_end_matches('/'))
        || current.lines().any(|line| line.trim() == entry)
    {
        return Ok(());
    }

    let mut next = current;
    if !next.is_empty() && !next.ends_with('\n') {
        next.push('\n');
    }
    next.push_str(&entry);
    next.push('\n');
    fs::write(gitignore, next)?;
    Ok(())
}

fn print_doctor(project: &ProjectPaths) {
    println!("ByteORM project");
    println!("  root: {}", project.root.display());
    match &project.config_path {
        Some(path) => println!("  config: {}", path.display()),
        None => println!("  config: <none>"),
    }
    println!("  schema source: {}", project.schema_source);
    println!("  schema files: {}", project.schema_files.len());
    for file in &project.schema_files {
        println!("    - {}", file.display());
    }
    println!("  client crate: {}", project.crate_name);
    println!("  client output: {}", project.client_output.display());
    println!(
        "  dependency source: {}",
        match project.dependency_source {
            DependencySource::Vendored => "vendored",
        }
    );
}

fn print_schema_help() {
    eprintln!("  Create schema.bo in the project root, or");
    eprintln!("  create byteorm/*.bo for multi-schema projects, or");
    eprintln!("  configure [schema].path / [schema].directory in byteorm.toml.");
}

fn is_destructive_change(change: &diff::Change) -> bool {
    matches!(
        change,
        diff::Change::RemoveTable(_)
            | diff::Change::RemoveColumn { .. }
            | diff::Change::DropEnum(_)
    )
}

fn discover_schema_files() -> Vec<PathBuf> {
    let current_dir = match env::current_dir() {
        Ok(dir) => dir,
        Err(_) => return vec![],
    };

    let byteorm_dir = current_dir.join("byteorm");

    if byteorm_dir.exists() && byteorm_dir.is_dir() {
        println!("📂 Using multi-schema directory: byteorm/");

        let mut schema_files = Vec::new();

        if let Ok(entries) = fs::read_dir(&byteorm_dir) {
            for entry in entries.flatten() {
                let path = entry.path();
                if path.extension().and_then(|s| s.to_str()) == Some("bo") {
                    schema_files.push(path);
                }
            }
        }

        schema_files.sort();
        return schema_files;
    }

    let single_schema = current_dir.join("schema.bo");
    if single_schema.exists() {
        println!("📄 Using single schema file: schema.bo");
        return vec![single_schema];
    }

    vec![]
}

fn load_and_merge_schemas(files: &[PathBuf]) -> Result<Schema, Box<dyn std::error::Error>> {
    let mut all_models = Vec::new();
    let mut all_enums = Vec::new();

    for file in files {
        let content = fs::read_to_string(file)?;
        let schema = parse_schema(&content)
            .map_err(|e| format!("Error parsing {}: {}", file.display(), e))?;

        all_models.extend(schema.models);
        all_enums.extend(schema.enums);
    }

    Ok(Schema {
        models: all_models,
        enums: all_enums,
    })
}

fn generate_client_package(
    schema: &Schema,
    project: &ProjectPaths,
) -> Result<(), Box<dyn std::error::Error>> {
    let client_path = project.root.join(&project.client_output);

    println!("Generating client in: {}", client_path.display());

    fs::create_dir_all(client_path.join("src/models"))?;

    match project.dependency_source {
        DependencySource::Vendored => embed_byteorm_macros(&client_path)?,
    }

    let cargo_toml = generate_client_cargo_toml(&project.crate_name);
    fs::write(client_path.join("Cargo.toml"), cargo_toml)?;

    let files = rustgen::generate_rust_code(schema);
    for (rel_path, content) in files {
        let full_path = client_path.join(rel_path);
        if let Some(parent) = full_path.parent() {
            fs::create_dir_all(parent)?;
        }
        fs::write(&full_path, content)?;

        let _ = std::process::Command::new("rustfmt")
            .arg(full_path)
            .output();
    }

    println!("Client generated successfully.");

    Ok(())
}

fn embed_byteorm_macros(client_path: &Path) -> Result<(), Box<dyn std::error::Error>> {
    let macros_path = client_path.join("byteorm-macros");
    for (rel, content) in embedded_macros_files() {
        let dest = macros_path.join(rel);
        if let Some(parent) = dest.parent() {
            fs::create_dir_all(parent)?;
        }
        fs::write(&dest, content)?;
    }
    Ok(())
}

fn generate_client_cargo_toml(crate_name: &str) -> String {
    format!(
        r#"[package]
name = "{}"
version = "0.1.0"
edition = "2024"

[dependencies]
byteorm-macros = {{ path = "./byteorm-macros" }}
serde = {{ version = "1.0.228", features = ["derive"]}}
serde_json = "1.0.145"
chrono = {{ version = "0.4.42", features = ["serde"]}}
tokio = {{ version = "1.48.0", features = ["full"]}}
tokio-postgres = {{ version = "0.7", features = ["with-chrono-0_4", "with-serde_json-1"] }}
tokio-postgres-rustls = "0.13"
rustls = {{ version = "0.23", default-features = false, features = ["ring", "std"] }}
webpki-roots = "0.26"
bb8 = "0.8"
bb8-postgres = "0.8"
once_cell = "1.21.3"
futures-util = "0.3.31"
"#,
        crate_name
    )
}

async fn self_update() -> Result<(), Box<dyn std::error::Error>> {
    use std::env;
    use std::fs;
    use std::process::Command;

    println!("Updating ByteORM from GitHub...");

    let current_exe = env::current_exe()?;

    #[cfg(target_os = "windows")]
    {
        let temp_dir = env::temp_dir();

        println!("Installing the new binary into a temporary location...");
        run_cargo_install_quiet(vec![
            "install".to_string(),
            "--git".to_string(),
            "https://github.com/bytematebot/byteorm".to_string(),
            "--package".to_string(),
            "byteorm".to_string(),
            "--bin".to_string(),
            "byteorm".to_string(),
            "--force".to_string(),
            "--root".to_string(),
            temp_dir.to_string_lossy().to_string(),
        ])?;

        let installed_exe = temp_dir.join("bin").join("byteorm.exe");
        if !installed_exe.exists() {
            return Err("New executable not found after install".into());
        }

        println!("Preparing the handoff script...");
        let script_path = temp_dir.join("byteorm_update.ps1");
        let script_content = format!(
            r#"Start-Sleep -Milliseconds 500
Remove-Item -Path '{}' -Force -ErrorAction SilentlyContinue
Copy-Item -Path '{}' -Destination '{}' -Force
Remove-Item -Path '{}' -Force -ErrorAction SilentlyContinue
Remove-Item -Path '{}' -Force -ErrorAction SilentlyContinue
Write-Host "ByteORM updated successfully."
"#,
            current_exe.display(),
            installed_exe.display(),
            current_exe.display(),
            installed_exe.display(),
            script_path.display()
        );

        fs::write(&script_path, script_content)?;

        println!("Launching the handoff script...");
        Command::new("powershell")
            .args([
                "-ExecutionPolicy",
                "Bypass",
                "-File",
                script_path.to_str().unwrap(),
            ])
            .spawn()?;

        println!("Update initiated. ByteORM will exit now.");
        std::process::exit(0);
    }

    #[cfg(not(target_os = "windows"))]
    {
        println!("Installing the new binary...");
        run_cargo_install_quiet(vec![
            "install".to_string(),
            "--git".to_string(),
            "https://github.com/bytematebot/byteorm".to_string(),
            "--package".to_string(),
            "byteorm".to_string(),
            "--bin".to_string(),
            "byteorm".to_string(),
            "--force".to_string(),
        ])?;

        println!("ByteORM updated successfully.");
        println!("Restart ByteORM to use the new version.");

        Ok(())
    }
}

fn run_cargo_install_quiet(args: Vec<String>) -> Result<(), Box<dyn std::error::Error>> {
    use std::io::{self, Write};
    use std::process::{Command, Stdio};
    use std::thread;
    use std::time::Duration;

    let mut command = Command::new("cargo");
    command
        .args(&args)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());

    let worker = thread::spawn(move || command.output());
    let frames = ["|", "/", "-", "\\"];
    let messages = [
        "borrowing the latest binary",
        "asking Cargo to keep it chill",
        "warming up the type checker",
        "compiling optimism",
        "linking tiny bits of confidence",
        "making semver behave",
        "convincing rustc this is fine",
        "installing fewer surprises",
    ];
    let mut tick = 0usize;

    while !worker.is_finished() {
        let frame = frames[tick % frames.len()];
        let message = messages[(tick / 4) % messages.len()];
        print!("\r{} {}", frame, message);
        io::stdout().flush()?;
        tick += 1;
        thread::sleep(Duration::from_millis(140));
    }

    print!("\r{}\r", " ".repeat(80));
    io::stdout().flush()?;

    let output = worker
        .join()
        .map_err(|_| "cargo install worker panicked")??;

    if output.status.success() {
        return Ok(());
    }

    eprintln!("cargo install failed with status: {}", output.status);

    let stdout = String::from_utf8_lossy(&output.stdout);
    if !stdout.trim().is_empty() {
        eprintln!("\nCargo stdout:\n{}", stdout.trim_end());
    }

    let stderr = String::from_utf8_lossy(&output.stderr);
    if !stderr.trim().is_empty() {
        eprintln!("\nCargo stderr:\n{}", stderr.trim_end());
    }

    Err("cargo install failed".into())
}

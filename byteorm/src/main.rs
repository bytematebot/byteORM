use byteorm_lib::*;
use clap::Parser;
use std::path::{Path, PathBuf};
use std::{env, fs};
mod studio;

#[derive(Parser)]
#[command(name = "byte")]
#[command(about = "ByteORM CLI - Database schema management", long_about = None)]
struct Cli {
    #[command(subcommand)]
    command: Option<Commands>,
}

#[derive(clap::Subcommand)]
enum Commands {
    /// Push schema to database, run migrations, and generate the byteorm-client crate
    Push,
    /// Drop all database tables and reset state (dangerous!)
    Reset,
    /// Launch ByteORM Studio (web UI) on port 5555
    Studio {
        /// Port to bind the Studio server on (default 5555)
        #[arg(long, default_value_t = 5555)]
        port: u16,
    },
}

#[tokio::main]
async fn main() {
    dotenvy::dotenv().ok();

    let cli = Cli::parse();

    match cli.command {
        Some(Commands::Push) => {
            println!("Pushing schema...");

            let schema_files = discover_schema_files();

            if schema_files.is_empty() {
                eprintln!("No schema files found!");
                eprintln!("  Create either:");
                eprintln!("   - schema.bo in current directory, OR");
                eprintln!("   - byteorm/*.bo files for multi-schema");
                return;
            }

            println!("Found {} schema file(s):", schema_files.len());
            for file in &schema_files {
                println!("   - {}", file.display());
            }

            let schema = match load_and_merge_schemas(&schema_files) {
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
                        existing_tables.iter().any(|t| t.eq_ignore_ascii_case(&m.name))
                    });
                    let removed = before_count - prev_schema.models.len();
                    if removed > 0 {
                        println!("⚠️  Removed {} model(s) from snapshot (tables don't exist in DB)", removed);
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

            let sql = codegen::generate_migration_sql(&changes);
            println!("\nGenerated SQL:\n{}", sql);

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

            println!("\n🔧 Generating byteorm-client crate...");
            if let Err(e) = generate_client_package(&schema) {
                eprintln!("Error generating client: {}", e);
                return;
            }

            println!("\nPush complete!");
            println!("Add to your Cargo.toml:");
            println!("   [dependencies]");
            println!("   byteorm-client = {{ path = \"generated\" }}");
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

            let schema_files = discover_schema_files();
            if schema_files.is_empty() {
                eprintln!("No schema files found!");
                return;
            }
            let schema = match load_and_merge_schemas(&schema_files) {
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
        Some(Commands::Studio { port }) => {
            println!("Starting ByteORM Studio on http://localhost:{} ...", port);

            let schema_files = discover_schema_files();
            if schema_files.is_empty() {
                eprintln!("No schema files found! Create schema.bo or byteorm/*.bo first.");
                return;
            }

            let schema = match load_and_merge_schemas(&schema_files) {
                Ok(s) => s,
                Err(e) => {
                    eprintln!("Error loading schemas: {}", e);
                    return;
                }
            };

            let pool = match db::create_pool().await {
                Ok(p) => p,
                Err(e) => {
                    eprintln!("Database pool error: {}", e);
                    return;
                }
            };

            if let Err(e) = crate::studio::run(schema, pool, port).await {
                eprintln!("Studio error: {}", e);
            }
        }
        None => {
            println!("ByteORM CLI v0.1.0");
            println!("Commands:");
            println!("  push   - Push schema, run migrations, and generate byteorm-client crate");
            println!("  reset  - Drop all database tables and reset state (dangerous!)");
            println!("  studio - Launch GUI to browse and edit data at http://localhost:5555");
            println!("\nUsage:");
            println!("  Single schema:  create schema.bo");
            println!("  Multi schema:   create byteorm/*.bo files");
            println!("\nUse --help for more information");
        }
    }
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

    Ok(Schema { models: all_models, enums: all_enums })
}

fn generate_client_package(schema: &Schema) -> Result<(), Box<dyn std::error::Error>> {
    let current_dir = env::current_dir()?;
    let client_path = current_dir.join("generated");

    println!("Generating client in: {}", client_path.display());

    fs::create_dir_all(client_path.join("src"))?;

    let cargo_toml = generate_client_cargo_toml();
    fs::write(client_path.join("Cargo.toml"), cargo_toml)?;

    let lib_rs = rustgen::generate_rust_code(schema);
    fs::write(client_path.join("src/lib.rs"), lib_rs)?;

    let _ = std::process::Command::new("rustfmt")
        .arg(client_path.join("src/lib.rs"))
        .output();

    println!("Client generated successfully.");

    Ok(())
}

fn generate_client_cargo_toml() -> String {
    r#"[package]
name = "byteorm-client"
version = "0.1.0"
edition = "2024"

[dependencies]
serde = { version = "1.0.228", features = ["derive"]}
serde_json = "1.0.145"
chrono = { version = "0.4.42", features = ["serde"]}
tokio = { version = "1.48.0", features = ["full"]}
tokio-postgres = { version = "0.7.15", features = ["with-chrono-0_4", "with-serde_json-1"] }
bb8 = "0.8"
bb8-postgres = "0.8"
once_cell = "1.21.3"
futures-util = "0.3.31"
"#
    .to_string()
}

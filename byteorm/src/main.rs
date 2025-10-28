use clap::Parser;
use std::{env, fs};
use std::path::{Path, PathBuf};
use byteorm_lib::*;

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

            let previous = snapshot::load_snapshot(&client)
                .await
                .unwrap_or(None);

            let changes = diff::diff_schemas(previous.as_ref(), &schema);

            println!("\nChanges detected:");
            for change in &changes {
                println!("  - {:?}", change);
            }

            let sql = codegen::generate_migration_sql(&changes);
            println!("\nGenerated SQL:\n{}", sql);

            println!("\n⚙️  Executing SQL...");
            if let Err(e) = db::execute_sql(&client, &sql).await {
                eprintln!("Error executing SQL: {}", e);
                return;
            }

            if let Err(e) = snapshot::save_snapshot(&client, &schema).await {
                eprintln!("Error saving snapshot: {}", e);
                return;
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

        None => {
            println!("ByteORM CLI v0.1.0");
            println!("Commands:");
            println!("  push  - Push schema, run migrations, and generate byteorm-client crate");
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

    let byteorm_dir = current_dir.join("..");

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

    for file in files {
        let content = fs::read_to_string(file)?;
        let schema = parse_schema(&content)
            .map_err(|e| format!("Error parsing {}: {}", file.display(), e))?;

        all_models.extend(schema.models);
    }

    Ok(Schema {
        models: all_models,
    })
}

fn generate_client_package(schema: &Schema) -> Result<(), Box<dyn std::error::Error>> {
    let current_dir = env::current_dir()?;
    let client_path = current_dir.join("generated");

    println!("Generating client in: {}", client_path.display());

    fs::create_dir_all(client_path.join("src"))?;

    let cargo_toml = generate_client_cargo_toml();
    fs::write(client_path.join("../../Cargo.toml"), cargo_toml)?;

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
"#
        .to_string()
}

mod ast;
mod parser;

use pest::Parser;
use pest_derive::Parser;

pub mod rustgen;
#[derive(Parser)]
#[grammar = "grammar.pest"]
pub struct SchemaParser;

pub use ast::*;
use ast::ForeignKeyAction;
pub use parser::parse_schema;

pub mod snapshot {
    use crate::Schema;
    use tokio_postgres::Client;

    pub async fn init_snapshot_table(client: &Client) -> Result<(), Box<dyn std::error::Error>> {
        let create_table = "CREATE TABLE IF NOT EXISTS _byteorm_schema ( id SERIAL PRIMARY KEY, schema_json JSONB NOT NULL, created_at TIMESTAMP DEFAULT now(), updated_at TIMESTAMP DEFAULT now() )";

        client.execute(create_table, &[]).await?;
        Ok(())
    }

    pub async fn save_snapshot(
        client: &Client,
        schema: &Schema,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let json_value = serde_json::to_value(schema)?;

        client
            .execute("DELETE FROM _byteorm_schema WHERE id != 0", &[])
            .await
            .ok();

        client
            .execute(
                "INSERT INTO _byteorm_schema (schema_json, updated_at) VALUES ($1, now())",
                &[&json_value],
            )
            .await?;

        println!("Snapshot saved to database");
        Ok(())
    }

    pub async fn load_snapshot(
        client: &Client,
    ) -> Result<Option<Schema>, Box<dyn std::error::Error>> {
        let rows = client
            .query(
                "SELECT schema_json FROM _byteorm_schema ORDER BY updated_at DESC LIMIT 1",
                &[],
            )
            .await?;

        if rows.is_empty() {
            return Ok(None);
        }

        let json_value: serde_json::Value = rows[0].get(0);
        Ok(Some(serde_json::from_value(json_value)?))
    }
}

pub mod diff {
    use crate::{Field, Model, Modifier, Schema, Enum};

    #[derive(Debug, Clone, PartialEq, Eq)]
    struct FieldSignature {
        type_name: String,
        nullable: bool,
        default_literal: Option<String>,
        has_index: bool,
    }

    impl FieldSignature {
        fn new(field: &Field) -> Self {
            Self {
                type_name: field.type_name.clone(),
                nullable: field.is_nullable_column(),
                default_literal: field.sql_default_literal(),
                has_index: field.has_index(),
            }
        }
    }

    #[derive(Debug, Clone)]
    pub enum Change {
        CreateEnum(Enum),
        CreateTable(Model),
        AddColumn {
            table: String,
            field: Field,
        },
        RemoveColumn {
            table: String,
            column: String,
        },
        AlterColumn {
            table: String,
            column: String,
            old: Field,
            new: Field,
        },
        RemoveTable(String),
    }

    pub fn diff_schemas(previous: Option<&Schema>, current: &Schema) -> Vec<Change> {
        let mut changes = Vec::new();

        for enum_def in &current.enums {
            let exists = if let Some(prev) = previous {
                prev.enums.iter().any(|e| e.name == enum_def.name)
            } else {
                false
            };
            if !exists {
                changes.push(Change::CreateEnum(enum_def.clone()));
            }
        }

        if let Some(prev) = previous {
            for prev_model in &prev.models {
                if !current.models.iter().any(|m| m.name == prev_model.name) {
                    changes.push(Change::RemoveTable(prev_model.name.clone()));
                }
            }

            for curr_model in &current.models {
                if let Some(prev_model) = prev.models.iter().find(|m| m.name == curr_model.name) {
                    for curr_field in &curr_model.fields {
                        if !prev_model.fields.iter().any(|f| f.name == curr_field.name) {
                            changes.push(Change::AddColumn {
                                table: curr_model.name.clone(),
                                field: curr_field.clone(),
                            });
                        }
                    }

                    for prev_field in &prev_model.fields {
                        if !curr_model.fields.iter().any(|f| f.name == prev_field.name) {
                            changes.push(Change::RemoveColumn {
                                table: curr_model.name.clone(),
                                column: prev_field.name.clone(),
                            });
                        }
                    }

                    for curr_field in &curr_model.fields {
                        if let Some(prev_field) =
                            prev_model.fields.iter().find(|f| f.name == curr_field.name)
                        {
                            if FieldSignature::new(prev_field) != FieldSignature::new(curr_field) {
                                changes.push(Change::AlterColumn {
                                    table: curr_model.name.clone(),
                                    column: curr_field.name.clone(),
                                    old: prev_field.clone(),
                                    new: curr_field.clone(),
                                });
                            }
                        }
                    }
                } else {
                    changes.push(Change::CreateTable(curr_model.clone()));
                }
            }
        } else {
            for model in &current.models {
                changes.push(Change::CreateTable(model.clone()));
            }
        }

        changes
    }
}

pub mod codegen {
    use crate::{Field, Modifier, ForeignKeyAction, diff::Change};

    pub fn postgres_type(type_name: &str) -> String {
        match type_name {
            "BigInt" => "BIGINT".to_string(),
            "Int" => "INTEGER".to_string(),
            "String" => "TEXT".to_string(),
            "JsonB" => "JSONB".to_string(),
            "TimestamptZ" => "TIMESTAMP WITH TIME ZONE".to_string(),
            "Timestamp" => "TIMESTAMP".to_string(),
            "Boolean" => "BOOLEAN".to_string(),
            "Real" => "REAL".to_string(),
            "Serial" => "SERIAL".to_string(),
            _ => type_name.to_string(),
        }
    }

    pub fn field_to_sql(field: &Field) -> String {
        let mut sql = format!("{} {}", field.name, postgres_type(&field.type_name));

        for modifier in &field.modifiers {
            match modifier {
                Modifier::PrimaryKey => sql.push_str(" PRIMARY KEY"),
                Modifier::NotNull => sql.push_str(" NOT NULL"),
                Modifier::Nullable => sql.push_str(" NULL"),
                Modifier::Unique => sql.push_str(" UNIQUE"),
                Modifier::Index => {}
                Modifier::ForeignKey { model, field, on_delete } => {
                    let fk_field = field.as_deref().unwrap_or("id");
                    sql.push_str(&format!(" REFERENCES {} ({})", model, fk_field));
                    if let Some(action) = on_delete {
                        match action {
                            ForeignKeyAction::Cascade => sql.push_str(" ON DELETE CASCADE"),
                            ForeignKeyAction::NoAction => sql.push_str(" ON DELETE NO ACTION"),
                            ForeignKeyAction::SetNull => sql.push_str(" ON DELETE SET NULL"),
                            ForeignKeyAction::Restrict => sql.push_str(" ON DELETE RESTRICT"),
                        }
                    }
                }
            }
        }

        if field.is_sql_default() {
            if let Some(value) = field.get_default_value() {
                let sql_type = postgres_type(&field.type_name);
                if matches!(
                    sql_type.as_str(),
                    "BOOLEAN" | "REAL" | "INTEGER" | "BIGINT" | "SERIAL"
                ) || value == "now()"
                {
                    sql.push_str(&format!(" DEFAULT {}", value));
                } else {
                    sql.push_str(&format!(" DEFAULT '{}'", value));
                }
            }
        }

        sql
    }

    pub fn change_to_sql(change: &Change) -> String {
        match change {
            Change::CreateEnum(enum_def) => {
                let values = enum_def.values.iter()
                    .map(|v| format!("'{}'", v))
                    .collect::<Vec<_>>()
                    .join(", ");
                format!("CREATE TYPE {} AS ENUM ({});", enum_def.name, values)
            }
            Change::CreateTable(model) => {
                let mut sql = format!("CREATE TABLE IF NOT EXISTS {} ( ", model.name);
                let pk_columns: Vec<String> = model
                    .fields
                    .iter()
                    .filter(|f| {
                        f.modifiers
                            .iter()
                            .any(|m| matches!(m, Modifier::PrimaryKey))
                    })
                    .map(|f| f.name.clone())
                    .collect();

                for (idx, field) in model.fields.iter().enumerate() {
                    let mut field_sql = field_to_sql(field);
                    if field
                        .modifiers
                        .iter()
                        .any(|m| matches!(m, Modifier::PrimaryKey))
                        && pk_columns.len() > 1
                    {
                        field_sql = field_sql.replace(" PRIMARY KEY", "");
                    }
                    sql.push_str(&field_sql);
                    if idx < model.fields.len() - 1 {
                        sql.push_str(", ");
                    }
                }

                if pk_columns.len() > 1 {
                    sql.push_str(&format!(", PRIMARY KEY ({}) ", pk_columns.join(", ")));
                }
                sql.push_str(");");

                for field in &model.fields {
                    if field.modifiers.iter().any(|m| matches!(m, Modifier::Index)) {
                        let table_name = model.name.to_lowercase();
                        let field_name = field.name.to_lowercase();
                        let index_name = format!("idx_{}_{}", table_name, field_name);
                        sql.push_str(&format!(
                            " CREATE INDEX IF NOT EXISTS {} ON {} ({});",
                            index_name, table_name, field_name
                        ));
                    }
                }

                for attr in &model.model_attributes {
                    if attr.name == "index" {
                        if let Some(args) = &attr.args {
                            let table_name = model.name.to_lowercase();
                            let args_content =
                                args.trim_start_matches('(').trim_end_matches(')').trim();

                            if args_content.starts_with('[') && args_content.ends_with(']') {
                                let fields: Vec<&str> = args_content[1..args_content.len() - 1]
                                    .split(',')
                                    .map(|s| s.trim())
                                    .collect();
                                let fields_str = fields.join("_");
                                let index_name = format!("idx_{}_{}", table_name, fields_str);
                                let fields_sql = fields.join(", ");
                                sql.push_str(&format!(
                                    " CREATE INDEX IF NOT EXISTS {} ON {} ({});",
                                    index_name, table_name, fields_sql
                                ));
                            }
                        }
                    }
                }

                sql
            }
            Change::AddColumn { table, field } => {
                let mut sql = format!("ALTER TABLE {} ADD COLUMN {};", table, field_to_sql(field));
                if field.has_index() {
                    let table_name = table.to_lowercase();
                    let field_name = field.name.to_lowercase();
                    let index_name = format!("idx_{}_{}", table_name, field_name);
                    sql.push_str(&format!(
                        " CREATE INDEX IF NOT EXISTS {} ON {} ({});",
                        index_name, table, field.name
                    ));
                }
                sql
            }
            Change::RemoveColumn { table, column } => {
                format!("ALTER TABLE {} DROP COLUMN {};", table, column)
            }
            Change::AlterColumn {
                table,
                column,
                old,
                new,
            } => {
                let mut statements = Vec::new();

                if old.type_name != new.type_name {
                    statements.push(format!(
                        "ALTER TABLE {} ALTER COLUMN {} TYPE {};",
                        table,
                        column,
                        postgres_type(&new.type_name)
                    ));
                }

                if old.is_nullable_column() != new.is_nullable_column() {
                    if new.is_nullable_column() {
                        statements.push(format!(
                            "ALTER TABLE {} ALTER COLUMN {} DROP NOT NULL;",
                            table, column
                        ));
                    } else {
                        statements.push(format!(
                            "ALTER TABLE {} ALTER COLUMN {} SET NOT NULL;",
                            table, column
                        ));
                    }
                }

                if old.sql_default_literal() != new.sql_default_literal() {
                    match new.sql_default_literal() {
                        Some(default) => statements.push(format!(
                            "ALTER TABLE {} ALTER COLUMN {} SET DEFAULT {};",
                            table, column, default
                        )),
                        None => statements.push(format!(
                            "ALTER TABLE {} ALTER COLUMN {} DROP DEFAULT;",
                            table, column
                        )),
                    }
                }

                if old.has_index() != new.has_index() {
                    let table_name = table.to_lowercase();
                    let column_name = column.to_lowercase();
                    let index_name = format!("idx_{}_{}", table_name, column_name);
                    if new.has_index() {
                        statements.push(format!(
                            "CREATE INDEX IF NOT EXISTS {} ON {} ({});",
                            index_name, table, column
                        ));
                    } else {
                        statements.push(format!("DROP INDEX IF EXISTS {};", index_name));
                    }
                }

                statements.join(" ")
            }
            Change::RemoveTable(name) => {
                format!("DROP TABLE IF EXISTS {};", name)
            }
            _ => String::new(),
        }
    }

    pub fn generate_migration_sql(changes: &[Change]) -> String {
        let mut sql = String::new();
        for change in changes {
            sql.push_str(&change_to_sql(change));
        }
        sql
    }
}

pub mod db {
    use crate::Schema;
    use bb8::Pool;
    use bb8_postgres::PostgresConnectionManager;
    use std::env;
    use tokio_postgres::Client;
    use tokio_postgres_rustls::MakeRustlsConnect;

    pub type DbPool = Pool<PostgresConnectionManager<MakeRustlsConnect>>;

    pub async fn create_pool() -> Result<DbPool, Box<dyn std::error::Error>> {
        let db_url = env::var("DATABASE_URL")
            .unwrap_or_else(|_| "host=localhost user=postgres dbname=byteorm".to_string());

        let root_store = rustls::RootCertStore {
            roots: webpki_roots::TLS_SERVER_ROOTS.iter().cloned().collect(),
        };
        let tls_config = rustls::ClientConfig::builder()
            .with_root_certificates(root_store)
            .with_no_client_auth();
        let tls = MakeRustlsConnect::new(tls_config);

        let manager = PostgresConnectionManager::new_from_stringlike(db_url, tls)?;
        let pool = Pool::builder().max_size(20).build(manager).await?;

        Ok(pool)
    }

    pub async fn connect() -> Result<Client, Box<dyn std::error::Error>> {
        let db_url = env::var("DATABASE_URL")
            .unwrap_or_else(|_| "host=localhost user=postgres dbname=byteorm".to_string());

        let root_store = rustls::RootCertStore {
            roots: webpki_roots::TLS_SERVER_ROOTS.iter().cloned().collect(),
        };
        let tls_config = rustls::ClientConfig::builder()
            .with_root_certificates(root_store)
            .with_no_client_auth();
        let tls = MakeRustlsConnect::new(tls_config);

        let (client, connection) =
            tokio_postgres::connect(&db_url, tls).await?;

        tokio::spawn(async move {
            if let Err(e) = connection.await {
                eprintln!("Connection error: {}", e);
            }
        });

        Ok(client)
    }

    pub async fn reset_database(
        client: &tokio_postgres::Client,
        schema: &Schema,
    ) -> Result<(), Box<dyn std::error::Error>> {
        for model in &schema.models {
            let table_name = model.name.to_lowercase();
            let sql = format!("DROP TABLE IF EXISTS {} CASCADE;", table_name);
            client.execute(&sql, &[]).await?;
        }

        for enum_def in &schema.enums {
            let sql = format!("DROP TYPE IF EXISTS {} CASCADE;", enum_def.name);
            client.execute(&sql, &[]).await?;
        }

        client.execute("TRUNCATE _byteorm_schema;", &[]).await.ok();

        Ok(())
    }

    pub async fn execute_sql(client: &Client, sql: &str) -> Result<(), Box<dyn std::error::Error>> {
        println!("Executing batch SQL...");
        match client.batch_execute(sql).await {
            Ok(_) => {
                println!("  ✅ Batch execution OK");
                Ok(())
            }
            Err(e) => {
                eprintln!("  ❌ Error: {}", e);
                eprintln!("  📍 Error details:");
                eprintln!("     Code: {:?}", e.code());
                eprintln!("     Message: {}", e);
                Err(e.into())
            }
        }
    }

    pub async fn get_existing_tables(client: &Client) -> Result<Vec<String>, Box<dyn std::error::Error>> {
        let rows = client
            .query(
                "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public' AND table_type = 'BASE TABLE'",
                &[],
            )
            .await?;

        let tables: Vec<String> = rows.iter().map(|row| row.get::<_, String>(0)).collect();
        Ok(tables)
    }
}

mod ast;
mod parser;

use pest::Parser;
use pest_derive::Parser;


pub mod rustgen;
#[derive(Parser)]
#[grammar = "grammar.pest"]
pub struct SchemaParser;

pub use ast::*;
pub use parser::parse_schema;


pub mod snapshot {
    use tokio_postgres::Client;
    use crate::Schema;

    pub async fn init_snapshot_table(client: &Client) -> Result<(), Box<dyn std::error::Error>> {
        let create_table = "CREATE TABLE IF NOT EXISTS _byteorm_schema ( id SERIAL PRIMARY KEY, schema_json JSONB NOT NULL, created_at TIMESTAMP DEFAULT now(), updated_at TIMESTAMP DEFAULT now() )";

        client.execute(create_table, &[]).await?;
        Ok(())
    }

    pub async fn save_snapshot(client: &Client, schema: &Schema) -> Result<(), Box<dyn std::error::Error>> {
        let json_value = serde_json::to_value(schema)?;

        client.execute(
            "DELETE FROM _byteorm_schema WHERE id != 0",
            &[],
        ).await.ok();

        client.execute(
            "INSERT INTO _byteorm_schema (schema_json, updated_at) VALUES ($1, now())",
            &[&json_value], 
        ).await?;

        println!("Snapshot saved to database");
        Ok(())
    }


    pub async fn load_snapshot(client: &Client) -> Result<Option<Schema>, Box<dyn std::error::Error>> {
        let rows = client.query(
            "SELECT schema_json FROM _byteorm_schema ORDER BY updated_at DESC LIMIT 1",
            &[],
        ).await?;

        if rows.is_empty() {
            return Ok(None);
        }

        let json_value: serde_json::Value = rows[0].get(0);
        Ok(Some(serde_json::from_value(json_value)?))
    }

}



pub mod diff {
    use crate::{Schema, Model, Field, Modifier};

    #[derive(Debug, Clone)]
    pub enum Change {
        CreateTable(Model),
        AddColumn { table: String, field: Field },
        RemoveColumn { table: String, column: String },
        AlterColumn { table: String, column: String, old: Field, new: Field },
        RemoveTable(String),
    }

    pub fn diff_schemas(previous: Option<&Schema>, current: &Schema) -> Vec<Change> {
        let mut changes = Vec::new();

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
    use crate::{Field, Modifier, diff::Change};

    pub fn postgres_type(type_name: &str) -> &'static str {
        match type_name {
            "BigInt" => "BIGINT",
            "Int" => "INTEGER",
            "String" => "TEXT",
            "JsonB" => "JSONB",
            "TimestamptZ" => "TIMESTAMP WITH TIME ZONE",
            "Timestamp" => "TIMESTAMP",
            "Boolean" => "BOOLEAN",
            "Real" => "REAL",
            "Serial" => "SERIAL",
            _ => "TEXT",
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
                Modifier::ForeignKey { model, field } => {
                    let fk_field = field.as_deref().unwrap_or("id");
                    sql.push_str(&format!(" REFERENCES {} ({})", model, fk_field));
                }
            }
        }

        if field.is_sql_default() {
            if let Some(value) = field.get_default_value() {
                let sql_type = postgres_type(&field.type_name);
                if matches!(
                sql_type,
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
            Change::CreateTable(model) => {
                let mut sql = format!("CREATE TABLE IF NOT EXISTS {} ( ", model.name);
                let pk_columns: Vec<String> = model.fields
                    .iter()
                    .filter(|f| f.modifiers.iter().any(|m| matches!(m, Modifier::PrimaryKey)))
                    .map(|f| f.name.clone())
                    .collect();

                for (idx, field) in model.fields.iter().enumerate() {
                    let mut field_sql = field_to_sql(field);
                    if field.modifiers.iter().any(|m| matches!(m, Modifier::PrimaryKey)) && pk_columns.len() > 1 {
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
                sql
            }
            Change::AddColumn { table, field } => {
                format!("ALTER TABLE {} ADD COLUMN {};", table, field_to_sql(field))
            }
            Change::RemoveColumn { table, column } => {
                format!("ALTER TABLE {} DROP COLUMN {};", table, column)
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
    use tokio_postgres::Client;
    use std::env;
    use crate::Schema;
    use bb8::Pool;
    use bb8_postgres::PostgresConnectionManager;
    use tokio_postgres::NoTls;

    pub type DbPool = Pool<PostgresConnectionManager<NoTls>>;

    pub async fn create_pool() -> Result<DbPool, Box<dyn std::error::Error>> {
        let db_url = env::var("DATABASE_URL")
            .unwrap_or_else(|_| "host=localhost user=postgres dbname=byteorm".to_string());

        let manager = PostgresConnectionManager::new_from_stringlike(db_url, NoTls)?;
        let pool = Pool::builder()
            .max_size(20)
            .build(manager)
            .await?;

        Ok(pool)
    }

    pub async fn connect() -> Result<Client, Box<dyn std::error::Error>> {
        let db_url = env::var("DATABASE_URL")
            .unwrap_or_else(|_| "host=localhost user=postgres dbname=byteorm".to_string());

        let (client, connection) = tokio_postgres::connect(&db_url, tokio_postgres::tls::NoTls).await?;

        tokio::spawn(async move {
            if let Err(e) = connection.await {
                eprintln!("Connection error: {}", e);
            }
        });

        Ok(client)
    }

    pub async fn reset_database(client: &tokio_postgres::Client, schema: &Schema) -> Result<(), Box<dyn std::error::Error>> {
        for model in &schema.models {
            let table_name = model.name.to_lowercase();
            let sql = format!("DROP TABLE IF EXISTS {} CASCADE;", table_name);
            client.execute(&sql, &[]).await?;
        }

        client.execute("TRUNCATE _byteorm_schema;", &[]).await.ok();

        Ok(())
    }



    pub async fn execute_sql(client: &Client, sql: &str) -> Result<(), Box<dyn std::error::Error>> {
        for statement in sql.split(';').filter(|s| !s.trim().is_empty()) {
            let normalized = statement
                .lines()
                .map(|l| l.trim())
                .filter(|l| !l.is_empty())
                .collect::<Vec<_>>()
                .join(" ");

            println!("Executing: {}", normalized.trim());
            match client.execute(&normalized, &[]).await {
                Ok(_) => println!("  ✅ OK"),
                Err(e) => {
                    eprintln!("  ❌ Error: {}", e);
                    eprintln!("  📍 Error details:");
                    eprintln!("     Code: {:?}", e.code());
                    eprintln!("     Message: {}", e);
                    return Err(e.into());
                }
            }
        }
        Ok(())
    }

}


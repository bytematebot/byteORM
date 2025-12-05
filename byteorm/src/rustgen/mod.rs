use crate::Schema;
use quote::quote;
use std::collections::HashMap;
use std::fs;

pub mod client;
pub mod create;
pub mod debug;
pub mod delete;
pub mod jsonb;
pub mod model;
pub mod query;
pub mod update;
pub mod upsert;
pub mod utils;

pub use client::*;
pub use create::*;
pub use debug::*;
pub use delete::*;
pub use jsonb::*;
pub use model::*;
pub use query::*;
pub use update::*;
pub use upsert::*;
pub use utils::*;

pub fn generate_rust_code(schema: &Schema) -> String {
    let mut jsonb_defaults = HashMap::new();
    for model in &schema.models {
        for field in &model.fields {
            if let Some(path) = field.get_jsonb_default_path() {
                match fs::read_to_string(&path) {
                    Ok(content) => {
                        jsonb_defaults.insert((model.name.clone(), field.name.clone()), content);
                    }
                    Err(e) => {
                        eprintln!("Warning: Could not read default file '{}': {}", path, e);
                    }
                }
            }
        }
    }

    let structs_and_impls = schema
        .models
        .iter()
        .map(|model| generate_model_with_query_builder(model));

    let client_struct = generate_client_struct(schema, &jsonb_defaults);
    let jsonb_ext = generate_jsonb_ext();

    let code = quote! {
        use serde::{Deserialize, Serialize};
        use chrono::{DateTime, Utc};
        use tokio_postgres::{Client as PgClient, NoTls, Error};
        use std::sync::Arc;
        use once_cell::sync::Lazy;
        use std::collections::HashMap;
        use futures_util::task::Context;
        use std::pin::Pin;
        use futures_util::task::Poll;

        #[derive(Debug)]
        pub enum ByteOrmError {
            Pool(String),
            Query(tokio_postgres::Error),
            MissingField(String),
            NotFound(String),
            Validation(String),
            Serialization(String),
        }

        impl std::fmt::Display for ByteOrmError {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                match self {
                    ByteOrmError::Pool(msg) => write!(f, "Pool error: {}", msg),
                    ByteOrmError::Query(e) => write!(f, "Query error: {}", e),
                    ByteOrmError::MissingField(field) => write!(f, "Missing required field: {}", field),
                    ByteOrmError::NotFound(msg) => write!(f, "Not found: {}", msg),
                    ByteOrmError::Validation(msg) => write!(f, "Validation error: {}", msg),
                    ByteOrmError::Serialization(msg) => write!(f, "Serialization error: {}", msg),
                }
            }
        }

        impl std::error::Error for ByteOrmError {
            fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
                match self {
                    ByteOrmError::Query(e) => Some(e),
                    _ => None,
                }
            }
        }

        impl From<tokio_postgres::Error> for ByteOrmError {
            fn from(e: tokio_postgres::Error) -> Self {
                ByteOrmError::Query(e)
            }
        }

        impl From<serde_json::Error> for ByteOrmError {
            fn from(e: serde_json::Error) -> Self {
                ByteOrmError::Serialization(e.to_string())
            }
        }

        pub fn expect_keys<T: Copy>(
            map: &std::collections::HashMap<String, T>,
            keys: &[&str]
        ) -> Result<Vec<T>, &'static str> {
            keys.iter()
                .map(|k| map.get(*k).copied().ok_or("missing key"))
                .collect()
        }

        pub mod debug {
            use std::sync::atomic::{AtomicBool, Ordering};

            static DEBUG_ENABLED: AtomicBool = AtomicBool::new(false);

            pub fn enable_debug() {
                DEBUG_ENABLED.store(true, Ordering::Relaxed);
            }

            pub fn disable_debug() {
                DEBUG_ENABLED.store(false, Ordering::Relaxed);
            }

            pub fn is_debug_enabled() -> bool {
                DEBUG_ENABLED.load(Ordering::Relaxed)
            }

            pub fn log_query(sql: &str, params_count: usize) {
                if is_debug_enabled() {
                    eprintln!("[ByteORM Debug] Executing SQL: {}", sql);
                    eprintln!("[ByteORM Debug] Parameters count: {}", params_count);
                }
            }

            pub fn log_result(operation: &str, rows_affected: u64) {
                if is_debug_enabled() {
                    eprintln!("[ByteORM Debug] {} - Rows affected: {}", operation, rows_affected);
                }
            }

            pub fn log_error(operation: &str, error: &str) {
                if is_debug_enabled() {
                    eprintln!("[ByteORM Debug] Error in {}: {}", operation, error);
                }
            }
        }

        #jsonb_ext
        #client_struct
        #(#structs_and_impls)*
    };

    let file: syn::File = syn::parse2(code).unwrap();
    prettyplease::unparse(&file)
}

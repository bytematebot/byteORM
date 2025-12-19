use crate::Schema;
use proc_macro2::TokenStream;
use quote::{format_ident, quote};
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

pub fn generate_rust_code(schema: &Schema) -> HashMap<String, String> {
    let mut files = HashMap::new();
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

    // Generate Enums
    let enums_code = if !schema.enums.is_empty() {
        let enums = schema.enums.iter().map(|e| {
            let name = term_ident(&e.name);
            let variants = e.values.iter().map(|v| {
                let v_ident = term_ident(v);
                quote! { #v_ident }
            });
            quote! {
                #[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
                #[allow(non_camel_case_types)]
                pub enum #name {
                    #(#variants),*
                }
            }
        });
        let code = quote! {
            use serde::{Deserialize, Serialize};
            #(#enums)*
        };
        pretty_print(&code)
    } else {
        String::new()
    };
    files.insert("src/enums.rs".to_string(), enums_code);

    // Generate Models
    let mut model_mods = Vec::new();
    for model in &schema.models {
        let model_name_snake_str = to_snake_case(&model.name);
        let model_name_snake = format_ident!("{}", model_name_snake_str);
        model_mods.push(quote! {
            pub mod #model_name_snake;
            pub use #model_name_snake::*;
        });

        let model_code = generate_model_with_query_builder(model);
        let accessor_code = generate_accessor_for_model(model, &jsonb_defaults);

        let full_model_code = quote! {
            use serde::{Deserialize, Serialize};
            use chrono::{DateTime, Utc};
            use std::sync::Arc;
            use std::collections::HashMap;
            use once_cell::sync::Lazy;
            use std::pin::Pin;
            use std::task::{Context, Poll};
            use crate::{ByteOrmError, ConnectionPool, PooledClient, debug, expect_keys, JsonbExt};
            use crate::enums::*;

            #model_code
            #accessor_code
        };

        files.insert(
            format!("src/models/{}.rs", model_name_snake_str),
            pretty_print(&full_model_code),
        );
    }

    let models_mod_code = quote! {
        #(#model_mods)*
    };
    files.insert("src/models/mod.rs".to_string(), pretty_print(&models_mod_code));

    // Generate lib.rs
    let model_accessors = schema.models.iter().map(|model| {
        let accessor_name = format_ident!("{}", to_snake_case(&model.name));
        let model_name_snake = format_ident!("{}", to_snake_case(&model.name));
        let accessor_struct = format_ident!("{}Accessor", model.name);
        quote! { pub #accessor_name: models::#model_name_snake::#accessor_struct }
    });

    let accessor_inits = schema.models.iter().map(|model| {
        let accessor_name = format_ident!("{}", to_snake_case(&model.name));
        let model_name_snake = format_ident!("{}", to_snake_case(&model.name));
        let accessor_struct = format_ident!("{}Accessor", model.name);
        quote! { #accessor_name: models::#model_name_snake::#accessor_struct::new(pool.clone()) }
    });

    let debug_accessor_fields = schema.models.iter().map(|model| {
        let accessor_name = to_snake_case(&model.name);
        let accessor_name_ident = format_ident!("{}", accessor_name);
        quote! { .field(#accessor_name, &self.#accessor_name_ident) }
    });

    let jsonb_ext = generate_jsonb_ext();

    let lib_code = quote! {
        use serde::{Deserialize, Serialize};
        use chrono::{DateTime, Utc};
        use tokio_postgres::{Client as PgClient, NoTls, Error};
        use std::sync::Arc;
        use once_cell::sync::Lazy;
        use std::collections::HashMap;
        use futures_util::task::Context;
        use std::pin::Pin;
        use futures_util::task::Poll;

        pub mod enums;
        pub mod models;
        pub use models::*;
        pub use enums::*;

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

        /// Enum to support both TLS and NoTLS connection pools
        #[derive(Clone)]
        pub enum ConnectionPool {
            Tls(Arc<bb8::Pool<bb8_postgres::PostgresConnectionManager<tokio_postgres_rustls::MakeRustlsConnect>>>),
            NoTls(Arc<bb8::Pool<bb8_postgres::PostgresConnectionManager<tokio_postgres::NoTls>>>),
        }

        impl ConnectionPool {
            pub async fn get(&self) -> Result<PooledClient, tokio_postgres::Error> {
                match self {
                    ConnectionPool::Tls(pool) => {
                        let conn = pool.get().await.map_err(|_| tokio_postgres::Error::__private_api_timeout())?;
                        Ok(PooledClient::Tls(conn))
                    }
                    ConnectionPool::NoTls(pool) => {
                        let conn = pool.get().await.map_err(|_| tokio_postgres::Error::__private_api_timeout())?;
                        Ok(PooledClient::NoTls(conn))
                    }
                }
            }
        }

        /// Wrapper for pooled connections that works with both TLS and NoTLS
        pub enum PooledClient<'a> {
            Tls(bb8::PooledConnection<'a, bb8_postgres::PostgresConnectionManager<tokio_postgres_rustls::MakeRustlsConnect>>),
            NoTls(bb8::PooledConnection<'a, bb8_postgres::PostgresConnectionManager<tokio_postgres::NoTls>>),
        }

        impl<'a> PooledClient<'a> {
            pub async fn query(&self, sql: &str, params: &[&(dyn tokio_postgres::types::ToSql + Sync)]) -> Result<Vec<tokio_postgres::Row>, tokio_postgres::Error> {
                match self {
                    PooledClient::Tls(c) => c.query(sql, params).await,
                    PooledClient::NoTls(c) => c.query(sql, params).await,
                }
            }

            pub async fn query_one(&self, sql: &str, params: &[&(dyn tokio_postgres::types::ToSql + Sync)]) -> Result<tokio_postgres::Row, tokio_postgres::Error> {
                match self {
                    PooledClient::Tls(c) => c.query_one(sql, params).await,
                    PooledClient::NoTls(c) => c.query_one(sql, params).await,
                }
            }

            pub async fn query_opt(&self, sql: &str, params: &[&(dyn tokio_postgres::types::ToSql + Sync)]) -> Result<Option<tokio_postgres::Row>, tokio_postgres::Error> {
                match self {
                    PooledClient::Tls(c) => c.query_opt(sql, params).await,
                    PooledClient::NoTls(c) => c.query_opt(sql, params).await,
                }
            }

            pub async fn execute(&self, sql: &str, params: &[&(dyn tokio_postgres::types::ToSql + Sync)]) -> Result<u64, tokio_postgres::Error> {
                match self {
                    PooledClient::Tls(c) => c.execute(sql, params).await,
                    PooledClient::NoTls(c) => c.execute(sql, params).await,
                }
            }
        }

        #[derive(Clone)]
        pub struct Client {
            pool: ConnectionPool,
            #(#model_accessors),*
        }
        impl std::fmt::Debug for Client {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                f.debug_struct("Client")
                    .field("pool", &"<ConnectionPool>")
                    #(#debug_accessor_fields)*
                    .finish()
            }
        }
        impl Client {
            pub async fn new(connection_string: &str) -> Result<Self, Error> {
                // Detect if this is a localhost connection (no TLS needed)
                let is_local = connection_string.contains("localhost") || connection_string.contains("127.0.0.1");
                let requires_ssl = connection_string.contains("sslmode=require") || connection_string.contains("sslmode=verify");
                
                let pool = if is_local && !requires_ssl {
                    // Use NoTls for localhost connections
                    let manager = bb8_postgres::PostgresConnectionManager::new_from_stringlike(
                        connection_string,
                        tokio_postgres::NoTls,
                    )?;
                    let pool = bb8::Pool::builder()
                        .max_size(20)
                        .build(manager)
                        .await?;
                    ConnectionPool::NoTls(Arc::new(pool))
                } else {
                    // Use TLS for remote connections
                    let root_store = rustls::RootCertStore {
                        roots: webpki_roots::TLS_SERVER_ROOTS.iter().cloned().collect(),
                    };
                    let tls_config = rustls::ClientConfig::builder()
                        .with_root_certificates(root_store)
                        .with_no_client_auth();
                    let tls = tokio_postgres_rustls::MakeRustlsConnect::new(tls_config);
                    let manager = bb8_postgres::PostgresConnectionManager::new_from_stringlike(
                        connection_string,
                        tls,
                    )?;
                    let pool = bb8::Pool::builder()
                        .max_size(20)
                        .build(manager)
                        .await?;
                    ConnectionPool::Tls(Arc::new(pool))
                };
                
                Ok(Self {
                    pool: pool.clone(),
                    #(#accessor_inits),*
                })
            }

            pub async fn get_client(&self) -> Result<PooledClient<'_>, Error> {
                self.pool.get().await
            }

            pub fn pool(&self) -> &ConnectionPool { &self.pool }

            pub async fn transaction<F, T, E>(&self, f: F) -> Result<T, E>
            where
                F: FnOnce(Transaction<'_>) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<T, E>> + Send + '_>> + Send,
                E: From<tokio_postgres::Error> + Send,
                T: Send,
            {
                let client = self.pool.get().await.map_err(|e| E::from(e))?;
                match client {
                    PooledClient::Tls(mut c) => {
                        let tx = c.transaction().await?;
                        let transaction = Transaction { inner: tx };
                        let result = f(transaction).await;
                        result
                    }
                    PooledClient::NoTls(mut c) => {
                        let tx = c.transaction().await?;
                        let transaction = Transaction { inner: tx };
                        let result = f(transaction).await;
                        result
                    }
                }
            }

            pub async fn execute_raw(&self, sql: &str, params: &[&(dyn tokio_postgres::types::ToSql + Sync)]) -> Result<u64, Error> {
                let client = self.pool.get().await?;
                client.execute(sql, params).await
            }

            pub async fn query_raw(&self, sql: &str, params: &[&(dyn tokio_postgres::types::ToSql + Sync)]) -> Result<Vec<tokio_postgres::Row>, Error> {
                let client = self.pool.get().await?;
                client.query(sql, params).await
            }
        }

        pub struct Transaction<'a> {
            inner: tokio_postgres::Transaction<'a>,
        }

        impl<'a> Transaction<'a> {
            pub async fn execute(&self, sql: &str, params: &[&(dyn tokio_postgres::types::ToSql + Sync)]) -> Result<u64, Error> {
                self.inner.execute(sql, params).await
            }

            pub async fn query(&self, sql: &str, params: &[&(dyn tokio_postgres::types::ToSql + Sync)]) -> Result<Vec<tokio_postgres::Row>, Error> {
                self.inner.query(sql, params).await
            }

            pub async fn commit(self) -> Result<(), Error> {
                self.inner.commit().await
            }

            pub async fn rollback(self) -> Result<(), Error> {
                self.inner.rollback().await
            }
        }
    };

    files.insert("src/lib.rs".to_string(), pretty_print(&lib_code));

    files
}

fn term_ident(s: &str) -> proc_macro2::Ident {
    quote::format_ident!("{}", s)
}

fn pretty_print(code: &TokenStream) -> String {
    match syn::parse2::<syn::File>(code.clone()) {
        Ok(file) => prettyplease::unparse(&file),
        Err(e) => {
            eprintln!("ERROR parsing generated code: {}", e);
            eprintln!("Generated code:\n{}", code);
            panic!("Failed to parse generated Rust code");
        }
    }
}

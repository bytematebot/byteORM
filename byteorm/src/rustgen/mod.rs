use std::collections::HashMap;
use std::fs;
use quote::quote;
use crate::Schema;

pub mod client;
pub mod create;
pub mod delete;
pub mod jsonb;
pub mod model;
pub mod query;
pub mod update;
pub mod upsert;
pub mod utils;

pub use client::*;
pub use create::*;
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

    let structs_and_impls = schema.models.iter().map(|model| {
        generate_model_with_query_builder(model)
    });

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
        pub fn expect_keys<T: Copy>(
            map: &std::collections::HashMap<String, T>,
            keys: &[&str]
        ) -> Result<Vec<T>, &'static str> {
            keys.iter()
                .map(|k| map.get(*k).copied().ok_or("missing key"))
                .collect()
        }
        
        #jsonb_ext
        #client_struct
        #(#structs_and_impls)*
    };

    let file: syn::File = syn::parse2(code).unwrap();
    prettyplease::unparse(&file)
}

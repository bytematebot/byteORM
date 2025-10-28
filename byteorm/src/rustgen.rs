// src/rustgen.rs
// Complete code generation using quote! for query builders

use quote::{quote, format_ident};
use proc_macro2::TokenStream;
use crate::{Schema, Model, Field, Modifier};

pub fn generate_rust_code(schema: &Schema) -> String {
    let structs_and_impls = schema.models.iter().map(|model| {
        generate_model_with_query_builder(model)
    });

    let code = quote! {
        use serde::{Deserialize, Serialize};
        use chrono::{DateTime, Utc};
        use tokio_postgres::Client;

        // Helper function for calculating JSON diffs (used by audit)
        fn calculate_json_diff(before: &serde_json::Value, after: &serde_json::Value) -> serde_json::Value {
            let mut diff = serde_json::Map::new();

            if let (Some(before_obj), Some(after_obj)) = (before.as_object(), after.as_object()) {
                // Check for changed and added fields
                for (key, after_val) in after_obj {
                    if let Some(before_val) = before_obj.get(key) {
                        if before_val != after_val {
                            diff.insert(
                                key.clone(),
                                serde_json::json!({
                                    "from": before_val,
                                    "to": after_val
                                })
                            );
                        }
                    } else {
                        diff.insert(key.clone(), serde_json::json!({ "added": after_val }));
                    }
                }

                // Check for removed fields
                for (key, before_val) in before_obj {
                    if !after_obj.contains_key(key) {
                        diff.insert(key.clone(), serde_json::json!({ "removed": before_val }));
                    }
                }
            }

            serde_json::Value::Object(diff)
        }

        #(#structs_and_impls)*
    };

    let file: syn::File = syn::parse2(code).unwrap();
    prettyplease::unparse(&file)
}

fn generate_model_with_query_builder(model: &Model) -> TokenStream {
    let model_struct = generate_model_struct(model);
    let query_builder_struct = generate_query_builder_struct(model);
    let query_builder_impl = generate_query_builder_impl(model);
    let model_impl = generate_model_impl(model);

    quote! {
        #model_struct

        #query_builder_struct

        #model_impl

        #query_builder_impl
    }
}

fn generate_model_struct(model: &Model) -> TokenStream {
    let name = format_ident!("{}", model.name);
    let fields = model.fields.iter().map(|field| {
        let field_name = format_ident!("{}", field.name);
        let field_type = rust_type_from_schema(&field.type_name);

        quote! {
            pub #field_name: #field_type
        }
    });

    quote! {
        #[derive(Debug, Clone, Serialize, Deserialize)]
        pub struct #name {
            #(#fields),*
        }
    }
}

fn generate_model_impl(model: &Model) -> TokenStream {
    let model_name = format_ident!("{}", model.name);
    let builder_name = format_ident!("{}Query", model.name);

    // Find primary key field
    let pk_field = model.fields.iter()
        .find(|f| f.modifiers.iter().any(|m| matches!(m, Modifier::PrimaryKey)));

    let find_by_id_impl = if let Some(pk) = pk_field {
        let pk_name = format_ident!("{}", to_snake_case(&pk.name));
        let pk_type = rust_type_from_schema(&pk.type_name);

        quote! {
            pub fn find_by_id(id: #pk_type) -> String {
                format!("SELECT * FROM {} WHERE {} = {}",
                    stringify!(#pk_name).replace("_", ""),
                    stringify!(#pk_name),
                    id
                )
            }
        }
    } else {
        quote! {}
    };

    quote! {
        impl #model_name {
            pub fn query() -> #builder_name {
                #builder_name::new()
            }

            #find_by_id_impl
        }
    }
}

fn generate_query_builder_struct(model: &Model) -> TokenStream {
    let builder_name = format_ident!("{}Query", model.name);

    quote! {
        pub struct #builder_name {
            table: String,
            conditions: Vec<String>,
            limit: Option<usize>,
            offset: Option<usize>,
            order_by: Vec<(String, String)>,
        }

        impl Clone for #builder_name {
            fn clone(&self) -> Self {
                Self {
                    table: self.table.clone(),
                    conditions: self.conditions.clone(),
                    limit: self.limit,
                    offset: self.offset,
                    order_by: self.order_by.clone(),
                }
            }
        }
    }
}

fn generate_query_builder_impl(model: &Model) -> TokenStream {
    let builder_name = format_ident!("{}Query", model.name);
    let model_name = format_ident!("{}", model.name);
    let table_name = model.name.to_lowercase();

    // Generate where_* methods for each field
    let where_methods = model.fields.iter().map(|field| {
        let method_name = format_ident!("where_{}", to_snake_case(&field.name));
        let field_name = to_snake_case(&field.name);
        let field_type = rust_type_from_schema(&field.type_name);

        quote! {
            pub fn #method_name(mut self, value: #field_type) -> Self {
                self.conditions.push(format!("{} = {:?}", #field_name, value));
                self
            }
        }
    });

    let field_gets = model.fields.iter().enumerate().map(|(idx, field)| {
        let field_name = format_ident!("{}", field.name);

        quote! {
            #field_name: row.get(#idx)
        }
    });

    // Check if model has @audit attribute
    let has_audit = model.fields.iter().any(|f| f.get_audit_model().is_some());

    let update_method = if has_audit {
        generate_update_with_audit(model)
    } else {
        quote! {} // No update method for non-audited models yet
    };

    quote! {
        impl #builder_name {
            pub fn new() -> Self {
                Self {
                    table: #table_name.to_string(),
                    conditions: vec![],
                    limit: None,
                    offset: None,
                    order_by: vec![],
                }
            }

            #(#where_methods)*

            pub fn limit(mut self, limit: usize) -> Self {
                self.limit = Some(limit);
                self
            }

            pub fn offset(mut self, offset: usize) -> Self {
                self.offset = Some(offset);
                self
            }

            pub fn order_by(mut self, column: &str, direction: &str) -> Self {
                self.order_by.push((column.to_string(), direction.to_string()));
                self
            }

            pub async fn select(&self, client: &Client)
                -> Result<Vec<#model_name>, Box<dyn std::error::Error>>
            {
                let sql = self.build_select();

                let rows = client.query(&sql, &[]).await?;
                let mut results = Vec::new();

                for row in rows {
                    results.push(#model_name {
                        #(#field_gets),*
                    });
                }

                Ok(results)
            }

            pub async fn first(&self, client: &Client)
                -> Result<Option<#model_name>, Box<dyn std::error::Error>>
            {
                let mut query = self.clone();
                query.limit = Some(1);
                let results = query.select(client).await?;
                Ok(results.into_iter().next())
            }

            pub async fn count(&self, client: &Client)
                -> Result<i64, Box<dyn std::error::Error>>
            {
                let sql = format!("SELECT COUNT(*) FROM {}{}",
                    self.table,
                    if self.conditions.is_empty() {
                        String::new()
                    } else {
                        format!(" WHERE {}", self.conditions.join(" AND "))
                    }
                );

                let row = client.query_one(&sql, &[]).await?;
                Ok(row.get(0))
            }

            #update_method

            fn build_select(&self) -> String {
                let mut sql = format!("SELECT * FROM {}", self.table);

                if !self.conditions.is_empty() {
                    sql.push_str(" WHERE ");
                    sql.push_str(&self.conditions.join(" AND "));
                }

                if !self.order_by.is_empty() {
                    sql.push_str(" ORDER BY ");
                    let order_clauses: Vec<String> = self.order_by.iter()
                        .map(|(col, dir)| format!("{} {}", col, dir))
                        .collect();
                    sql.push_str(&order_clauses.join(", "));
                }

                if let Some(limit) = self.limit {
                    sql.push_str(&format!(" LIMIT {}", limit));
                }

                if let Some(offset) = self.offset {
                    sql.push_str(&format!(" OFFSET {}", offset));
                }

                sql
            }
        }
    }
}

fn generate_update_with_audit(model: &Model) -> TokenStream {
    let table_name = model.name.to_lowercase();

    // Find the audit field and target model
    let audit_field = model.fields.iter()
        .find(|f| f.get_audit_model().is_some())
        .expect("Called generate_update_with_audit without audit field");

    let audit_model_name = audit_field.get_audit_model().unwrap();
    let audit_table = audit_model_name.to_lowercase();
    let audited_field_name = &audit_field.name;

    // Find primary key for WHERE clause
    let pk_field = model.fields.iter()
        .find(|f| f.modifiers.iter().any(|m| matches!(m, Modifier::PrimaryKey)))
        .expect("Model must have primary key for audit");

    let pk_column = to_snake_case(&pk_field.name);

    quote! {
        pub async fn update(
            &self,
            client: &Client,
            new_value: serde_json::Value,
            who: i64,
        ) -> Result<(), Box<dyn std::error::Error>> {
            // Start transaction
            let transaction = client.transaction().await?;

            // Extract primary key value from conditions (assuming first condition is PK)
            if self.conditions.is_empty() {
                return Err("No WHERE condition specified for update".into());
            }

            // Parse PK value from condition string (e.g., "guild_id = 123")
            let pk_value: i64 = self.conditions[0]
                .split('=')
                .nth(1)
                .and_then(|s| s.trim().parse().ok())
                .ok_or("Failed to parse primary key value")?;

            // 1. Get current value (before)
            let before_sql = format!(
                "SELECT {} FROM {} WHERE {} = $1",
                #audited_field_name,
                #table_name,
                #pk_column
            );

            let before_row = transaction.query_one(&before_sql, &[&pk_value]).await?;
            let before: serde_json::Value = before_row.get(0);

            // 2. Update the main table
            let update_sql = format!(
                "UPDATE {} SET {} = $1, updated_at = now() WHERE {} = $2",
                #table_name,
                #audited_field_name,
                #pk_column
            );

            transaction.execute(&update_sql, &[&new_value, &pk_value]).await?;

            // 3. Calculate diff
            let diff = calculate_json_diff(&before, &new_value);

            // 4. Insert audit record
            let audit_sql = format!(
                "INSERT INTO {} ({}, who, changed_at, before, after, diff) VALUES ($1, $2, now(), $3, $4, $5)",
                #audit_table,
                #pk_column
            );

            transaction.execute(
                &audit_sql,
                &[&pk_value, &who, &before, &new_value, &diff]
            ).await?;

            // 5. Commit transaction
            transaction.commit().await?;

            println!("✅ Updated with audit log");
            Ok(())
        }
    }
}

// ====== Type Conversion Helpers ======

fn rust_type_from_schema(type_name: &str) -> TokenStream {
    match type_name {
        "BigInt" => quote! { i64 },
        "Int" => quote! { i32 },
        "String" => quote! { String },
        "JsonB" => quote! { serde_json::Value },
        "TimestamptZ" => quote! { DateTime<Utc> },
        "Boolean" => quote! { bool },
        "Float" => quote! { f64 },
        _ => quote! { String },
    }
}

fn to_snake_case(s: &str) -> String {
    let mut result = String::new();
    for (i, ch) in s.chars().enumerate() {
        if ch.is_uppercase() && i > 0 {
            result.push('_');
        }
        result.push(ch.to_lowercase().next().unwrap_or(ch));
    }
    result
}

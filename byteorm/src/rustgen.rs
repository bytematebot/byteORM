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

        fn calculate_json_diff(before: &serde_json::Value, after: &serde_json::Value) -> serde_json::Value {
            let mut diff = serde_json::Map::new();
            if let (Some(before_obj), Some(after_obj)) = (before.as_object(), after.as_object()) {
                for (key, after_val) in after_obj {
                    if let Some(before_val) = before_obj.get(key) {
                        if before_val != after_val {
                            diff.insert(
                                key.clone(),
                                serde_json::json!({ "from": before_val, "to": after_val })
                            );
                        }
                    } else {
                        diff.insert(key.clone(), serde_json::json!({ "added": after_val }));
                    }
                }
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
        let is_nullable = field.modifiers.iter().any(|m| matches!(m, Modifier::Nullable));
        let field_type = rust_type_from_schema(&field.type_name, is_nullable);

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

    let pk_field = model.fields.iter()
        .find(|f| f.modifiers.iter().any(|m| matches!(m, Modifier::PrimaryKey)));

    let field_gets = model.fields.iter().enumerate().map(|(idx, field)| {
        let field_name = format_ident!("{}", field.name);
        quote! { #field_name: row.get(#idx) }
    });

    let find_by_id_impl = if let Some(pk) = pk_field {
        let is_nullable = pk.modifiers.iter().any(|m| matches!(m, Modifier::Nullable));
        let pk_type = rust_type_from_schema(&pk.type_name, is_nullable);

        let pk_name = to_snake_case(&pk.name);

        quote! {
            pub async fn find_by_id(client: &Client, id: #pk_type)
                -> Result<Option<#model_name>, Box<dyn std::error::Error>>
            {
                let sql = format!("SELECT * FROM {} WHERE {} = $1", stringify!(#model_name).to_lowercase(), #pk_name);
                let row_opt = client.query_opt(&sql, &[&id]).await?;
                Ok(row_opt.map(|row| #model_name {
                    #(#field_gets),*
                }))
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
            where_fragments: Vec<(&'static str, usize)>,
            args: Vec<Box<dyn tokio_postgres::types::ToSql + Sync>>,
            limit: Option<usize>,
            offset: Option<usize>,
            order_by: Vec<(String, String)>,
        }
        impl Clone for #builder_name {
            fn clone(&self) -> Self {
                Self {
                    table: self.table.clone(),
                    where_fragments: self.where_fragments.clone(),
                    args: Vec::new(),
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

    let field_methods = model.fields.iter().enumerate().map(|(i, field)| {
        let method_name = format_ident!("where_{}", to_snake_case(&field.name));
        let is_nullable = field.modifiers.iter().any(|m| matches!(m, Modifier::Nullable));
        let field_type = rust_type_from_schema(&field.type_name, is_nullable);
        let field_col = to_snake_case(&field.name);

        quote! {
            pub fn #method_name(mut self, value: #field_type) -> Self {
                self.args.push(Box::new(value));
                self.where_fragments.push((#field_col, self.args.len()));
                self
            }
        }
    });

    let field_gets = model.fields.iter().enumerate().map(|(idx, field)| {
        let field_name = format_ident!("{}", field.name);
        quote! { #field_name: row.get(#idx) }
    });

    quote! {
        impl #builder_name {
            pub fn new() -> Self {
                Self {
                    table: #table_name.to_string(),
                    where_fragments: vec![],
                    args: vec![],
                    limit: None,
                    offset: None,
                    order_by: vec![],
                }
            }
            #(#field_methods)*
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
                let (sql, params) = self.build_select();
                let rows = client.query(&sql, &params[..]).await?;
                let mut results = Vec::new();
                for row in rows {
                    results.push(#model_name { #(#field_gets),* });
                }
                Ok(results)
            }

            pub async fn first(&self, client: &Client)
                -> Result<Option<#model_name>, Box<dyn std::error::Error>>
            {
                let mut query = #builder_name::new();
                query.table = self.table.clone();
                query.where_fragments = self.where_fragments.clone();
                query.args = Vec::new();
                query.limit = Some(1);
                query.offset = self.offset;
                query.order_by = self.order_by.clone();

                let results = query.select(client).await?;
                Ok(results.into_iter().next())
            }

            pub async fn count(&self, client: &Client)
                -> Result<i64, Box<dyn std::error::Error>>
            {
                let (sql, params) = self.build_count();
                let row = client.query_one(&sql, &params[..]).await?;
                Ok(row.get(0))
            }

            fn build_select(&self) -> (String, Vec<&(dyn tokio_postgres::types::ToSql + Sync)>) {
                let mut sql = format!("SELECT * FROM {}", self.table);
                let mut params: Vec<&(dyn tokio_postgres::types::ToSql + Sync)> = vec![];

                if !self.where_fragments.is_empty() {
                    let conds: Vec<String> = self.where_fragments.iter()
                        .enumerate()
                        .map(|(i, &(col, idx))| format!("{} = ${}", col, i + 1))
                        .collect();
                    sql.push_str(" WHERE ");
                    sql.push_str(&conds.join(" AND "));
                    for arg in &self.args {
                        params.push(arg.as_ref());
                    }
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
                (sql, params)
            }
            fn build_count(&self) -> (String, Vec<&(dyn tokio_postgres::types::ToSql + Sync)>) {
                let mut sql = format!("SELECT COUNT(*) FROM {}", self.table);
                let mut params: Vec<&(dyn tokio_postgres::types::ToSql + Sync)> = vec![];
                if !self.where_fragments.is_empty() {
                    let conds: Vec<String> = self.where_fragments.iter()
                        .enumerate()
                        .map(|(i, &(col, idx))| format!("{} = ${}", col, i + 1))
                        .collect();
                    sql.push_str(" WHERE ");
                    sql.push_str(&conds.join(" AND "));
                    for arg in &self.args {
                        params.push(arg.as_ref());
                    }
                }
                (sql, params)
            }
        }
    }
}

fn rust_type_from_schema(type_name: &str, nullable: bool) -> TokenStream {
    let base_type = match type_name {
        "BigInt" => quote! { i64 },
        "Int" => quote! { i32 },
        "String" => quote! { String },
        "JsonB" => quote! { serde_json::Value },
        "TimestamptZ" | "Timestamp" => quote! { DateTime<Utc> },
        "Boolean" => quote! { bool },
        "Float" => quote! { f64 },
        _ => quote! { String },
    };

    if nullable {
        quote! { Option<#base_type> }
    } else {
        base_type
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

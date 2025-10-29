use quote::{quote, format_ident};
use proc_macro2::TokenStream;
use crate::{Schema, Model, Field, Modifier};

pub fn generate_rust_code(schema: &Schema) -> String {
    let structs_and_impls = schema.models.iter().map(|model| {
        generate_model_with_query_builder(model)
    });

    let client_struct = generate_client_struct(schema);
    let jsonb_ext = generate_jsonb_ext();

    let code = quote! {
        use serde::{Deserialize, Serialize};
        use chrono::{DateTime, Utc};
        use tokio_postgres::{Client as PgClient, NoTls, Error};
        use std::sync::Arc;

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

        #jsonb_ext
        #client_struct
        #(#structs_and_impls)*
    };

    let file: syn::File = syn::parse2(code).unwrap();
    prettyplease::unparse(&file)
}

fn generate_jsonb_ext() -> TokenStream {
    quote! {
        /// Extension trait for easier JSONB field access
        pub trait JsonbExt {
            fn get_value<T>(&self, key: &str) -> Result<T, Box<dyn std::error::Error + Send + Sync>>
            where
                T: serde::de::DeserializeOwned;

            fn get_string(&self, key: &str) -> Result<String, Box<dyn std::error::Error + Send + Sync>>;

            fn get_i64(&self, key: &str) -> Result<i64, Box<dyn std::error::Error + Send + Sync>>;

            fn get_bool(&self, key: &str) -> Result<bool, Box<dyn std::error::Error + Send + Sync>>;

            fn get_or_default<T>(&self, key: &str, default: T) -> T
            where
                T: serde::de::DeserializeOwned;

            fn has_key(&self, key: &str) -> bool;
        }

        impl JsonbExt for serde_json::Value {
            fn get_value<T>(&self, key: &str) -> Result<T, Box<dyn std::error::Error + Send + Sync>>
            where
                T: serde::de::DeserializeOwned,
            {
                let value = self.get(key)
                    .ok_or_else(|| format!("Key '{}' not found", key))?;

                serde_json::from_value(value.clone())
                    .map_err(|e| format!("Failed to parse key '{}': {}", key, e).into())
            }

            fn get_string(&self, key: &str) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
                match self.get(key) {
                    Some(serde_json::Value::String(s)) => Ok(s.clone()),
                    Some(v) => serde_json::from_value(v.clone())
                        .map_err(|e| format!("Failed to parse '{}' as string: {}", key, e).into()),
                    None => Err(format!("Key '{}' not found", key).into()),
                }
            }

            fn get_i64(&self, key: &str) -> Result<i64, Box<dyn std::error::Error + Send + Sync>> {
                match self.get(key) {
                    Some(serde_json::Value::Number(n)) => n.as_i64()
                        .ok_or_else(|| format!("Key '{}' is not a valid i64", key).into()),
                    Some(v) => serde_json::from_value(v.clone())
                        .map_err(|e| format!("Failed to parse '{}' as i64: {}", key, e).into()),
                    None => Err(format!("Key '{}' not found", key).into()),
                }
            }

            fn get_bool(&self, key: &str) -> Result<bool, Box<dyn std::error::Error + Send + Sync>> {
                match self.get(key) {
                    Some(serde_json::Value::Bool(b)) => Ok(*b),
                    Some(v) => serde_json::from_value(v.clone())
                        .map_err(|e| format!("Failed to parse '{}' as bool: {}", key, e).into()),
                    None => Err(format!("Key '{}' not found", key).into()),
                }
            }

            fn get_or_default<T>(&self, key: &str, default: T) -> T
            where
                T: serde::de::DeserializeOwned,
            {
                self.get_value(key).unwrap_or(default)
            }

            fn has_key(&self, key: &str) -> bool {
                self.get(key).is_some()
            }
        }
    }
}

fn generate_client_struct(schema: &Schema) -> TokenStream {
    let model_accessors = schema.models.iter().map(|model| {
        let accessor_name = format_ident!("{}", to_snake_case(&model.name));
        let accessor_struct = format_ident!("{}Accessor", model.name);

        quote! {
            pub #accessor_name: #accessor_struct
        }
    });

    let accessor_structs = schema.models.iter().map(|model| {
        let model_name = format_ident!("{}", model.name);
        let accessor_struct = format_ident!("{}Accessor", model.name);
        let query_builder = format_ident!("{}Query", model.name);
        let table_name = model.name.to_lowercase();

        let pk_field = model.fields.iter()
            .find(|f| f.modifiers.iter().any(|m| matches!(m, Modifier::PrimaryKey)));

        let jsonb_fields: Vec<_> = model.fields.iter()
            .filter(|f| f.type_name == "JsonB")
            .collect();

        let find_unique = if let Some(pk) = pk_field {
            let is_nullable = pk.modifiers.iter().any(|m| matches!(m, Modifier::Nullable));
            let pk_type = rust_type_from_schema(&pk.type_name, is_nullable);

            quote! {
                pub async fn find_unique(&self, id: #pk_type)
                    -> Result<Option<#model_name>, Box<dyn std::error::Error + Send + Sync>>
                {
                    #model_name::find_by_id(&self.client, id).await
                }
            }
        } else {
            quote! {}
        };

        // Generate JSONB sub-accessors
        let jsonb_sub_accessors = if let Some(pk) = pk_field {
            let is_nullable = pk.modifiers.iter().any(|m| matches!(m, Modifier::Nullable));
            let pk_type = rust_type_from_schema(&pk.type_name, is_nullable);
            let pk_field_name = format_ident!("where_{}", to_snake_case(&pk.name));
            let pk_col_name = to_snake_case(&pk.name);

            jsonb_fields.iter().map(|jsonb| {
                let jsonb_name = &jsonb.name;
                let jsonb_snake = to_snake_case(jsonb_name);
                let jsonb_field_ident = format_ident!("{}", jsonb_name);
                let sub_accessor_struct = format_ident!("{}{}Accessor", model.name, capitalize_first(jsonb_name));

                let doc_struct = format!("Accessor for the `{}` JSONB field", jsonb_name);
                let doc_get = format!("Get a string value from `{}` by key", jsonb_name);
                let doc_get_as = format!("Get a typed value from `{}` by key", jsonb_name);
                let doc_get_or = format!("Get a value from `{}` with a default fallback", jsonb_name);
                let doc_has = format!("Check if key exists in `{}`", jsonb_name);
                let doc_set = format!("Set a value in `{}` by key (creates/updates the key)", jsonb_name);

                quote! {
                    #[doc = #doc_struct]
                    #[derive(Clone)]
                    pub struct #sub_accessor_struct {
                        client: Arc<PgClient>,
                    }

                    impl std::fmt::Debug for #sub_accessor_struct {
                        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                            f.debug_struct(stringify!(#sub_accessor_struct))
                                .field("client", &"<PgClient>")
                                .finish()
                        }
                    }

                    impl #sub_accessor_struct {
                        pub fn new(client: Arc<PgClient>) -> Self {
                            Self { client }
                        }

                        #[doc = #doc_get]
                        ///
                        /// # Example
                        /// ```
                        /// let value = client.guild_settings.settings.get(guild_id, "settingsLang").await?;
                        /// ```
                        pub async fn get(&self, id: #pk_type, key: &str)
                            -> Result<String, Box<dyn std::error::Error + Send + Sync>>
                        {
                            let record = #query_builder::new()
                                .#pk_field_name(id)
                                .first(&self.client)
                                .await?
                                .ok_or_else(|| format!("Record with id {:?} not found", id))?;

                            record.#jsonb_field_ident.get_string(key)
                        }

                        #[doc = #doc_get_as]
                        ///
                        /// # Example
                        /// ```
                        /// let count: i64 = client.guild_settings.settings.get_as(guild_id, "messageCount").await?;
                        /// let tags: Vec<String> = client.guild_settings.settings.get_as(guild_id, "tags").await?;
                        /// ```
                        pub async fn get_as<T>(&self, id: #pk_type, key: &str)
                            -> Result<T, Box<dyn std::error::Error + Send + Sync>>
                        where
                            T: serde::de::DeserializeOwned,
                        {
                            let record = #query_builder::new()
                                .#pk_field_name(id)
                                .first(&self.client)
                                .await?
                                .ok_or_else(|| format!("Record with id {:?} not found", id))?;

                            record.#jsonb_field_ident.get_value(key)
                        }

                        #[doc = #doc_get_or]
                        ///
                        /// # Example
                        /// ```
                        /// let prefix = client.guild_settings.settings.get_or(guild_id, "prefix", "!".to_string()).await?;
                        /// ```
                        pub async fn get_or<T>(&self, id: #pk_type, key: &str, default: T)
                            -> Result<T, Box<dyn std::error::Error + Send + Sync>>
                        where
                            T: serde::de::DeserializeOwned,
                        {
                            let record = #query_builder::new()
                                .#pk_field_name(id)
                                .first(&self.client)
                                .await?
                                .ok_or_else(|| format!("Record with id {:?} not found", id))?;

                            Ok(record.#jsonb_field_ident.get_or_default(key, default))
                        }

                        #[doc = #doc_has]
                        ///
                        /// # Example
                        /// ```
                        /// if client.guild_settings.settings.has(guild_id, "premium").await? {
                        ///     // Premium is configured
                        /// }
                        /// ```
                        pub async fn has(&self, id: #pk_type, key: &str)
                            -> Result<bool, Box<dyn std::error::Error + Send + Sync>>
                        {
                            let record = #query_builder::new()
                                .#pk_field_name(id)
                                .first(&self.client)
                                .await?
                                .ok_or_else(|| format!("Record with id {:?} not found", id))?;

                            Ok(record.#jsonb_field_ident.has_key(key))
                        }

                        #[doc = #doc_set]
                        ///
                        /// # Example
                        /// ```
                        /// client.guild_settings.settings.set(guild_id, "settingsLang", "en").await?;
                        /// ```
                        pub async fn set<T>(&self, id: #pk_type, key: &str, value: T)
                            -> Result<(), Box<dyn std::error::Error + Send + Sync>>
                        where
                            T: serde::Serialize + Send + Sync,
                        {
                            let value_json = serde_json::to_value(&value)?;
                            let value_str = value_json.to_string();

                            let sql = format!(
                                "INSERT INTO {} ({}, {}, updated_at) VALUES ($1, jsonb_build_object($2, $3), NOW()) \
                                 ON CONFLICT ({}) DO UPDATE SET {} = jsonb_set(COALESCE({}.{}, '{{}}'::jsonb), $4, $5, true), updated_at = NOW()",
                                #table_name,
                                #pk_col_name,
                                #jsonb_snake,
                                #pk_col_name,
                                #jsonb_snake,
                                #table_name,
                                #jsonb_snake
                            );

                            let key_path = format!("{{{}}}", key);
                            self.client.execute(
                                &sql,
                                &[&id, &key, &value_str, &key_path, &value_str]
                            ).await?;

                            Ok(())
                        }
                    }
                }
            }).collect::<Vec<_>>()
        } else {
            vec![]
        };

        // Fields for JSONB sub-accessors in main accessor
        let jsonb_accessor_fields = jsonb_fields.iter().map(|jsonb| {
            let jsonb_snake = to_snake_case(&jsonb.name);
            let sub_accessor_struct = format_ident!("{}{}Accessor", model.name, capitalize_first(&jsonb.name));
            let sub_accessor_field = format_ident!("{}", jsonb_snake);

            quote! {
                pub #sub_accessor_field: #sub_accessor_struct
            }
        });

        // Initialize JSONB sub-accessors
        let jsonb_accessor_inits = jsonb_fields.iter().map(|jsonb| {
            let jsonb_snake = to_snake_case(&jsonb.name);
            let sub_accessor_struct = format_ident!("{}{}Accessor", model.name, capitalize_first(&jsonb.name));
            let sub_accessor_field = format_ident!("{}", jsonb_snake);

            quote! {
                #sub_accessor_field: #sub_accessor_struct::new(client.clone())
            }
        });

        let jsonb_debug_fields = jsonb_fields.iter().map(|jsonb| {
            let jsonb_snake = to_snake_case(&jsonb.name);
            let sub_accessor_field = format_ident!("{}", jsonb_snake);

            quote! {
                .field(stringify!(#sub_accessor_field), &self.#sub_accessor_field)
            }
        });

        quote! {
            #(#jsonb_sub_accessors)*

            #[derive(Clone)]
            pub struct #accessor_struct {
                client: Arc<PgClient>,
                #(#jsonb_accessor_fields),*
            }

            impl std::fmt::Debug for #accessor_struct {
                fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                    f.debug_struct(stringify!(#accessor_struct))
                        .field("client", &"<PgClient>")
                        #(#jsonb_debug_fields)*
                        .finish()
                }
            }

            impl #accessor_struct {
                pub fn new(client: Arc<PgClient>) -> Self {
                    Self {
                        client: client.clone(),
                        #(#jsonb_accessor_inits),*
                    }
                }

                pub fn find_many(&self) -> #query_builder {
                    #query_builder::new()
                }

                #find_unique

                pub async fn find_first(&self) -> Result<Option<#model_name>, Box<dyn std::error::Error + Send + Sync>> {
                    #query_builder::new().first(&self.client).await
                }

                pub async fn count(&self) -> Result<i64, Box<dyn std::error::Error + Send + Sync>> {
                    #query_builder::new().count(&self.client).await
                }

                pub fn client(&self) -> &PgClient {
                    &self.client
                }
            }
        }
    });

    let accessor_inits = schema.models.iter().map(|model| {
        let accessor_name = format_ident!("{}", to_snake_case(&model.name));
        let accessor_struct = format_ident!("{}Accessor", model.name);

        quote! {
            #accessor_name: #accessor_struct::new(client.clone())
        }
    });

    let debug_accessor_fields = schema.models.iter().map(|model| {
        let accessor_name = to_snake_case(&model.name);
        let accessor_name_ident = format_ident!("{}", accessor_name);

        quote! {
            .field(#accessor_name, &self.#accessor_name_ident)
        }
    });

    quote! {
        #(#accessor_structs)*

        pub struct Client {
            client: Arc<PgClient>,
            #(#model_accessors),*
        }

        impl std::fmt::Debug for Client {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                f.debug_struct("Client")
                    .field("client", &"<PgClient>")
                    #(#debug_accessor_fields)*
                    .finish()
            }
        }

        impl Client {
            pub async fn new(connection_string: &str) -> Result<Self, Error> {
                let (client, connection) = tokio_postgres::connect(connection_string, NoTls).await?;

                tokio::spawn(async move {
                    if let Err(e) = connection.await {
                        eprintln!("connection error: {}", e);
                    }
                });

                let client = Arc::new(client);

                Ok(Self {
                    client: client.clone(),
                    #(#accessor_inits),*
                })
            }

            pub fn client(&self) -> &PgClient {
                &self.client
            }
        }
    }
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
            pub async fn find_by_id(client: &PgClient, id: #pk_type)
                -> Result<Option<#model_name>, Box<dyn std::error::Error + Send + Sync>>
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

        // Safety: All types that implement ToSql for common Rust types (i64, String, etc.) are Send
        unsafe impl Send for #builder_name {}

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

    let field_methods = model.fields.iter().map(|field| {
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

            pub async fn select(&self, client: &PgClient)
                -> Result<Vec<#model_name>, Box<dyn std::error::Error + Send + Sync>>
            {
                let (sql, params) = self.build_select();
                let rows = client.query(&sql, &params[..]).await?;
                let mut results = Vec::new();
                for row in rows {
                    results.push(#model_name { #(#field_gets),* });
                }
                Ok(results)
            }

            pub async fn first(&self, client: &PgClient)
                -> Result<Option<#model_name>, Box<dyn std::error::Error + Send + Sync>>
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

            pub async fn count(&self, client: &PgClient)
                -> Result<i64, Box<dyn std::error::Error + Send + Sync>>
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
                        .map(|(i, &(col, _idx))| format!("{} = ${}", col, i + 1))
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
                        .map(|(i, &(col, _idx))| format!("{} = ${}", col, i + 1))
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
        "Serial" => quote! { i32 },
        "Real" => quote! { f32 },
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

fn capitalize_first(s: &str) -> String {
    let mut chars = s.chars();
    match chars.next() {
        None => String::new(),
        Some(first) => first.to_uppercase().chain(chars).collect(),
    }
}

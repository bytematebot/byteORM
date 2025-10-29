use quote::{quote, format_ident};
use proc_macro2::TokenStream;
use crate::{Schema, Model, Field, Modifier};
use std::fs;
use std::collections::HashMap;

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

fn generate_client_struct(schema: &Schema, jsonb_defaults: &HashMap<(String, String), String>) -> TokenStream {
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
        let update_builder = format_ident!("{}Update", model.name);
        let table_name = model.name.to_lowercase();

        let pk_fields: Vec<_> = model.fields.iter()
            .filter(|f| f.modifiers.iter().any(|m| matches!(m, Modifier::PrimaryKey)))
            .collect();

        let jsonb_fields: Vec<_> = model.fields.iter()
            .filter(|f| f.type_name == "JsonB")
            .collect();

        let find_unique = if !pk_fields.is_empty() {
            if pk_fields.len() == 1 {
                let pk = &pk_fields[0];
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
                let pk_params = pk_fields.iter().map(|pk| {
                    let param_name = format_ident!("{}", to_snake_case(&pk.name));
                    let is_nullable = pk.modifiers.iter().any(|m| matches!(m, Modifier::Nullable));
                    let pk_type = rust_type_from_schema(&pk.type_name, is_nullable);
                    quote! { #param_name: #pk_type }
                });

                let pk_args = pk_fields.iter().map(|pk| {
                    let param_name = format_ident!("{}", to_snake_case(&pk.name));
                    quote! { #param_name }
                });

                quote! {
                    pub async fn find_unique(&self, #(#pk_params),*)
                        -> Result<Option<#model_name>, Box<dyn std::error::Error + Send + Sync>>
                    {
                        #model_name::find_by_composite_pk(&self.client, #(#pk_args),*).await
                    }
                }
            }
        } else {
            quote! {}
        };

        let find_or_create = if !pk_fields.is_empty() {
            if pk_fields.len() == 1 {
                let pk = &pk_fields[0];
                let is_nullable = pk.modifiers.iter().any(|m| matches!(m, Modifier::Nullable));
                let pk_type = rust_type_from_schema(&pk.type_name, is_nullable);
                let pk_col = to_snake_case(&pk.name);

                quote! {
                    pub async fn find_or_create(&self, id: #pk_type)
                        -> Result<#model_name, Box<dyn std::error::Error + Send + Sync>>
                    {
                        self.client.execute(
                            &format!("INSERT INTO {} ({}) VALUES ($1) ON CONFLICT DO NOTHING", #table_name, #pk_col),
                            &[&id]
                        ).await?;

                        self.find_unique(id)
                            .await?
                            .ok_or("Record should exist after find_or_create".into())
                    }
                }
            } else {
                let pk_params = pk_fields.iter().map(|pk| {
                    let param_name = format_ident!("{}", to_snake_case(&pk.name));
                    let is_nullable = pk.modifiers.iter().any(|m| matches!(m, Modifier::Nullable));
                    let pk_type = rust_type_from_schema(&pk.type_name, is_nullable);
                    quote! { #param_name: #pk_type }
                });

                let pk_cols: Vec<_> = pk_fields.iter().map(|pk| to_snake_case(&pk.name)).collect();
                let pk_cols_str = pk_cols.join(", ");
                let pk_placeholders: Vec<_> = (1..=pk_fields.len()).map(|i| format!("${}", i)).collect();
                let pk_placeholders_str = pk_placeholders.join(", ");
                let pk_conflict = pk_cols.join(", ");

                let pk_args = pk_fields.iter().map(|pk| {
                    let param_name = format_ident!("{}", to_snake_case(&pk.name));
                    quote! { &#param_name }
                });

                let pk_args_call = pk_fields.iter().map(|pk| {
                    let param_name = format_ident!("{}", to_snake_case(&pk.name));
                    quote! { #param_name }
                });

                quote! {
                    pub async fn find_or_create(&self, #(#pk_params),*)
                        -> Result<#model_name, Box<dyn std::error::Error + Send + Sync>>
                    {
                        let sql = format!(
                            "INSERT INTO {} ({}) VALUES ({}) ON CONFLICT ({}) DO NOTHING",
                            #table_name, #pk_cols_str, #pk_placeholders_str, #pk_conflict
                        );
                        self.client.execute(&sql, &[#(#pk_args),*]).await?;

                        self.find_unique(#(#pk_args_call),*)
                            .await?
                            .ok_or("Record should exist after find_or_create".into())
                    }
                }
            }
        } else {
            quote! {}
        };

        let jsonb_sub_accessors = if !pk_fields.is_empty() {
            jsonb_fields.iter().map(|jsonb| {
                let jsonb_name = &jsonb.name;
                let jsonb_snake = to_snake_case(jsonb_name);
                let jsonb_field_ident = format_ident!("{}", jsonb_name);
                let sub_accessor_struct = format_ident!("{}{}Accessor", model.name, capitalize_first(jsonb_name));
                let defaults_const = format_ident!("{}_DEFAULTS", jsonb_snake.to_uppercase());

                let default_json_init = if let Some(json_content) = jsonb_defaults.get(&(model.name.clone(), jsonb.name.clone())) {
                    quote! {
                        static #defaults_const: Lazy<serde_json::Value> = Lazy::new(|| {
                            serde_json::from_str(#json_content)
                                .expect(&format!("Failed to parse default JSON for {}.{}", stringify!(#model_name), #jsonb_name))
                        });
                    }
                } else {
                    quote! {
                        static #defaults_const: Lazy<serde_json::Value> = Lazy::new(|| {
                            serde_json::json!({})
                        });
                    }
                };

                let (pk_params, pk_where_methods, pk_args_for_set) = if pk_fields.len() == 1 {
                    let pk = &pk_fields[0];
                    let is_nullable = pk.modifiers.iter().any(|m| matches!(m, Modifier::Nullable));
                    let pk_type = rust_type_from_schema(&pk.type_name, is_nullable);
                    let pk_field_name = format_ident!("where_{}", to_snake_case(&pk.name));

                    (
                        quote! { id: #pk_type },
                        quote! { .#pk_field_name(id) },
                        vec![quote! { &id }],
                    )
                } else {
                    let params = pk_fields.iter().map(|pk| {
                        let param_name = format_ident!("{}", to_snake_case(&pk.name));
                        let is_nullable = pk.modifiers.iter().any(|m| matches!(m, Modifier::Nullable));
                        let pk_type = rust_type_from_schema(&pk.type_name, is_nullable);
                        quote! { #param_name: #pk_type }
                    });

                    let where_methods = pk_fields.iter().map(|pk| {
                        let method_name = format_ident!("where_{}", to_snake_case(&pk.name));
                        let param_name = format_ident!("{}", to_snake_case(&pk.name));
                        quote! { .#method_name(#param_name) }
                    });

                    let set_args = pk_fields.iter().map(|pk| {
                        let param_name = format_ident!("{}", to_snake_case(&pk.name));
                        quote! { &#param_name }
                    });

                    (
                        quote! { #(#params),* },
                        quote! { #(#where_methods)* },
                        set_args.collect::<Vec<_>>(),
                    )
                };

                let pk_args_clone = if pk_fields.len() == 1 {
                    quote! { id }
                } else {
                    let args = pk_fields.iter().map(|pk| {
                        let param_name = format_ident!("{}", to_snake_case(&pk.name));
                        quote! { #param_name }
                    });
                    quote! { #(#args),* }
                };

                let pk_columns: Vec<_> = pk_fields.iter().map(|pk| to_snake_case(&pk.name)).collect();
                let pk_placeholders: Vec<_> = (1..=pk_fields.len()).map(|i| format!("${}", i)).collect();
                let insert_pk_part = pk_columns.join(", ");
                let insert_values_part = pk_placeholders.join(", ");
                let conflict_clause = pk_columns.join(", ");

                let jsonb_field_param_idx = pk_fields.len() + 1;
                let key_param_idx = pk_fields.len() + 2;
                let value_param_idx = pk_fields.len() + 3;
                let key_path_param_idx = pk_fields.len() + 4;
                let value_param_idx2 = pk_fields.len() + 5;

                quote! {
                    #default_json_init

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

                        pub async fn get(&self, #pk_params, key: &str)
                            -> Result<String, Box<dyn std::error::Error + Send + Sync>>
                        {
                            match #query_builder::new()
                                #pk_where_methods
                                .first(&self.client)
                                .await
                            {
                                Ok(Some(record)) => {
                                    match record.#jsonb_field_ident.get_string(key) {
                                        Ok(value) => Ok(value),
                                        Err(_) => #defaults_const.get_string(key)
                                    }
                                },
                                Ok(None) => #defaults_const.get_string(key),
                                Err(e) => Err(e),
                            }
                        }

                        pub async fn get_as<T>(&self, #pk_params, key: &str)
                            -> Result<T, Box<dyn std::error::Error + Send + Sync>>
                        where
                            T: serde::de::DeserializeOwned,
                        {
                            match #query_builder::new()
                                #pk_where_methods
                                .first(&self.client)
                                .await
                            {
                                Ok(Some(record)) => {
                                    match record.#jsonb_field_ident.get_value(key) {
                                        Ok(value) => Ok(value),
                                        Err(_) => #defaults_const.get_value(key),
                                    }
                                },
                                Ok(None) => #defaults_const.get_value(key),
                                Err(e) => Err(e),
                            }
                        }

                        pub async fn get_or<T>(&self, #pk_params, key: &str, default: T)
                            -> Result<T, Box<dyn std::error::Error + Send + Sync>>
                        where
                            T: serde::de::DeserializeOwned,
                        {
                            match self.get_as(#pk_args_clone, key).await {
                                Ok(value) => Ok(value),
                                Err(_) => Ok(default),
                            }
                        }

                        pub async fn has(&self, #pk_params, key: &str)
                            -> Result<bool, Box<dyn std::error::Error + Send + Sync>>
                        {
                            match #query_builder::new()
                                #pk_where_methods
                                .first(&self.client)
                                .await
                            {
                                Ok(Some(record)) => Ok(record.#jsonb_field_ident.has_key(key) || #defaults_const.has_key(key)),
                                Ok(None) => Ok(#defaults_const.has_key(key)),
                                Err(e) => Err(e),
                            }
                        }

                        pub async fn set<T>(&self, #pk_params, key: &str, value: T)
                            -> Result<(), Box<dyn std::error::Error + Send + Sync>>
                        where
                            T: serde::Serialize + Send + Sync,
                        {
                            let value_json = serde_json::to_value(&value)?;
                            let value_str = value_json.to_string();

                            let sql = format!(
                                "INSERT INTO {} ({}, {}, updated_at) VALUES ({}, jsonb_build_object(${}, ${}), NOW()) \
                                 ON CONFLICT ({}) DO UPDATE SET {} = jsonb_set(COALESCE({}.{}, '{{}}'::jsonb), ${}, ${}, true), updated_at = NOW()",
                                #table_name,
                                #insert_pk_part,
                                #jsonb_snake,
                                #insert_values_part,
                                #key_param_idx,
                                #value_param_idx,
                                #conflict_clause,
                                #jsonb_snake,
                                #table_name,
                                #jsonb_snake,
                                #key_path_param_idx,
                                #value_param_idx2
                            );

                            let key_path = format!("{{{}}}", key);
                            self.client.execute(
                                &sql,
                                &[#(#pk_args_for_set),*, &key, &value_str, &key_path, &value_str]
                            ).await?;

                            Ok(())
                        }
                    }
                }
            }).collect::<Vec<_>>()
        } else {
            vec![]
        };

        let jsonb_accessor_fields = jsonb_fields.iter().map(|jsonb| {
            let jsonb_snake = to_snake_case(&jsonb.name);
            let sub_accessor_struct = format_ident!("{}{}Accessor", model.name, capitalize_first(&jsonb.name));
            let sub_accessor_field = format_ident!("{}", jsonb_snake);

            quote! {
                pub #sub_accessor_field: #sub_accessor_struct
            }
        });

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

                pub fn update(&self) -> #update_builder {
                    #update_builder::new(self.client.clone())
                }

                #find_unique
                #find_or_create

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
    let update_builder = generate_update_builder(model);
    let model_impl = generate_model_impl(model);

    quote! {
        #model_struct
        #query_builder_struct
        #update_builder
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

    let pk_fields: Vec<_> = model.fields.iter()
        .filter(|f| f.modifiers.iter().any(|m| matches!(m, Modifier::PrimaryKey)))
        .collect();

    let field_gets = model.fields.iter().enumerate().map(|(idx, field)| {
        let field_name = format_ident!("{}", field.name);
        quote! { #field_name: row.get(#idx) }
    });

    let find_by_id_impl = if !pk_fields.is_empty() {
        if pk_fields.len() == 1 {
            let pk = &pk_fields[0];
            let is_nullable = pk.modifiers.iter().any(|m| matches!(m, Modifier::Nullable));
            let pk_type = rust_type_from_schema(&pk.type_name, is_nullable);
            let pk_name = to_snake_case(&pk.name);

            quote! {
                pub async fn find_by_id(client: &PgClient, id: #pk_type)
                    -> Result<Option<#model_name>, Box<dyn std::error::Error + Send + Sync>>
                {
                    let sql = format!("SELECT * FROM {} WHERE {} = $1",
                        stringify!(#model_name).to_lowercase(), #pk_name);
                    let row_opt = client.query_opt(&sql, &[&id]).await?;
                    Ok(row_opt.map(|row| #model_name {
                        #(#field_gets),*
                    }))
                }
            }
        } else {
            let pk_params = pk_fields.iter().map(|pk| {
                let param_name = format_ident!("{}", to_snake_case(&pk.name));
                let is_nullable = pk.modifiers.iter().any(|m| matches!(m, Modifier::Nullable));
                let pk_type = rust_type_from_schema(&pk.type_name, is_nullable);
                quote! { #param_name: #pk_type }
            });

            let pk_conditions = pk_fields.iter().enumerate().map(|(i, pk)| {
                let pk_col = to_snake_case(&pk.name);
                let param_num = i + 1;
                format!("{} = ${}", pk_col, param_num)
            });
            let where_clause = pk_conditions.collect::<Vec<_>>().join(" AND ");

            let pk_args = pk_fields.iter().map(|pk| {
                let param_name = format_ident!("{}", to_snake_case(&pk.name));
                quote! { &#param_name }
            });

            quote! {
                pub async fn find_by_composite_pk(client: &PgClient, #(#pk_params),*)
                    -> Result<Option<#model_name>, Box<dyn std::error::Error + Send + Sync>>
                {
                    let sql = format!("SELECT * FROM {} WHERE {}",
                        stringify!(#model_name).to_lowercase(), #where_clause);
                    let row_opt = client.query_opt(&sql, &[#(#pk_args),*]).await?;
                    Ok(row_opt.map(|row| #model_name {
                        #(#field_gets),*
                    }))
                }
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

fn generate_update_builder(model: &Model) -> TokenStream {
    let model_name = format_ident!("{}", model.name);
    let update_builder_name = format_ident!("{}Update", model.name);
    let table_name = model.name.to_lowercase();

    let where_methods = model.fields.iter().map(|field| {
        let method_name = format_ident!("where_{}", to_snake_case(&field.name));
        let is_nullable = field.modifiers.iter().any(|m| matches!(m, Modifier::Nullable));
        let field_type = rust_type_from_schema(&field.type_name, is_nullable);
        let field_col = to_snake_case(&field.name);

        quote! {
            pub fn #method_name(mut self, value: #field_type) -> Self {
                self.where_args.push(Box::new(value));
                self.where_fragments.push((#field_col, self.where_args.len()));
                self
            }
        }
    });

    let set_methods = model.fields.iter().map(|field| {
        let method_name = format_ident!("set_{}", to_snake_case(&field.name));
        let is_nullable = field.modifiers.iter().any(|m| matches!(m, Modifier::Nullable));
        let field_type = rust_type_from_schema(&field.type_name, is_nullable);
        let field_col = to_snake_case(&field.name);

        quote! {
            pub fn #method_name(mut self, value: #field_type) -> Self {
                self.set_args.push(Box::new(value));
                self.set_fragments.push(#field_col);
                self
            }
        }
    });

    let field_gets = model.fields.iter().enumerate().map(|(idx, field)| {
        let field_name = format_ident!("{}", field.name);
        quote! { #field_name: row.get(#idx) }
    });

    quote! {
        pub struct #update_builder_name {
            client: Arc<PgClient>,
            table: String,
            where_fragments: Vec<(&'static str, usize)>,
            where_args: Vec<Box<dyn tokio_postgres::types::ToSql + Sync>>,
            set_fragments: Vec<&'static str>,
            set_args: Vec<Box<dyn tokio_postgres::types::ToSql + Sync>>,
        }

        unsafe impl Send for #update_builder_name {}

        impl #update_builder_name {
            pub fn new(client: Arc<PgClient>) -> Self {
                Self {
                    client,
                    table: #table_name.to_string(),
                    where_fragments: vec![],
                    where_args: vec![],
                    set_fragments: vec![],
                    set_args: vec![],
                }
            }

            #(#where_methods)*
            #(#set_methods)*

            pub async fn execute(self) -> Result<#model_name, Box<dyn std::error::Error + Send + Sync>> {
                if self.set_fragments.is_empty() {
                    return Err("No fields to update".into());
                }

                let mut sql = format!("UPDATE {} SET ", self.table);

                let set_clauses: Vec<String> = self.set_fragments.iter()
                    .enumerate()
                    .map(|(i, col)| format!("{} = ${}", col, i + 1))
                    .collect();
                sql.push_str(&set_clauses.join(", "));

                let mut all_params: Vec<&(dyn tokio_postgres::types::ToSql + Sync)> = vec![];
                for arg in &self.set_args {
                    all_params.push(arg.as_ref());
                }

                if !self.where_fragments.is_empty() {
                    let where_clauses: Vec<String> = self.where_fragments.iter()
                        .enumerate()
                        .map(|(i, &(col, _))| format!("{} = ${}", col, self.set_args.len() + i + 1))
                        .collect();
                    sql.push_str(" WHERE ");
                    sql.push_str(&where_clauses.join(" AND "));

                    for arg in &self.where_args {
                        all_params.push(arg.as_ref());
                    }
                }

                sql.push_str(" RETURNING *");

                let row = self.client.query_one(&sql, &all_params[..]).await?;
                Ok(#model_name {
                    #(#field_gets),*
                })
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

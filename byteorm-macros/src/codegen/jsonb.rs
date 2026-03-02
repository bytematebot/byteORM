use crate::types::*;
use crate::codegen::utils::*;
use proc_macro2::TokenStream;
use quote::{format_ident, quote};

pub fn generate_jsonb_accessor_fields(model: &Model) -> Vec<(proc_macro2::Ident, proc_macro2::Ident)> {
    model.fields.iter()
        .filter(|f| (f.type_name == "JsonB" || f.type_name == "Jsonb")
            && f.attributes.iter().any(|a| a.name == "jsonb_default"))
        .map(|f| {
            let field_name = format_ident!("{}", to_snake_case(&f.name));
            let struct_name = format_ident!("{}{}Accessor", model.name, capitalize_first(&f.name));
            (field_name, struct_name)
        })
        .collect()
}

pub fn generate_jsonb_sub_accessors(model: &Model) -> Vec<TokenStream> {
    let model_name_str = &model.name;
    let query_builder = format_ident!("{}Query", model.name);
    let where_builder_name = format_ident!("{}WhereBuilder", model.name);
    let table_name = &model.table_name;

    let pk_fields: Vec<_> = model.fields.iter()
        .filter(|f| f.modifiers.iter().any(|m| matches!(m, Modifier::PrimaryKey)))
        .collect();

    let jsonb_fields: Vec<_> = model.fields.iter()
        .filter(|f| (f.type_name == "JsonB" || f.type_name == "Jsonb")
            && f.attributes.iter().any(|a| a.name == "jsonb_default"))
        .collect();

    jsonb_fields.into_iter().map(|jsonb| {
        let jsonb_name = &jsonb.name;
        let jsonb_snake = to_snake_case(jsonb_name);
        let jsonb_field_ident = format_ident!("{}", jsonb_name);
        let sub_accessor_struct = format_ident!("{}{}Accessor", model.name, capitalize_first(jsonb_name));
        let defaults_const = format_ident!("{}_DEFAULTS", jsonb_snake.to_uppercase());

        let is_nullable = jsonb.modifiers.iter().any(|m| matches!(m, Modifier::Nullable));

        let json_content = jsonb.attributes.iter()
            .find(|a| a.name == "jsonb_default")
            .and_then(|a| a.args.as_ref());

        let default_json_init = if let Some(content) = json_content {
            quote! {
                static #defaults_const: Lazy<serde_json::Value> = Lazy::new(|| {
                    serde_json::from_str(#content)
                        .expect(&format!("Failed to parse default JSON for {}.{}", #model_name_str, #jsonb_name))
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
            let is_pk_nullable = pk.modifiers.iter().any(|m| matches!(m, Modifier::Nullable));
            let pk_type = rust_type_from_schema(&pk.type_name, is_pk_nullable);
            let pk_field_name = format_ident!("where_{}", to_snake_case(&pk.name));
            (
                quote! { id: #pk_type },
                quote! { .#pk_field_name(id) },
                vec![quote! { &id }],
            )
        } else {
            let params = pk_fields.iter().map(|pk| {
                let param_name = format_ident!("{}", to_snake_case(&pk.name));
                let is_pk_nullable = pk.modifiers.iter().any(|m| matches!(m, Modifier::Nullable));
                let pk_type = rust_type_from_schema(&pk.type_name, is_pk_nullable);
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

        let (pk_field_type, pk_field_ident, pk_field_name_in) = if pk_fields.len() == 1 {
            let pk = &pk_fields[0];
            let is_pk_nullable = pk.modifiers.iter().any(|m| matches!(m, Modifier::Nullable));
            let pk_type = rust_type_from_schema(&pk.type_name, is_pk_nullable);
            let pk_ident = format_ident!("{}", to_snake_case(&pk.name));
            let pk_method_in = format_ident!("where_{}_in", to_snake_case(&pk.name));
            (pk_type, pk_ident, pk_method_in)
        } else {
            (quote! { () }, format_ident!("_unused"), format_ident!("_unused"))
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

        let (_, _, pk_columns, pk_placeholders, _) = pk_args(model);
        let insert_pk_part = pk_columns.join(", ");
        let insert_values_part = pk_placeholders.join(", ");
        let conflict_clause = pk_columns.join(", ");
        let key_placeholder = format!("${}", pk_columns.len() + 1);
        let value_placeholder = format!("${}", pk_columns.len() + 2);

        let get_all_body = if is_nullable {
            quote! {
                Ok(Some(record)) => Ok(record.#jsonb_field_ident.clone().unwrap_or_else(|| #defaults_const.clone())),
            }
        } else {
            quote! {
                Ok(Some(record)) => Ok(record.#jsonb_field_ident.clone()),
            }
        };

        let get_body = if is_nullable {
            quote! {
                Ok(Some(record)) => {
                    if let Some(ref jsonb_val) = record.#jsonb_field_ident {
                        jsonb_val.get_string(key)
                            .or_else(|_| #defaults_const.get_string(key))
                    } else {
                        #defaults_const.get_string(key)
                    }
                },
            }
        } else {
            quote! {
                Ok(Some(record)) => {
                    record.#jsonb_field_ident.get_string(key)
                        .or_else(|_| #defaults_const.get_string(key))
                },
            }
        };

        let get_as_body = if is_nullable {
            quote! {
                Ok(Some(record)) => {
                    if let Some(ref jsonb_val) = record.#jsonb_field_ident {
                        jsonb_val.get_value(key)
                            .or_else(|_| #defaults_const.get_value(key))
                    } else {
                        #defaults_const.get_value(key)
                    }
                },
            }
        } else {
            quote! {
                Ok(Some(record)) => {
                    record.#jsonb_field_ident.get_value(key)
                        .or_else(|_| #defaults_const.get_value(key))
                },
            }
        };

        let has_body = if is_nullable {
            quote! {
                Ok(Some(record)) => {
                    if let Some(ref jsonb_val) = record.#jsonb_field_ident {
                        Ok(jsonb_val.has_key(key) || #defaults_const.has_key(key))
                    } else {
                        Ok(#defaults_const.has_key(key))
                    }
                },
            }
        } else {
            quote! {
                Ok(Some(record)) => Ok(record.#jsonb_field_ident.has_key(key) || #defaults_const.has_key(key)),
            }
        };

        let get_many_for_loop = if is_nullable {
            quote! {
                if let Some(record) = opt {
                    if let Some(ref jsonb_val) = record.#jsonb_field_ident {
                        for &key in keys {
                            if let Some(v) = jsonb_val.get(key) {
                                out.insert(key.to_string(), v.clone());
                            } else if let Some(v) = #defaults_const.get(key) {
                                out.insert(key.to_string(), v.clone());
                            }
                        }
                    } else {
                        for &key in keys {
                            if let Some(v) = #defaults_const.get(key) {
                                out.insert(key.to_string(), v.clone());
                            }
                        }
                    }
                } else {
                    for &key in keys {
                        if let Some(v) = #defaults_const.get(key) {
                            out.insert(key.to_string(), v.clone());
                        }
                    }
                }
            }
        } else {
            quote! {
                if let Some(record) = opt {
                    for &key in keys {
                        if let Some(v) = record.#jsonb_field_ident.get(key) {
                            out.insert(key.to_string(), v.clone());
                        } else if let Some(v) = #defaults_const.get(key) {
                            out.insert(key.to_string(), v.clone());
                        }
                    }
                } else {
                    for &key in keys {
                        if let Some(v) = #defaults_const.get(key) {
                            out.insert(key.to_string(), v.clone());
                        }
                    }
                }
            }
        };

        let get_many_ids_for_loop = if is_nullable {
            quote! {
                for record in records {
                    if let Some(ref jsonb_val) = record.#jsonb_field_ident {
                        if let Ok(value) = jsonb_val.get_value::<T>(key) {
                            map.insert(record.#pk_field_ident, value);
                        } else if let Ok(value) = #defaults_const.get_value::<T>(key) {
                            map.insert(record.#pk_field_ident, value);
                        }
                    } else if let Ok(value) = #defaults_const.get_value::<T>(key) {
                        map.insert(record.#pk_field_ident, value);
                    }
                }
            }
        } else {
            quote! {
                for record in records {
                    if let Ok(value) = record.#jsonb_field_ident.get_value::<T>(key) {
                        map.insert(record.#pk_field_ident, value);
                    } else if let Ok(value) = #defaults_const.get_value::<T>(key) {
                        map.insert(record.#pk_field_ident, value);
                    }
                }
            }
        };

        quote! {
            #default_json_init

            #[derive(Clone)]
            pub struct #sub_accessor_struct {
                pool: ConnectionPool,
            }

            impl std::fmt::Debug for #sub_accessor_struct {
                fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                    f.debug_struct(stringify!(#sub_accessor_struct))
                        .field("pool", &"<bb8::Pool>")
                        .finish()
                }
            }

            impl #sub_accessor_struct {
                pub fn new(pool: ConnectionPool) -> Self {
                    Self { pool }
                }

                pub async fn get_all(&self, #pk_params)
                    -> Result<serde_json::Value, Box<dyn std::error::Error + Send + Sync>>
                {
                    match #query_builder::from_builder(
                        self.pool.clone(),
                        #where_builder_name::new()
                            #pk_where_methods
                    )
                        .first()
                        .await
                    {
                        #get_all_body
                        Ok(None) => Ok(#defaults_const.clone()),
                        Err(e) => Err(e),
                    }
                }

                pub async fn get_all_as<T>(&self, #pk_params)
                    -> Result<T, Box<dyn std::error::Error + Send + Sync>>
                where
                    T: serde::de::DeserializeOwned,
                {
                    let value = self.get_all(#pk_args_clone).await?;
                    Ok(serde_json::from_value(value)?)
                }

                pub async fn get(&self, #pk_params, key: &str)
                    -> Result<String, Box<dyn std::error::Error + Send + Sync>>
                {
                    match #query_builder::from_builder(
                        self.pool.clone(),
                        #where_builder_name::new()
                            #pk_where_methods
                    )
                        .first()
                        .await
                    {
                        #get_body
                        Ok(None) => #defaults_const.get_string(key),
                        Err(e) => Err(e),
                    }
                }

                pub async fn get_as<T>(&self, #pk_params, key: &str)
                    -> Result<T, Box<dyn std::error::Error + Send + Sync>>
                where
                    T: serde::de::DeserializeOwned,
                {
                    match #query_builder::from_builder(
                        self.pool.clone(),
                        #where_builder_name::new()
                            #pk_where_methods
                    )
                        .first()
                        .await
                    {
                        #get_as_body
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
                    match #query_builder::from_builder(
                        self.pool.clone(),
                        #where_builder_name::new()
                            #pk_where_methods
                    )
                        .first()
                        .await
                    {
                        #has_body
                        Ok(None) => Ok(#defaults_const.has_key(key)),
                        Err(e) => Err(e),
                    }
                }

                pub async fn set<T>(&self, #pk_params, key: &str, value: T)
                    -> Result<(), Box<dyn std::error::Error + Send + Sync>>
                where
                    T: serde::Serialize + Send + Sync,
                {
                    let value_json = serde_json::to_value(&value)
                        .map_err(|e| format!("Failed to serialize value for key '{}': {}", key, e))?;

                    let sql = format!(
                        "INSERT INTO {} ({}, {}, updated_at) VALUES ({}, jsonb_set('{{}}'::jsonb, string_to_array({}, '.')::text[], {}::jsonb, true), NOW()) \
                         ON CONFLICT ({}) DO UPDATE SET {} = jsonb_set(COALESCE({}.{}, '{{}}'::jsonb), string_to_array({}, '.')::text[], {}::jsonb, true), updated_at = NOW()",
                        #table_name,
                        #insert_pk_part,
                        #jsonb_snake,
                        #insert_values_part,
                        #key_placeholder,
                        #value_placeholder,
                        #conflict_clause,
                        #jsonb_snake,
                        #table_name,
                        #jsonb_snake,
                        #key_placeholder,
                        #value_placeholder,
                    );

                    let client = self.pool.get().await
                        .map_err(|e| format!("Failed to get database connection from pool: {}", e))?;

                    let params = vec![#(#pk_args_for_set),*, &key as &(dyn tokio_postgres::types::ToSql + Sync), &value_json as &(dyn tokio_postgres::types::ToSql + Sync)];
                    debug::log_query(&sql, params.len());

                    client.execute(
                        &sql,
                        &params[..]
                    ).await
                        .map_err(|e| format!("Database error setting key '{}': {} (SQL: {}, value: {:?})", key, e, sql, value_json))?;

                    Ok(())
                }

                pub async fn get_many(
                    &self, #pk_params, keys: &[&str]
                ) -> Result<HashMap<String, serde_json::Value>, Box<dyn std::error::Error + Send + Sync>>
                {
                    let opt = #query_builder::from_builder(
                        self.pool.clone(),
                        #where_builder_name::new()
                            #pk_where_methods
                    )
                        .first().await?;

                    let mut out = HashMap::new();

                    #get_many_for_loop
                    Ok(out)
                }

                pub async fn get_many_as<T>(
                    &self, #pk_params, keys: &[&str]
                ) -> Result<HashMap<String, T>, Box<dyn std::error::Error + Send + Sync>>
                where T: serde::de::DeserializeOwned
                {
                    let values = self.get_many(#pk_args_clone, keys).await?;
                    let mut map = HashMap::new();
                    for (k, v) in values {
                        if let Ok(x) = serde_json::from_value::<T>(v) {
                            map.insert(k, x);
                        }
                    }
                    Ok(map)
                }

                pub async fn get_many_ids<T>(
                    &self, ids: &[#pk_field_type], key: &str
                ) -> Result<HashMap<#pk_field_type, T>, Box<dyn std::error::Error + Send + Sync>>
                where T: serde::de::DeserializeOwned
                {
                    if ids.is_empty() {
                        return Ok(HashMap::new());
                    }

                    let records = #query_builder::from_builder(
                        self.pool.clone(),
                        #where_builder_name::new()
                            .#pk_field_name_in(ids.to_vec())
                    )
                        .await?;

                    let mut map = HashMap::new();
                    #get_many_ids_for_loop
                    Ok(map)
                }
            }
        }
    }).collect()
}

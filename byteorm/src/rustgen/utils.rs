use std::collections::HashMap;
use quote::{quote, format_ident};
use proc_macro2::TokenStream;
use crate::Modifier;

pub fn rust_type_from_schema(type_name: &str, nullable: bool) -> TokenStream {
    let base_type = match type_name {
        "BigInt"      => quote! { i64 },
        "Int"         => quote! { i32 },
        "String"      => quote! { String },
        "JsonB"       => quote! { serde_json::Value },
        "TimestamptZ" | "Timestamp" => quote! { DateTime<Utc> },
        "Boolean"     => quote! { bool },
        "Float"       => quote! { f64 },
        "Serial"      => quote! { i32 },
        "Real"        => quote! { f32 },
        _             => quote! { String },
    };

    if nullable {
        quote! { Option<#base_type> }
    } else {
        base_type
    }
}

pub fn to_snake_case(s: &str) -> String {
    let mut result = String::new();
    for (i, ch) in s.chars().enumerate() {
        if ch.is_uppercase() && i > 0 {
            result.push('_');
        }
        result.push(ch.to_lowercase().next().unwrap_or(ch));
    }
    result
}

pub fn capitalize_first(s: &str) -> String {
    let mut chars = s.chars();
    match chars.next() {
        None => String::new(),
        Some(first) => first.to_uppercase().chain(chars).collect(),
    }
}

pub fn pk_args(model: &crate::Model) -> (Vec<proc_macro2::Ident>, Vec<proc_macro2::TokenStream>, Vec<String>, Vec<String>, Vec<proc_macro2::TokenStream>) {
    let pk_fields: Vec<_> = model.fields.iter()
        .filter(|f| f.modifiers.iter().any(|m| matches!(m, Modifier::PrimaryKey)))
        .collect();
    let pk_names = pk_fields.iter().map(|pk| format_ident!("{}", to_snake_case(&pk.name))).collect();
    let pk_types = pk_fields.iter().map(|pk| {
        let is_nullable = pk.modifiers.iter().any(|m| matches!(m, Modifier::Nullable));
        rust_type_from_schema(&pk.type_name, is_nullable)
    }).collect();
    let pk_cols: Vec<_> = pk_fields.iter().map(|pk| to_snake_case(&pk.name)).collect();
    let pk_placeholders: Vec<_> = (1..=pk_fields.len()).map(|i| format!("${}", i)).collect();
    let pk_arg_refs = pk_fields.iter().map(|pk| {
        let name = format_ident!("{}", to_snake_case(&pk.name));
        quote! { &#name }
    }).collect();
    (pk_names, pk_types, pk_cols, pk_placeholders, pk_arg_refs)
}

pub fn generate_jsonb_sub_accessors(model: &crate::Model, jsonb_defaults: &HashMap<(String, String), String>) -> Vec<TokenStream> {
    let model_name = &model.name;
    let query_builder = format_ident!("{}Query", model.name);
    let where_builder_name = format_ident!("{}WhereBuilder", model.name);
    let table_name = model.name.to_lowercase();

    let pk_fields: Vec<_> = model.fields.iter()
        .filter(|f| f.modifiers.iter().any(|m| matches!(m, Modifier::PrimaryKey)))
        .collect();

    let jsonb_fields: Vec<_> = model.fields.iter()
        .filter(|f| f.type_name == "JsonB")
        .collect();

    jsonb_fields.into_iter().map(|jsonb| {
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

        let (_, _, pk_columns, pk_placeholders, _) = pk_args(model);
        let insert_pk_part = pk_columns.join(", ");
        let insert_values_part = pk_placeholders.join(", ");
        let conflict_clause = pk_columns.join(", ");

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
                    match #query_builder::from_builder(
                        self.client.clone(),
                        #where_builder_name::new()
                            #pk_where_methods
                    )
                        .first()
                        .await
                    {
                        Ok(Some(record)) => {
                            record.#jsonb_field_ident.get_string(key)
                                .or_else(|_| #defaults_const.get_string(key))
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
                    match #query_builder::from_builder(
                        self.client.clone(),
                        #where_builder_name::new()
                            #pk_where_methods
                    )
                        .first()
                        .await
                    {
                        Ok(Some(record)) => {
                            record.#jsonb_field_ident.get_value(key)
                                .or_else(|_| #defaults_const.get_value(key))
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
                    match #query_builder::from_builder(
                        self.client.clone(),
                        #where_builder_name::new()
                            #pk_where_methods
                    )
                        .first()
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
                        "INSERT INTO {} ({}, {}, updated_at) VALUES ({}, jsonb_build_object($1, $2), NOW()) \
                         ON CONFLICT ({}) DO UPDATE SET {} = jsonb_set(COALESCE({}.{}, '{{}}'::jsonb), $3, $4, true), updated_at = NOW()",
                        #table_name,
                        #insert_pk_part,
                        #jsonb_snake,
                        #insert_values_part,
                        #conflict_clause,
                        #jsonb_snake,
                        #table_name,
                        #jsonb_snake,
                    );

                    let key_path = format!("{{{}}}", key);
                    self.client.execute(
                        &sql,
                        &[#(#pk_args_for_set),*, &key, &value_str, &key_path, &value_str]
                    ).await?;

                    Ok(())
                }

                pub async fn get_many(
                    &self, #pk_params, keys: &[&str]
                ) -> Result<HashMap<String, serde_json::Value>, Box<dyn std::error::Error + Send + Sync>>
                {
                    let opt = #query_builder::from_builder(
                        self.client.clone(),
                        #where_builder_name::new()
                            #pk_where_methods
                    )
                        .first().await?;

                    let mut out = HashMap::new();

                    if let Some(record) = opt {
                        for &key in keys {
                            if let Some(v) = record.#jsonb_field_ident.get(key) {
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
            }
        }
    }).collect()
}

pub fn generate_field_gets(model: &crate::Model) -> impl Iterator<Item = TokenStream> {
    model.fields.iter().enumerate().map(|(idx, field)| {
        let field_name = format_ident!("{}", field.name);
        quote! { #field_name: row.get(#idx) }
    })
}

pub fn is_numeric_type(ty: &str) -> bool {
    matches!(ty, "BigInt" | "Int" | "Serial" | "Float" | "Real")
}

pub fn generate_inc_methods(model: &crate::Model, target_ops: &str, target_values: Option<&str>) -> impl Iterator<Item = TokenStream> {
    model.fields.iter()
        .filter(|f| is_numeric_type(&f.type_name))
        .map(move |field| {
            let field_col = to_snake_case(&field.name);
            let inc_method = format_ident!("inc_{}", field_col);
            let dec_method = format_ident!("dec_{}", field_col);
            let mul_method = format_ident!("mul_{}", field_col);
            let div_method = format_ident!("div_{}", field_col);
            let ops_ident = format_ident!("{}", target_ops);
            let values_ident = target_values.map(|v| format_ident!("{}", v));

            let inc_body = if let Some(values) = &values_ident {
                quote! {
                    self.#ops_ident.insert(#field_col, ("inc", amount));
                    self.#values.insert(#field_col, Box::new(amount));
                }
            } else {
                quote! {
                    self.#ops_ident.push((#field_col, "inc", amount));
                }
            };

            let dec_body = if let Some(values) = &values_ident {
                quote! {
                    self.#ops_ident.insert(#field_col, ("dec", amount));
                    self.#values.insert(#field_col, Box::new(-amount));
                }
            } else {
                quote! {
                    self.#ops_ident.push((#field_col, "dec", amount));
                }
            };

            let mul_body = if let Some(values) = &values_ident {
                quote! {
                    self.#ops_ident.insert(#field_col, ("mul", factor));
                    self.#values.insert(#field_col, Box::new(0));
                }
            } else {
                quote! {
                    self.#ops_ident.push((#field_col, "mul", factor));
                }
            };

            let div_body = if let Some(values) = &values_ident {
                quote! {
                    self.#ops_ident.insert(#field_col, ("div", divisor));
                    self.#values.insert(#field_col, Box::new(0));
                }
            } else {
                quote! {
                    self.#ops_ident.push((#field_col, "div", divisor));
                }
            };

            quote! {
                pub fn #inc_method(mut self, amount: i64) -> Self {
                    #inc_body
                    self
                }
                pub fn #dec_method(mut self, amount: i64) -> Self {
                    #dec_body
                    self
                }
                pub fn #mul_method(mut self, factor: i64) -> Self {
                    #mul_body
                    self
                }
                pub fn #div_method(mut self, divisor: i64) -> Self {
                    #div_body
                    self
                }
            }
        })
}

pub fn generate_where_methods(model: &crate::Model, target_args: &str, target_fragments: &str) -> impl Iterator<Item = TokenStream> {
    model.fields.iter().map(move |field| {
        let method_name = format_ident!("where_{}", to_snake_case(&field.name));
        let is_nullable = field.modifiers.iter().any(|m| matches!(m, Modifier::Nullable));
        let field_type = rust_type_from_schema(&field.type_name, is_nullable);
        let field_col = to_snake_case(&field.name);
        let args_ident = format_ident!("{}", target_args);
        let fragments_ident = format_ident!("{}", target_fragments);

        quote! {
            pub fn #method_name(mut self, value: #field_type) -> Self {
                self.#args_ident.push(Box::new(value) as Box<dyn tokio_postgres::types::ToSql + Sync + Send>);
                self.#fragments_ident.push((#field_col, self.#args_ident.len()));
                self
            }
        }
    })
}

pub fn generate_set_methods(model: &crate::Model, use_hashmap: bool, hashmap_field: &str, vec_field: Option<&str>, fragments_field: Option<&str>) -> impl Iterator<Item = TokenStream> {
    model.fields.iter().map(move |field| {
        let method_name = format_ident!("set_{}", to_snake_case(&field.name));
        let is_nullable = field.modifiers.iter().any(|m| matches!(m, Modifier::Nullable));
        let field_type = rust_type_from_schema(&field.type_name, is_nullable);
        let field_col = to_snake_case(&field.name);

        let body = if use_hashmap {
            let hashmap_ident = format_ident!("{}", hashmap_field);
            quote! {
                self.#hashmap_ident.insert(#field_col, Box::new(value));
            }
        } else {
            let vec_ident = format_ident!("{}", vec_field.unwrap());
            let fragments_ident = format_ident!("{}", fragments_field.unwrap());
            quote! {
                self.#vec_ident.push(Box::new(value) as Box<dyn tokio_postgres::types::ToSql + Sync + Send>);
                self.#fragments_ident.push(#field_col);
            }
        };

        quote! {
            pub fn #method_name(mut self, value: #field_type) -> Self {
                #body
                self
            }
        }
    })
}


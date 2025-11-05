use std::collections::HashMap;
use proc_macro2::TokenStream;
use quote::{format_ident, quote};
use crate::rustgen::{capitalize_first, generate_jsonb_sub_accessors, pk_args, rust_type_from_schema, to_snake_case, is_numeric_type};
use crate::{Modifier, Schema};

fn generate_find_unique(model_name: &proc_macro2::Ident, model: &crate::Model) -> TokenStream {
    let pk_fields: Vec<_> = model.fields.iter()
        .filter(|f| f.modifiers.iter().any(|m| matches!(m, Modifier::PrimaryKey)))
        .collect();

    if pk_fields.is_empty() {
        quote! {}
    } else if pk_fields.len() == 1 {
        let pk = &pk_fields[0];
        let is_nullable = pk.modifiers.iter().any(|m| matches!(m, Modifier::Nullable));
        let pk_type = rust_type_from_schema(&pk.type_name, is_nullable);
        quote! {
            pub async fn find_unique(&self, id: #pk_type)
                -> Result<Option<#model_name>, Box<dyn std::error::Error + Send + Sync>>
            {
                #model_name::find_by_id(self.pool.clone(), id).await
            }
        }
    } else {
        let pk_params = pk_fields.iter().map(|pk| {
            let name = format_ident!("{}", to_snake_case(&pk.name));
            let is_nullable = pk.modifiers.iter().any(|m| matches!(m, Modifier::Nullable));
            let pk_type = rust_type_from_schema(&pk.type_name, is_nullable);
            quote! { #name: #pk_type }
        });

        let pk_args = pk_fields.iter().map(|pk| {
            let name = format_ident!("{}", to_snake_case(&pk.name));
            quote! { #name }
        });

        quote! {
            pub async fn find_unique(&self, #(#pk_params),*)
                -> Result<Option<#model_name>, Box<dyn std::error::Error + Send + Sync>>
            {
                #model_name::find_by_composite_pk(self.pool.clone(), #(#pk_args),*).await
            }
        }
    }
}

fn generate_find_or_create(model_name: &proc_macro2::Ident, model: &crate::Model, table_name: &str) -> TokenStream {
    let pk_fields: Vec<_> = model.fields.iter()
        .filter(|f| f.modifiers.iter().any(|m| matches!(m, Modifier::PrimaryKey)))
        .collect();

    if pk_fields.is_empty() {
        quote! {}
    } else if pk_fields.len() == 1 {
        let pk = &pk_fields[0];
        let is_nullable = pk.modifiers.iter().any(|m| matches!(m, Modifier::Nullable));
        let pk_type = rust_type_from_schema(&pk.type_name, is_nullable);
        let pk_col = to_snake_case(&pk.name);
        quote! {
            pub async fn find_or_create(&self, id: #pk_type)
                -> Result<#model_name, Box<dyn std::error::Error + Send + Sync>>
            {
                let sql = format!("INSERT INTO {} ({}) VALUES ($1) ON CONFLICT ({}) DO NOTHING", #table_name, #pk_col, #pk_col);
                debug::log_query(&sql, 1);
                let client = self.pool.get().await.map_err(|_| "Failed to get connection from pool")?;
                client.execute(&sql, &[&id]).await?;
                self.find_unique(id).await?.ok_or("Record should exist after find_or_create".into())
            }
        }
    } else {
        let (_, _, pk_cols, pk_placeholders, pk_arg_refs) = pk_args(model);

        let pk_cols_str = pk_cols.join(", ");
        let pk_placeholders_str = pk_placeholders.join(", ");

        let pk_params = pk_fields.iter().map(|pk| {
            let name = format_ident!("{}", to_snake_case(&pk.name));
            let is_nullable = pk.modifiers.iter().any(|m| matches!(m, Modifier::Nullable));
            let pk_type = rust_type_from_schema(&pk.type_name, is_nullable);
            quote! { #name: #pk_type }
        });

        let pk_args_call = pk_fields.iter().map(|pk| {
            let name = format_ident!("{}", to_snake_case(&pk.name));
            quote! { #name }
        });

        quote! {
            pub async fn find_or_create(&self, #(#pk_params),*)
                -> Result<#model_name, Box<dyn std::error::Error + Send + Sync>>
            {
                let sql = format!(
                    "INSERT INTO {} ({}) VALUES ({}) ON CONFLICT ({}) DO NOTHING",
                    #table_name, #pk_cols_str, #pk_placeholders_str, #pk_cols_str
                );
                debug::log_query(&sql, #pk_cols_str.split(", ").count());
                let client = self.pool.get().await.map_err(|_| "Failed to get connection from pool")?;
                client.execute(&sql, &[#(#pk_arg_refs),*]).await?;
                self.find_unique(#(#pk_args_call),*).await?.ok_or("Record should exist after find_or_create".into())
            }
        }
    }
}

pub fn generate_client_struct(schema: &Schema, jsonb_defaults: &HashMap<(String, String), String>) -> TokenStream {
    let model_accessors = schema.models.iter().map(|model| {
        let accessor_name = format_ident!("{}", to_snake_case(&model.name));
        let accessor_struct = format_ident!("{}Accessor", model.name);
        quote! { pub #accessor_name: #accessor_struct }
    });

    let accessor_structs = schema.models.iter().map(|model| {
        let model_name = format_ident!("{}", model.name);
        let accessor_struct = format_ident!("{}Accessor", model.name);
        let query_builder = format_ident!("{}Query", model.name);
        let update_builder = format_ident!("{}Update", model.name);
        let upsert_builder = format_ident!("{}Upsert", model.name);
        let create_builder = format_ident!("{}Create", model.name);
        let delete_builder = format_ident!("{}Delete", model.name);
        let table_name = model.name.to_lowercase();

        let find_unique = generate_find_unique(&model_name, model);
        let find_or_create = generate_find_or_create(&model_name, model, &table_name);

        let jsonb_fields: Vec<_> = model.fields.iter()
            .filter(|f| f.type_name == "JsonB")
            .collect();

        let jsonb_accessor_fields = jsonb_fields.iter().map(|jsonb| {
            let jsonb_snake = to_snake_case(&jsonb.name);
            let sub_accessor_struct = format_ident!("{}{}Accessor", model.name, capitalize_first(&jsonb.name));
            let sub_accessor_field = format_ident!("{}", jsonb_snake);
            quote! { pub #sub_accessor_field: #sub_accessor_struct }
        });

        let jsonb_accessor_inits = jsonb_fields.iter().map(|jsonb| {
            let jsonb_snake = to_snake_case(&jsonb.name);
            let sub_accessor_struct = format_ident!("{}{}Accessor", model.name, capitalize_first(&jsonb.name));
            let sub_accessor_field = format_ident!("{}", jsonb_snake);
            quote! { #sub_accessor_field: #sub_accessor_struct::new(pool.clone()) }
        });

        let jsonb_debug_fields = jsonb_fields.iter().map(|jsonb| {
            let jsonb_snake = to_snake_case(&jsonb.name);
            let sub_accessor_field = format_ident!("{}", jsonb_snake);
            quote! { .field(stringify!(#sub_accessor_field), &self.#sub_accessor_field) }
        });

        let jsonb_sub_accessors = generate_jsonb_sub_accessors(model, jsonb_defaults);
        let where_builder = format_ident!("{}WhereBuilder", model.name);

        let computed_methods = std::iter::empty::<TokenStream>();

        let typed_agg_methods = {
            let field_methods = model.fields.iter()
                .filter(|f| is_numeric_type(&f.type_name))
                .map(|field| {
                    let col_snake = to_snake_case(&field.name);
                    let sum_name = format_ident!("sum_{}", col_snake);
                    match field.type_name.as_str() {
                        "Int" | "Serial" => {
                            quote! {
                                pub async fn #sum_name<F>(&self, f: F) -> Result<i64, Box<dyn std::error::Error + Send + Sync>>
                                where
                                    F: FnOnce(#where_builder) -> #where_builder,
                                {
                                    let builder = f(#where_builder::new());
                                    #query_builder::from_builder(self.pool.clone(), builder).sum::<i64>(#col_snake).await
                                }
                            }
                        }
                        "BigInt" => {
                            quote! {
                                pub async fn #sum_name<F>(&self, f: F) -> Result<i64, Box<dyn std::error::Error + Send + Sync>>
                                where
                                    F: FnOnce(#where_builder) -> #where_builder,
                                {
                                    let builder = f(#where_builder::new());
                                    #query_builder::from_builder(self.pool.clone(), builder).sum_cast_i64(#col_snake).await
                                }
                            }
                        }
                        "Float" => {
                            quote! {
                                pub async fn #sum_name<F>(&self, f: F) -> Result<f64, Box<dyn std::error::Error + Send + Sync>>
                                where
                                    F: FnOnce(#where_builder) -> #where_builder,
                                {
                                    let builder = f(#where_builder::new());
                                    #query_builder::from_builder(self.pool.clone(), builder).sum::<f64>(#col_snake).await
                                }
                            }
                        }
                        "Real" => {
                            quote! {
                                pub async fn #sum_name<F>(&self, f: F) -> Result<f32, Box<dyn std::error::Error + Send + Sync>>
                                where
                                    F: FnOnce(#where_builder) -> #where_builder,
                                {
                                    let builder = f(#where_builder::new());
                                    #query_builder::from_builder(self.pool.clone(), builder).sum::<f32>(#col_snake).await
                                }
                            }
                        }
                        _ => quote! {},
                    }
                });
            quote! { #(#field_methods)* }
        };

        quote! {
            #(#jsonb_sub_accessors)*

            #[derive(Clone)]
            pub struct #accessor_struct {
                pool: Arc<bb8::Pool<bb8_postgres::PostgresConnectionManager<tokio_postgres::NoTls>>>,
                #(#jsonb_accessor_fields),*
            }
            impl std::fmt::Debug for #accessor_struct {
                fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                    f.debug_struct(stringify!(#accessor_struct))
                        .field("pool", &"<bb8::Pool>")
                        #(#jsonb_debug_fields)*
                        .finish()
                }
            }

            impl #accessor_struct {
                pub fn new(pool: Arc<bb8::Pool<bb8_postgres::PostgresConnectionManager<tokio_postgres::NoTls>>>) -> Self {
                    Self {
                        pool: pool.clone(),
                        #(#jsonb_accessor_inits),*
                    }
                }
                pub async fn find_many<F>(&self, f: F) -> Result<Vec<#model_name>, Box<dyn std::error::Error + Send + Sync>>
                where
                    F: FnOnce(#where_builder) -> #where_builder,
                {
                    let builder = f(#where_builder::new());
                    #query_builder::from_builder(self.pool.clone(), builder).await
                }
                pub async fn find_first<F>(&self, f: F) -> Result<Option<#model_name>, Box<dyn std::error::Error + Send + Sync>>
                where
                    F: FnOnce(#where_builder) -> #where_builder,
                {
                    let builder = f(#where_builder::new());
                    #query_builder::from_builder(self.pool.clone(), builder).first().await
                }
                pub async fn sum<F, T>(&self, f: F, field: &str) -> Result<T, Box<dyn std::error::Error + Send + Sync>>
                where
                    F: FnOnce(#where_builder) -> #where_builder,
                    T: for<'a> tokio_postgres::types::FromSql<'a> + Default + tokio_postgres::types::ToSql + Sync,
                {
                    let builder = f(#where_builder::new());
                    #query_builder::from_builder(self.pool.clone(), builder).sum(field).await
                }
                pub async fn count<F>(&self, f: F) -> Result<i64, Box<dyn std::error::Error + Send + Sync>>
                where
                    F: FnOnce(#where_builder) -> #where_builder,
                {
                    let builder = f(#where_builder::new());
                    #query_builder::from_builder(self.pool.clone(), builder).count().await
                }
                pub async fn avg<F, T>(&self, f: F, field: &str) -> Result<Option<T>, Box<dyn std::error::Error + Send + Sync>>
                where
                    F: FnOnce(#where_builder) -> #where_builder,
                    T: for<'a> tokio_postgres::types::FromSql<'a>,
                {
                    let builder = f(#where_builder::new());
                    #query_builder::from_builder(self.pool.clone(), builder).avg::<T>(field).await
                }
                pub async fn min<F, T>(&self, f: F, field: &str) -> Result<Option<T>, Box<dyn std::error::Error + Send + Sync>>
                where
                    F: FnOnce(#where_builder) -> #where_builder,
                    T: for<'a> tokio_postgres::types::FromSql<'a>,
                {
                    let builder = f(#where_builder::new());
                    #query_builder::from_builder(self.pool.clone(), builder).min::<T>(field).await
                }
                pub async fn max<F, T>(&self, f: F, field: &str) -> Result<Option<T>, Box<dyn std::error::Error + Send + Sync>>
                where
                    F: FnOnce(#where_builder) -> #where_builder,
                    T: for<'a> tokio_postgres::types::FromSql<'a>,
                {
                    let builder = f(#where_builder::new());
                    #query_builder::from_builder(self.pool.clone(), builder).max::<T>(field).await
                }
                #typed_agg_methods
                pub fn update<F>(&self, f: F) -> #update_builder
                where
                    F: FnOnce(#update_builder) -> #update_builder,
                {
                    let builder = #update_builder::new(self.pool.clone());
                    f(builder)
                }
                pub fn upsert<F>(&self, f: F) -> #upsert_builder
                where
                    F: FnOnce(#upsert_builder) -> #upsert_builder,
                {
                    let builder = #upsert_builder::new(self.pool.clone());
                    f(builder)
                }
                pub fn create<F>(&self, f: F) -> #create_builder
                where
                    F: FnOnce(#create_builder) -> #create_builder,
                {
                    let builder = #create_builder::new(self.pool.clone());
                    f(builder)
                }
                pub fn delete<F>(&self, f: F) -> #delete_builder
                where
                    F: FnOnce(#delete_builder) -> #delete_builder,
                {
                    let builder = #delete_builder::new(self.pool.clone());
                    f(builder)
                }
                #find_unique
                #find_or_create
                pub async fn get_client(&self) -> Result<bb8::PooledConnection<'_, bb8_postgres::PostgresConnectionManager<tokio_postgres::NoTls>>, tokio_postgres::Error> {
                    self.pool.get().await.map_err(|_| tokio_postgres::Error::__private_api_timeout())
                }
                pub fn pool(&self) -> &bb8::Pool<bb8_postgres::PostgresConnectionManager<tokio_postgres::NoTls>> {
                    &self.pool
                }
            }
        }
    });

    let accessor_inits = schema.models.iter().map(|model| {
        let accessor_name = format_ident!("{}", to_snake_case(&model.name));
        let accessor_struct = format_ident!("{}Accessor", model.name);
        quote! { #accessor_name: #accessor_struct::new(pool.clone()) }
    });

    let debug_accessor_fields = schema.models.iter().map(|model| {
        let accessor_name = to_snake_case(&model.name);
        let accessor_name_ident = format_ident!("{}", accessor_name);
        quote! { .field(#accessor_name, &self.#accessor_name_ident) }
    });

    quote! {
        #(#accessor_structs)*

        pub struct Client {
            pool: Arc<bb8::Pool<bb8_postgres::PostgresConnectionManager<tokio_postgres::NoTls>>>,
            #(#model_accessors),*
        }
        impl std::fmt::Debug for Client {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                f.debug_struct("Client")
                    .field("pool", &"<bb8::Pool>")
                    #(#debug_accessor_fields)*
                    .finish()
            }
        }
        impl Client {
            pub async fn new(connection_string: &str) -> Result<Self, Error> {
                let manager = bb8_postgres::PostgresConnectionManager::new_from_stringlike(
                    connection_string,
                    NoTls,
                )?;
                let pool = bb8::Pool::builder()
                    .max_size(20)
                    .build(manager)
                    .await?;
                let pool = Arc::new(pool);
                Ok(Self {
                    pool: pool.clone(),
                    #(#accessor_inits),*
                })
            }
            pub async fn get_client(&self) -> Result<bb8::PooledConnection<'_, bb8_postgres::PostgresConnectionManager<NoTls>>, Error> {
                self.pool.get().await.map_err(|e| Error::__private_api_timeout())
            }
            pub fn pool(&self) -> &bb8::Pool<bb8_postgres::PostgresConnectionManager<NoTls>> { &self.pool }
        }
    }
}

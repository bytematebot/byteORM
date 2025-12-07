use crate::rustgen::{
    generate_field_gets, generate_set_methods, generate_where_methods, rust_type_from_schema,
    to_snake_case,
};
use crate::{Model, Modifier};
use proc_macro2::TokenStream;
use quote::{format_ident, quote};

pub fn generate_create_builder(model: &Model) -> TokenStream {
    let model_name = format_ident!("{}", model.name);
    let create_builder_name = format_ident!("{}Create", model.name);
    let table_name = model.name.to_lowercase();

    let required_fields: Vec<String> = model
        .fields
        .iter()
        .filter(|field| {
            !field.attributes.iter().any(|a| a.name == "default")
                && !field
                    .modifiers
                    .iter()
                    .any(|m| matches!(m, Modifier::Nullable))
                && field.type_name != "Serial"
        })
        .map(|field| to_snake_case(&field.name))
        .collect();

    let where_methods = generate_where_methods(model, "where_args", "where_fragments");

    let set_methods = generate_set_methods(model, true, "set_values", None, None);

    let field_gets = generate_field_gets(model);

    quote! {
        pub struct #create_builder_name {
            pool: Arc<bb8::Pool<bb8_postgres::PostgresConnectionManager<tokio_postgres_rustls::MakeRustlsConnect>>>,
            table: String,
            where_fragments: Vec<(String, usize)>,
            where_args: Vec<Box<dyn tokio_postgres::types::ToSql + Sync + Send>>,
            set_values: std::collections::HashMap<&'static str, Box<dyn tokio_postgres::types::ToSql + Sync + Send>>,
            fut: Option<std::pin::Pin<Box<dyn std::future::Future<Output = Result<#model_name, Box<dyn std::error::Error + Send + Sync>>> + Send>>>,
        }

        unsafe impl Send for #create_builder_name {}

        impl #create_builder_name {
            pub fn new(pool: Arc<bb8::Pool<bb8_postgres::PostgresConnectionManager<tokio_postgres_rustls::MakeRustlsConnect>>>) -> Self {
                Self {
                    pool,
                    table: #table_name.to_string(),
                    where_fragments: vec![],
                    where_args: vec![],
                    set_values: std::collections::HashMap::new(),
                    fut: None,
                }
            }

            #(#where_methods)*
            #(#set_methods)*
        }

        impl std::future::Future for #create_builder_name {
            type Output = Result<#model_name, Box<dyn std::error::Error + Send + Sync>>;
            fn poll(mut self: std::pin::Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> std::task::Poll<Self::Output> {
                let me = &mut *self;

                if me.fut.is_none() {
                    let required_fields = vec![#(#required_fields),*];

                    for req in &required_fields {
                        if !me.set_values.contains_key(req) {
                            return std::task::Poll::Ready(Err(format!("Missing required field: {}", req).into()));
                        }
                    }

                    if me.set_values.is_empty() && !required_fields.is_empty() {
                        return std::task::Poll::Ready(Err("No fields to create".into()));
                    }

                    let pool = me.pool.clone();
                    let table = me.table.clone();
                    let where_fragments = std::mem::take(&mut me.where_fragments);
                    let where_args = std::mem::take(&mut me.where_args);
                    let set_values = std::mem::take(&mut me.set_values);

                    let fut = async move {
                        let client = pool.get().await.map_err(|_| "Failed to get connection from pool")?;

                        if !where_fragments.is_empty() {
                            let mut sql = format!("SELECT COUNT(*) FROM {}", table);
                            let mut params: Vec<&(dyn tokio_postgres::types::ToSql + Sync)> = vec![];
                            let conds: Vec<String> = where_fragments.iter()
                                .enumerate()
                                .map(|(i, (col, idx))| format!("{} = ${}", col, i + 1))
                                .collect();
                            sql.push_str(" WHERE ");
                            sql.push_str(&conds.join(" AND "));
                            for arg in &where_args {
                                params.push(arg.as_ref());
                            }
                            let row = client.query_one(&sql, &params[..]).await?;
                            let count: i64 = row.get(0);
                            if count > 0 {
                                return Err("Record already exists".into());
                            }
                        }

                        let mut columns: Vec<&str> = set_values.keys().copied().collect();
                        columns.sort();

                        let columns_str = columns.join(", ");
                        let placeholders: Vec<String> = (1..=columns.len())
                            .map(|i| format!("${}", i))
                            .collect();
                        let placeholders_str = placeholders.join(", ");

                        let mut params: Vec<&(dyn tokio_postgres::types::ToSql + Sync)> = vec![];
                        for col in &columns {
                            params.push(set_values.get(col).unwrap().as_ref());
                        }

                        let sql = format!(
                            "INSERT INTO {} ({}) VALUES ({}) RETURNING *",
                            table, columns_str, placeholders_str
                        );

                        debug::log_query(&sql, params.len());

                        let row = client.query_one(&sql, &params[..]).await?;
                        Ok(#model_name {
                            #(#field_gets),*
                        })
                    };
                    me.fut = Some(Box::pin(fut));
                }

                me.fut.as_mut().unwrap().as_mut().poll(cx)
            }
        }
    }
}

use crate::Model;
use crate::rustgen::generate_where_methods;
use proc_macro2::TokenStream;
use quote::{format_ident, quote};

pub fn generate_delete_builder(model: &Model) -> TokenStream {
    let model_name = format_ident!("{}", model.name);
    let delete_builder_name = format_ident!("{}Delete", model.name);
    let table_name = model.name.to_lowercase();

    let where_methods = generate_where_methods(model, "where_args", "where_fragments");

    quote! {
        pub struct #delete_builder_name {
            pool: Arc<bb8::Pool<bb8_postgres::PostgresConnectionManager<tokio_postgres_rustls::MakeRustlsConnect>>>,
            table: String,
            where_fragments: Vec<(String, usize)>,
            where_args: Vec<Box<dyn tokio_postgres::types::ToSql + Sync + Send>>,
            fut: Option<std::pin::Pin<Box<dyn std::future::Future<Output = Result<u64, Box<dyn std::error::Error + Send + Sync>>> + Send>>>,
        }

        unsafe impl Send for #delete_builder_name {}

        impl #delete_builder_name {
            pub fn new(pool: Arc<bb8::Pool<bb8_postgres::PostgresConnectionManager<tokio_postgres_rustls::MakeRustlsConnect>>>) -> Self {
                Self {
                    pool,
                    table: #table_name.to_string(),
                    where_fragments: vec![],
                    where_args: vec![],
                    fut: None,
                }
            }

            #(#where_methods)*
        }

        impl std::future::Future for #delete_builder_name {
            type Output = Result<u64, Box<dyn std::error::Error + Send + Sync>>;
            fn poll(mut self: std::pin::Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> std::task::Poll<Self::Output> {
                let me = &mut *self;

                if me.fut.is_none() {
                    if me.where_fragments.is_empty() {
                        return std::task::Poll::Ready(Err("DELETE without WHERE clause is not allowed".into()));
                    }

                    let mut sql = format!("DELETE FROM {}", me.table);
                    let conds: Vec<String> = me.where_fragments.iter()
                        .enumerate()
                        .map(|(i, (col, idx))| {
                            format!("{} = ${}", col, i + 1)
                        })
                        .collect();
                    sql.push_str(" WHERE ");
                    sql.push_str(&conds.join(" AND "));

                    let mut all_params: Vec<Box<dyn tokio_postgres::types::ToSql + Sync + Send>> = vec![];
                    for arg in std::mem::take(&mut me.where_args) {
                        all_params.push(arg);
                    }

                    let pool = me.pool.clone();
                    let fut = async move {
                        debug::log_query(&sql, all_params.len());
                        let client = pool.get().await.map_err(|_| "Failed to get connection from pool")?;
                        let params: Vec<&(dyn tokio_postgres::types::ToSql + Sync)> =
                            all_params.iter().map(|b| b.as_ref() as &(dyn tokio_postgres::types::ToSql + Sync)).collect();
                        let count = client.execute(&sql, &params[..]).await?;
                        Ok(count)
                    };
                    me.fut = Some(Box::pin(fut));
                }

                me.fut.as_mut().unwrap().as_mut().poll(cx)
            }
        }
    }
}

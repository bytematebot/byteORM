use crate::rustgen::{
    generate_field_gets, generate_inc_methods, generate_set_methods, generate_where_methods,
    is_numeric_type, rust_type_from_schema, to_snake_case,
};
use crate::{Model, Modifier};
use proc_macro2::TokenStream;
use quote::{format_ident, quote};

pub fn generate_update_builder(model: &Model) -> TokenStream {
    let model_name = format_ident!("{}", model.name);
    let update_builder_name = format_ident!("{}Update", model.name);
    let table_name = model.name.to_lowercase();

    let where_methods = generate_where_methods(model, "where_args", "where_fragments");

    let set_methods =
        generate_set_methods(model, false, "", Some("set_args"), Some("set_fragments"));

    let inc_methods = generate_inc_methods(model, "inc_ops", None);

    let field_gets = generate_field_gets(model);

    quote! {
        pub struct #update_builder_name {
            pool: Arc<bb8::Pool<bb8_postgres::PostgresConnectionManager<tokio_postgres_rustls::MakeRustlsConnect>>>,
            table: String,
            where_fragments: Vec<(String, usize)>,
            where_args: Vec<Box<dyn tokio_postgres::types::ToSql + Sync + Send>>,
            set_fragments: Vec<&'static str>,
            set_args: Vec<Box<dyn tokio_postgres::types::ToSql + Sync + Send>>,
            inc_ops: Vec<(&'static str, &'static str, i64)>,
            fut: Option<std::pin::Pin<Box<dyn std::future::Future<Output = Result<#model_name, Box<dyn std::error::Error + Send + Sync>>> + Send>>>,
        }

        unsafe impl Send for #update_builder_name {}

        impl #update_builder_name {
            pub fn new(pool: Arc<bb8::Pool<bb8_postgres::PostgresConnectionManager<tokio_postgres_rustls::MakeRustlsConnect>>>) -> Self {
                Self {
                    pool,
                    table: #table_name.to_string(),
                    where_fragments: vec![],
                    where_args: vec![],
                    set_fragments: vec![],
                    set_args: vec![],
                    inc_ops: vec![],
                    fut: None,
                }
            }

            #(#where_methods)*
            #(#set_methods)*
            #(#inc_methods)*
        }

        impl std::future::Future for #update_builder_name {
            type Output = Result<#model_name, Box<dyn std::error::Error + Send + Sync>>;
            fn poll(mut self: std::pin::Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> std::task::Poll<Self::Output> {
                let me = &mut *self;

                if me.fut.is_none() {
                    if me.set_fragments.is_empty() && me.inc_ops.is_empty() {
                        return std::task::Poll::Ready(Err("No fields to update".into()));
                    }

                    let mut sql = format!("UPDATE {} SET ", me.table);
                    let mut set_clauses: Vec<String> = vec![];
                    let mut param_idx = 1;
                    for col in me.set_fragments.iter() {
                        set_clauses.push(format!("{} = ${}", col, param_idx));
                        param_idx += 1;
                    }
                    for (field, op, _) in &me.inc_ops {
                        let clause = match *op {
                            "inc" => format!("{} = {} + ${}", field, field, param_idx),
                            "dec" => format!("{} = {} - ${}", field, field, param_idx),
                            "mul" => format!("{} = {} * ${}", field, field, param_idx),
                            "div" => format!("{} = {} / ${}", field, field, param_idx),
                            _ => continue,
                        };
                        set_clauses.push(clause);
                        param_idx += 1;
                    }
                    sql.push_str(&set_clauses.join(", "));

                    let mut all_params: Vec<Box<dyn tokio_postgres::types::ToSql + Sync + Send>> = vec![];
                    for arg in std::mem::take(&mut me.set_args) {
                        all_params.push(arg);
                    }
                    for (_, _, val) in &me.inc_ops {
                        all_params.push(Box::new(*val));
                    }

                    if !me.where_fragments.is_empty() {
                        let where_clauses: Vec<String> = me.where_fragments.iter()
                            .enumerate()
                            .map(|(i, (col, _))| format!(
                                "{} = ${}", col, param_idx + i))
                            .collect();
                        sql.push_str(" WHERE ");
                        sql.push_str(&where_clauses.join(" AND "));
                        for arg in std::mem::take(&mut me.where_args) {
                            all_params.push(arg);
                        }
                    }
                    sql.push_str(" RETURNING *");

                    let pool = me.pool.clone();
                    let fut = async move {
                        debug::log_query(&sql, all_params.len());
                        let client = pool.get().await.map_err(|_| "Failed to get connection from pool")?;
                        let params: Vec<&(dyn tokio_postgres::types::ToSql + Sync)> =
                            all_params.iter().map(|b| b.as_ref() as &(dyn tokio_postgres::types::ToSql + Sync)).collect();
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

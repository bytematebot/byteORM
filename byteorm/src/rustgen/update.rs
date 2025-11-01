use proc_macro2::TokenStream;
use quote::{format_ident, quote};
use crate::{Model, Modifier};
use crate::rustgen::{generate_field_gets, generate_inc_methods, generate_set_methods, generate_where_methods, is_numeric_type, rust_type_from_schema, to_snake_case};

pub fn generate_update_builder(model: &Model) -> TokenStream {
    let model_name = format_ident!("{}", model.name);
    let update_builder_name = format_ident!("{}Update", model.name);
    let table_name = model.name.to_lowercase();

    let where_methods = generate_where_methods(model, "where_args", "where_fragments");

    let set_methods = generate_set_methods(model, false, "", Some("set_args"), Some("set_fragments"));

    let inc_methods = generate_inc_methods(model, "inc_ops", None);

    let field_gets = generate_field_gets(model);

    quote! {
        pub struct #update_builder_name {
            client: Arc<PgClient>,
            table: String,
            where_fragments: Vec<(&'static str, usize)>,
            where_args: Vec<Box<dyn tokio_postgres::types::ToSql + Sync + Send>>,
            set_fragments: Vec<&'static str>,
            set_args: Vec<Box<dyn tokio_postgres::types::ToSql + Sync + Send>>,
            inc_ops: Vec<(&'static str, &'static str, i64)>,
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
                    inc_ops: vec![],
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
                let fut = async move {
                    if me.set_fragments.is_empty() && me.inc_ops.is_empty() {
                        return Err("No fields to update".into());
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
                    let mut all_params: Vec<&(dyn tokio_postgres::types::ToSql + Sync)> =
                        me.set_args.iter().map(|a| a.as_ref() as &(dyn tokio_postgres::types::ToSql + Sync)).collect();
                    for (_, _, val) in &me.inc_ops {
                        all_params.push(val);
                    }
                    if !me.where_fragments.is_empty() {
                        let where_clauses: Vec<String> = me.where_fragments.iter()
                            .enumerate()
                            .map(|(i, &(col, _))| format!(
                                "{} = ${}", col, me.set_args.len() + me.inc_ops.len() + i + 1))
                            .collect();
                        sql.push_str(" WHERE ");
                        sql.push_str(&where_clauses.join(" AND "));
                        for arg in &me.where_args {
                            all_params.push(arg.as_ref() as &(dyn tokio_postgres::types::ToSql + Sync));
                        }
                    }
                    sql.push_str(" RETURNING *");
                    let row = me.client.query_one(&sql, &all_params[..]).await?;
                    Ok(#model_name {
                        #(#field_gets),*
                    })
                };
                let mut fut = std::pin::pin!(fut);
                std::future::Future::poll(fut.as_mut(), cx)
            }
        }
    }
}

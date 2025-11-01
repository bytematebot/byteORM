use proc_macro2::TokenStream;
use quote::{format_ident, quote};
use crate::Model;
use crate::rustgen::{generate_where_methods};

pub fn generate_delete_builder(model: &Model) -> TokenStream {
    let model_name = format_ident!("{}", model.name);
    let delete_builder_name = format_ident!("{}Delete", model.name);
    let table_name = model.name.to_lowercase();

    let where_methods = generate_where_methods(model, "where_args", "where_fragments");

    quote! {
        pub struct #delete_builder_name {
            client: Arc<PgClient>,
            table: String,
            where_fragments: Vec<(&'static str, usize)>,
            where_args: Vec<Box<dyn tokio_postgres::types::ToSql + Sync + Send>>,
            polled: bool,
        }

        unsafe impl Send for #delete_builder_name {}

        impl #delete_builder_name {
            pub fn new(client: Arc<PgClient>) -> Self {
                Self {
                    client,
                    table: #table_name.to_string(),
                    where_fragments: vec![],
                    where_args: vec![],
                    polled: false,
                }
            }

            #(#where_methods)*
        }

        impl std::future::Future for #delete_builder_name {
            type Output = Result<u64, Box<dyn std::error::Error + Send + Sync>>;
            fn poll(mut self: std::pin::Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> std::task::Poll<Self::Output> {
                if self.polled {
                    panic!("future polled more than once");
                }
                self.polled = true;

                let client = self.client.clone();
                let table = self.table.clone();
                let where_fragments = std::mem::take(&mut self.where_fragments);
                let where_args = std::mem::take(&mut self.where_args);

                let fut = async move {
                    if where_fragments.is_empty() {
                        return Err("DELETE without WHERE clause is not allowed".into());
                    }

                    let mut sql = format!("DELETE FROM {}", table);
                    let mut params: Vec<&(dyn tokio_postgres::types::ToSql + Sync)> = vec![];
                    let conds: Vec<String> = where_fragments.iter()
                        .enumerate()
                        .map(|(i, &(col, idx))| {
                            format!("{} = ${}", col, i + 1)
                        })
                        .collect();
                    sql.push_str(" WHERE ");
                    sql.push_str(&conds.join(" AND "));
                    for arg in &where_args {
                        params.push(arg.as_ref());
                    }

                    let count = client.execute(&sql, &params[..]).await?;
                    Ok(count)
                };

                let mut pinned = std::pin::pin!(fut);
                match std::future::Future::poll(pinned.as_mut(), cx) {
                    std::task::Poll::Ready(res) => std::task::Poll::Ready(res),
                    std::task::Poll::Pending => std::task::Poll::Pending,
                }
            }
        }
    }
}
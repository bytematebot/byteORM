use proc_macro2::TokenStream;
use quote::{format_ident, quote};
use crate::{Model, Modifier};
use crate::rustgen::{rust_type_from_schema, to_snake_case};

pub fn generate_query_builder_struct(model: &Model) -> TokenStream {
    let model_name = format_ident!("{}", model.name);
    let builder_name = format_ident!("{}Query", model.name);
    let table_name = model.name.to_lowercase();

    let where_methods = model.fields.iter().map(|field| {
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

    let order_by_methods = model.fields.iter().map(|field| {
        let asc_method = format_ident!("order_by_{}_asc", to_snake_case(&field.name));
        let desc_method = format_ident!("order_by_{}_desc", to_snake_case(&field.name));
        let field_col = to_snake_case(&field.name);

        quote! {
            pub fn #asc_method(mut self) -> Self {
                self.order_by.push((#field_col.to_string(), "ASC".to_string()));
                self
            }
            pub fn #desc_method(mut self) -> Self {
                self.order_by.push((#field_col.to_string(), "DESC".to_string()));
                self
            }
        }
    });

    let field_gets = model.fields.iter().enumerate().map(|(idx, field)| {
        let field_name = format_ident!("{}", field.name);
        quote! { #field_name: row.get(#idx) }
    });

    quote! {
        pub struct #builder_name {
            client: Arc<PgClient>,
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
                    client: self.client.clone(),
                    table: self.table.clone(),
                    where_fragments: self.where_fragments.clone(),
                    args: Vec::new(),
                    limit: self.limit,
                    offset: self.offset,
                    order_by: self.order_by.clone(),
                }
            }
        }

        impl #builder_name {
            pub fn new(client: Arc<PgClient>) -> Self {
                Self {
                    client,
                    table: #table_name.to_string(),
                    where_fragments: vec![],
                    args: vec![],
                    limit: None,
                    offset: None,
                    order_by: vec![],
                }
            }

            #(#where_methods)*
            #(#order_by_methods)*

            pub fn limit(mut self, limit: usize) -> Self {
                self.limit = Some(limit);
                self
            }

            pub fn offset(mut self, offset: usize) -> Self {
                self.offset = Some(offset);
                self
            }

            pub async fn select(mut self)
                -> Result<Vec<#model_name>, Box<dyn std::error::Error + Send + Sync>>
            {
                let mut sql = format!("SELECT * FROM {}", self.table);

                let params: Vec<&(dyn tokio_postgres::types::ToSql + Sync)> =
                    self.args.iter().map(|b| b.as_ref()).collect();

                if !self.where_fragments.is_empty() {
                    let where_clauses: Vec<String> = self.where_fragments.iter()
                        .map(|&(col, idx)| format!("{} = ${}", col, idx))
                        .collect();
                    sql.push_str(" WHERE ");
                    sql.push_str(&where_clauses.join(" AND "));
                }

                if !self.order_by.is_empty() {
                    let order_clauses: Vec<String> = self.order_by.iter()
                        .map(|(col, dir)| format!("{} {}", col, dir))
                        .collect();
                    sql.push_str(" ORDER BY ");
                    sql.push_str(&order_clauses.join(", "));
                }

                if let Some(limit) = self.limit {
                    sql.push_str(&format!(" LIMIT {}", limit));
                }

                if let Some(offset) = self.offset {
                    sql.push_str(&format!(" OFFSET {}", offset));
                }

                let rows = self.client.query(&sql, &params[..]).await?;
                Ok(rows.into_iter().map(|row| #model_name {
                    #(#field_gets),*
                }).collect())
            }

            pub async fn first(self)
                -> Result<Option<#model_name>, Box<dyn std::error::Error + Send + Sync>>
            {
                let result = self.limit(1).select().await?;
                Ok(result.into_iter().next())
            }

            pub async fn count(self)
                -> Result<i64, Box<dyn std::error::Error + Send + Sync>>
            {
                let mut sql = format!("SELECT COUNT(*) FROM {}", self.table);

                let params: Vec<&(dyn tokio_postgres::types::ToSql + Sync)> =
                    self.args.iter().map(|b| b.as_ref()).collect();

                if !self.where_fragments.is_empty() {
                    let where_clauses: Vec<String> = self.where_fragments.iter()
                        .map(|&(col, idx)| format!("{} = ${}", col, idx))
                        .collect();
                    sql.push_str(" WHERE ");
                    sql.push_str(&where_clauses.join(" AND "));
                }

                let row = self.client.query_one(&sql, &params[..]).await?;
                Ok(row.get(0))
            }
        }
    }
}

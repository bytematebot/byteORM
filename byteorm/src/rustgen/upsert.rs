use proc_macro2::TokenStream;
use quote::{format_ident, quote};
use crate::{Model, Modifier};
use crate::rustgen::{rust_type_from_schema, to_snake_case};

fn is_numeric_type(ty: &str) -> bool {
    matches!(ty, "BigInt" | "Int" | "Serial" | "Float" | "Real")
}

pub fn generate_upsert_builder(model: &Model) -> TokenStream {
    let model_name = format_ident!("{}", model.name);
    let upsert_builder_name = format_ident!("{}Upsert", model.name);
    let table_name = model.name.to_lowercase();

    let pk_fields: Vec<_> = model.fields.iter()
        .filter(|f| f.modifiers.iter().any(|m| matches!(m, Modifier::PrimaryKey)))
        .collect();

    if pk_fields.is_empty() {
        return quote! {
            pub struct #upsert_builder_name;

            impl #upsert_builder_name {
                pub fn new(_client: Arc<PgClient>) -> Self {
                    Self
                }
            }
        };
    }

    let all_fields: Vec<_> = model.fields.iter().collect();

    let where_methods = pk_fields.iter().map(|field| {
        let method_name = format_ident!("where_{}", to_snake_case(&field.name));
        let is_nullable = field.modifiers.iter().any(|m| matches!(m, Modifier::Nullable));
        let field_type = rust_type_from_schema(&field.type_name, is_nullable);
        let field_col = to_snake_case(&field.name);

        quote! {
            pub fn #method_name(mut self, value: #field_type) -> Self {
                self.pk_values.insert(#field_col, Box::new(value));
                self
            }
        }
    });

    let set_methods = all_fields.iter().map(|field| {
        let method_name = format_ident!("set_{}", to_snake_case(&field.name));
        let is_nullable = field.modifiers.iter().any(|m| matches!(m, Modifier::Nullable));
        let field_type = rust_type_from_schema(&field.type_name, is_nullable);
        let field_col = to_snake_case(&field.name);

        quote! {
            pub fn #method_name(mut self, value: #field_type) -> Self {
                self.set_values.insert(#field_col, Box::new(value));
                self
            }
        }
    });

    let inc_methods = model.fields.iter()
        .filter(|f| is_numeric_type(&f.type_name))
        .map(|field| {
            let field_col = to_snake_case(&field.name);
            let inc_method = format_ident!("inc_{}", field_col);
            let dec_method = format_ident!("dec_{}", field_col);
            let mul_method = format_ident!("mul_{}", field_col);
            let div_method = format_ident!("div_{}", field_col);

            quote! {
                pub fn #inc_method(mut self, amount: i64) -> Self {
                    self.inc_ops.insert(#field_col, ("inc", amount));
                    self.set_values.insert(#field_col, Box::new(amount));
                    self
                }
                pub fn #dec_method(mut self, amount: i64) -> Self {
                    self.inc_ops.insert(#field_col, ("dec", amount));
                    self.set_values.insert(#field_col, Box::new(-amount));
                    self
                }
                pub fn #mul_method(mut self, factor: i64) -> Self {
                    self.inc_ops.insert(#field_col, ("mul", factor));
                    self.set_values.insert(#field_col, Box::new(0));
                    self
                }
                pub fn #div_method(mut self, divisor: i64) -> Self {
                    self.inc_ops.insert(#field_col, ("div", divisor));
                    self.set_values.insert(#field_col, Box::new(0));
                    self
                }
            }
        });

    let field_gets = model.fields.iter().enumerate().map(|(idx, field)| {
        let field_name = format_ident!("{}", field.name);
        quote! { #field_name: row.get(#idx) }
    });

    let pk_col_names: Vec<String> = pk_fields.iter()
        .map(|f| to_snake_case(&f.name))
        .collect();
    let conflict_clause = pk_col_names.join(", ");

    quote! {
        pub struct #upsert_builder_name {
            client: Arc<PgClient>,
            table: String,
            pk_values: std::collections::HashMap<&'static str, Box<dyn tokio_postgres::types::ToSql + Sync>>,
            set_values: std::collections::HashMap<&'static str, Box<dyn tokio_postgres::types::ToSql + Sync>>,
            inc_ops: std::collections::HashMap<&'static str, (&'static str, i64)>,
            polled: bool,
        }

        unsafe impl Send for #upsert_builder_name {}

        impl #upsert_builder_name {
            pub fn new(client: Arc<PgClient>) -> Self {
                Self {
                    client,
                    table: #table_name.to_string(),
                    pk_values: std::collections::HashMap::new(),
                    set_values: std::collections::HashMap::new(),
                    inc_ops: std::collections::HashMap::new(),
                    polled: false,
                }
            }

            #(#where_methods)*
            #(#set_methods)*
            #(#inc_methods)*
        }

        impl std::future::Future for #upsert_builder_name {
            type Output = Result<#model_name, Box<dyn std::error::Error + Send + Sync>>;
            fn poll(mut self: std::pin::Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> std::task::Poll<Self::Output> {
                if self.polled {
                    panic!("future polled more than once");
                }
                self.polled = true;

                let client = self.client.clone();
                let table = self.table.clone();
                let pk_values = std::mem::take(&mut self.pk_values);
                let set_values = std::mem::take(&mut self.set_values);
                let inc_ops = std::mem::take(&mut self.inc_ops);

                let pk_columns = vec![#(#pk_col_names),*];
                let conflict_clause = #conflict_clause.to_string();

                let fut = async move {
                    for pk_col in &pk_columns {
                        if !pk_values.contains_key(pk_col) && !set_values.contains_key(pk_col) {
                            return Err(format!("Missing primary key field: {}", pk_col).into());
                        }
                    }

                    let mut all_values = pk_values;
                    for (k, v) in set_values {
                        all_values.insert(k, v);
                    }

                    if all_values.is_empty() {
                        return Err("No fields to upsert".into());
                    }

                    let mut columns: Vec<&str> = all_values.keys().copied().collect();
                    columns.sort();

                    let columns_str = columns.join(", ");
                    let placeholders: Vec<String> = (1..=columns.len())
                        .map(|i| format!("${}", i))
                        .collect();
                    let placeholders_str = placeholders.join(", ");

                    let mut params: Vec<&(dyn tokio_postgres::types::ToSql + Sync)> = vec![];
                    for col in &columns {
                        params.push(all_values.get(col).unwrap().as_ref());
                    }

                    let update_columns: Vec<&str> = columns.iter()
                        .filter(|col| !pk_columns.iter().any(|pk| pk == *col))
                        .copied()
                        .collect();

                    let sql = if update_columns.is_empty() && inc_ops.is_empty() {
                        format!(
                            "INSERT INTO {} ({}) VALUES ({}) ON CONFLICT ({}) DO NOTHING RETURNING *",
                            table, columns_str, placeholders_str, conflict_clause
                        )
                    } else {
                        let mut update_clauses: Vec<String> = vec![];

                        for col in update_columns {
                            if let Some((op, value)) = inc_ops.get(col) {
                                let clause = match *op {
                                    "inc" => format!("{} = COALESCE({}.{}, 0) + {}", col, table, col, value),
                                    "dec" => format!("{} = COALESCE({}.{}, 0) - {}", col, table, col, value.abs()),
                                    "mul" => format!("{} = COALESCE({}.{}, 0) * {}", col, table, col, value),
                                    "div" => format!("{} = COALESCE({}.{}, 0) / {}", col, table, col, value),
                                    _ => format!("{} = EXCLUDED.{}", col, col),
                                };
                                update_clauses.push(clause);
                            } else {
                                update_clauses.push(format!("{} = EXCLUDED.{}", col, col));
                            }
                        }

                        format!(
                            "INSERT INTO {} ({}) VALUES ({}) ON CONFLICT ({}) DO UPDATE SET {} RETURNING *",
                            table, columns_str, placeholders_str, conflict_clause, update_clauses.join(", ")
                        )
                    };

                    let row = client.query_one(&sql, &params[..]).await?;
                    Ok(#model_name {
                        #(#field_gets),*
                    })
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

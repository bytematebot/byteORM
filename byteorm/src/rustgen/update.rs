use proc_macro2::TokenStream;
use quote::{format_ident, quote};
use crate::{Model, Modifier};
use crate::rustgen::{rust_type_from_schema, to_snake_case};

fn is_numeric_type(ty: &str) -> bool {
    matches!(ty, "BigInt" | "Int" | "Serial" | "Float" | "Real")
}

pub fn generate_update_builder(model: &Model) -> TokenStream {
    let model_name = format_ident!("{}", model.name);
    let update_builder_name = format_ident!("{}Update", model.name);
    let table_name = model.name.to_lowercase();

    let where_methods = model.fields.iter().map(|field| {
        let method_name = format_ident!("where_{}", to_snake_case(&field.name));
        let is_nullable = field.modifiers.iter().any(|m| matches!(m, Modifier::Nullable));
        let field_type = rust_type_from_schema(&field.type_name, is_nullable);
        let field_col = to_snake_case(&field.name);

        quote! {
            pub fn #method_name(mut self, value: #field_type) -> Self {
                self.where_args.push(Box::new(value));
                self.where_fragments.push((#field_col, self.where_args.len()));
                self
            }
        }
    });

    let set_methods = model.fields.iter().map(|field| {
        let method_name = format_ident!("set_{}", to_snake_case(&field.name));
        let is_nullable = field.modifiers.iter().any(|m| matches!(m, Modifier::Nullable));
        let field_type = rust_type_from_schema(&field.type_name, is_nullable);
        let field_col = to_snake_case(&field.name);

        quote! {
            pub fn #method_name(mut self, value: #field_type) -> Self {
                self.set_args.push(Box::new(value));
                self.set_fragments.push(#field_col);
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
                    self.inc_ops.push((#field_col, "inc", amount));
                    self
                }
                pub fn #dec_method(mut self, amount: i64) -> Self {
                    self.inc_ops.push((#field_col, "dec", amount));
                    self
                }
                pub fn #mul_method(mut self, factor: i64) -> Self {
                    self.inc_ops.push((#field_col, "mul", factor));
                    self
                }
                pub fn #div_method(mut self, divisor: i64) -> Self {
                    self.inc_ops.push((#field_col, "div", divisor));
                    self
                }
            }
        });

    let field_gets = model.fields.iter().enumerate().map(|(idx, field)| {
        let field_name = format_ident!("{}", field.name);
        quote! { #field_name: row.get(#idx) }
    });

    quote! {
        pub struct #update_builder_name {
            client: Arc<PgClient>,
            table: String,
            where_fragments: Vec<(&'static str, usize)>,
            where_args: Vec<Box<dyn tokio_postgres::types::ToSql + Sync>>,
            set_fragments: Vec<&'static str>,
            set_args: Vec<Box<dyn tokio_postgres::types::ToSql + Sync>>,
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

            pub async fn execute(self) -> Result<#model_name, Box<dyn std::error::Error + Send + Sync>> {
                if self.set_fragments.is_empty() && self.inc_ops.is_empty() {
                    return Err("No fields to update".into());
                }

                let mut sql = format!("UPDATE {} SET ", self.table);
                let mut set_clauses: Vec<String> = vec![];
                let mut param_idx = 1;

                for (i, col) in self.set_fragments.iter().enumerate() {
                    set_clauses.push(format!("{} = ${}", col, param_idx));
                    param_idx += 1;
                }

                for (field, op, _) in &self.inc_ops {
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
                    self.set_args.iter().map(|a| a.as_ref()).collect();
                for (_, _, val) in &self.inc_ops {
                    all_params.push(val);
                }

                if !self.where_fragments.is_empty() {
                    let where_clauses: Vec<String> = self.where_fragments.iter()
                        .enumerate()
                        .map(|(i, &(col, _))| format!("{} = ${}", col, self.set_args.len() + self.inc_ops.len() + i + 1))
                        .collect();
                    sql.push_str(" WHERE ");
                    sql.push_str(&where_clauses.join(" AND "));

                    for arg in &self.where_args {
                        all_params.push(arg.as_ref());
                    }
                }

                sql.push_str(" RETURNING *");

                let row = self.client.query_one(&sql, &all_params[..]).await?;
                Ok(#model_name {
                    #(#field_gets),*
                })
            }
        }
    }
}

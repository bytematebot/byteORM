use proc_macro2::TokenStream;
use quote::{format_ident, quote};
use crate::{Model, Modifier};
use crate::rustgen::{rust_type_from_schema, to_snake_case};

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
        }

        unsafe impl Send for #upsert_builder_name {}

        impl #upsert_builder_name {
            pub fn new(client: Arc<PgClient>) -> Self {
                Self {
                    client,
                    table: #table_name.to_string(),
                    pk_values: std::collections::HashMap::new(),
                    set_values: std::collections::HashMap::new(),
                }
            }

            #(#where_methods)*
            #(#set_methods)*

            pub async fn execute(self) -> Result<#model_name, Box<dyn std::error::Error + Send + Sync>> {
                let pk_columns = vec![#(#pk_col_names),*];
                for pk_col in &pk_columns {
                    if !self.pk_values.contains_key(pk_col) && !self.set_values.contains_key(pk_col) {
                        return Err(format!("Missing primary key field: {}", pk_col).into());
                    }
                }

                let mut all_values = self.pk_values;
                for (k, v) in self.set_values {
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

                let sql = if update_columns.is_empty() {
                    format!(
                        "INSERT INTO {} ({}) VALUES ({}) ON CONFLICT ({}) DO NOTHING RETURNING *",
                        self.table, columns_str, placeholders_str, #conflict_clause
                    )
                } else {
                    let update_clauses: Vec<String> = update_columns.iter()
                        .map(|col| format!("{} = EXCLUDED.{}", col, col))
                        .collect();

                    format!(
                        "INSERT INTO {} ({}) VALUES ({}) ON CONFLICT ({}) DO UPDATE SET {} RETURNING *",
                        self.table, columns_str, placeholders_str, #conflict_clause, update_clauses.join(", ")
                    )
                };

                let row = self.client.query_one(&sql, &params[..]).await?;
                Ok(#model_name {
                    #(#field_gets),*
                })
            }
        }
    }
}
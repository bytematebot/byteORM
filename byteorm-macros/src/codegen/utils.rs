use crate::types::*;
use proc_macro2::TokenStream;
use quote::{format_ident, quote};

pub fn rust_type_from_schema(type_name: &str, nullable: bool) -> TokenStream {
    let base_type = match type_name {
        "BigInt" => quote! { i64 },
        "Int" => quote! { i32 },
        "String" | "Text" => quote! { String },
        "JsonB" | "Jsonb" => quote! { serde_json::Value },
        "TimestamptZ" | "Timestamp" => quote! { DateTime<Utc> },
        "Date" => quote! { NaiveDate },
        "Boolean" => quote! { bool },
        "Float" => quote! { f64 },
        "Serial" => quote! { i32 },
        "Real" => quote! { f32 },
        _ => quote! { String },
    };
    if nullable { quote! { Option<#base_type> } } else { base_type }
}

pub fn to_snake_case(s: &str) -> String {
    let mut result = String::new();
    for (i, ch) in s.chars().enumerate() {
        if ch.is_uppercase() && i > 0 { result.push('_'); }
        result.push(ch.to_lowercase().next().unwrap_or(ch));
    }
    result
}

pub fn capitalize_first(s: &str) -> String {
    let mut chars = s.chars();
    match chars.next() {
        None => String::new(),
        Some(first) => first.to_uppercase().chain(chars).collect(),
    }
}

pub fn is_numeric_type(ty: &str) -> bool {
    matches!(ty, "BigInt" | "Int" | "Serial" | "Float" | "Real")
}

pub fn is_builtin_type(ty: &str) -> bool {
    matches!(ty, "BigInt" | "Int" | "String" | "JsonB" | "Jsonb" | "TimestamptZ" | "Timestamp" | "Boolean" | "Float" | "Serial" | "Real" | "Text" | "Date")
}

pub fn generate_select_columns(model: &Model) -> String {
    model.fields.iter().map(|field| {
        let col_name = to_snake_case(&field.name);
        if is_builtin_type(&field.type_name) { col_name }
        else { format!("CAST({} AS TEXT) as {}", col_name, col_name) }
    }).collect::<Vec<_>>().join(", ")
}

pub fn generate_from_row_impl(model: &Model) -> TokenStream {
    let model_name = format_ident!("{}", model.name);
    let field_gets = model.fields.iter().map(|field| {
        let field_name = format_ident!("{}", field.name);
        let col_name = to_snake_case(&field.name);
        quote! { #field_name: row.get(#col_name) }
    });
    quote! {
        impl crate::FromRow for #model_name {
            fn from_row(row: &tokio_postgres::Row) -> Self {
                Self { #(#field_gets),* }
            }
        }
    }
}

pub fn pk_args(model: &Model) -> (Vec<proc_macro2::Ident>, Vec<TokenStream>, Vec<String>, Vec<String>, Vec<TokenStream>) {
    let pk_fields: Vec<_> = model.fields.iter()
        .filter(|f| f.modifiers.iter().any(|m| matches!(m, Modifier::PrimaryKey)))
        .collect();
    let pk_names: Vec<_> = pk_fields.iter().map(|pk| format_ident!("{}", to_snake_case(&pk.name))).collect();
    let pk_types: Vec<_> = pk_fields.iter().map(|pk| {
        let is_nullable = pk.modifiers.iter().any(|m| matches!(m, Modifier::Nullable));
        rust_type_from_schema(&pk.type_name, is_nullable)
    }).collect();
    let pk_cols: Vec<_> = pk_fields.iter().map(|pk| to_snake_case(&pk.name)).collect();
    let pk_placeholders: Vec<_> = (1..=pk_fields.len()).map(|i| format!("${}", i)).collect();
    let pk_arg_refs: Vec<_> = pk_fields.iter().map(|pk| {
        let name = format_ident!("{}", to_snake_case(&pk.name));
        quote! { &#name }
    }).collect();
    (pk_names, pk_types, pk_cols, pk_placeholders, pk_arg_refs)
}

pub fn generate_where_methods<'a>(model: &'a Model, target_args: &'a str, target_fragments: &'a str) -> impl Iterator<Item = TokenStream> + 'a {
    model.fields.iter().flat_map(move |field| {
        let method_name = format_ident!("where_{}", to_snake_case(&field.name));
        let method_in = format_ident!("where_{}_in", to_snake_case(&field.name));
        let is_nullable = field.modifiers.iter().any(|m| matches!(m, Modifier::Nullable));
        let field_type = rust_type_from_schema(&field.type_name, is_nullable);
        let field_col = to_snake_case(&field.name);
        let args_ident = format_ident!("{}", target_args);
        let fragments_ident = format_ident!("{}", target_fragments);

        let base_method = quote! {
            pub fn #method_name(mut self, value: #field_type) -> Self {
                self.#args_ident.push(Box::new(value) as Box<dyn tokio_postgres::types::ToSql + Sync + Send>);
                self.#fragments_ident.push((#field_col.to_string(), self.#args_ident.len()));
                self
            }
        };
        let in_method = quote! {
            pub fn #method_in(mut self, values: Vec<#field_type>) -> Self {
                if values.is_empty() { return self; }
                let start_idx = self.#args_ident.len() + 1;
                let placeholders: Vec<String> = (start_idx..start_idx + values.len()).map(|i| format!("${}", i)).collect();
                let in_clause = format!("{} IN ({})", #field_col, placeholders.join(", "));
                for value in values { self.#args_ident.push(Box::new(value) as Box<dyn tokio_postgres::types::ToSql + Sync + Send>); }
                self.#fragments_ident.push((in_clause, 0));
                self
            }
        };
        let null_methods = if is_nullable {
            let method_is_null = format_ident!("where_{}_is_null", to_snake_case(&field.name));
            let method_is_not_null = format_ident!("where_{}_is_not_null", to_snake_case(&field.name));
            vec![
                quote! { pub fn #method_is_null(mut self) -> Self { self.#fragments_ident.push((format!("{} IS NULL", #field_col), 0)); self } },
                quote! { pub fn #method_is_not_null(mut self) -> Self { self.#fragments_ident.push((format!("{} IS NOT NULL", #field_col), 0)); self } }
            ]
        } else { vec![] };

        let mut methods = vec![base_method, in_method];
        methods.extend(null_methods);

        if field.type_name == "TimestamptZ" {
            let method_gt = format_ident!("where_{}_gt", to_snake_case(&field.name));
            let method_lt = format_ident!("where_{}_lt", to_snake_case(&field.name));
            let method_gte = format_ident!("where_{}_gte", to_snake_case(&field.name));
            let method_lte = format_ident!("where_{}_lte", to_snake_case(&field.name));
            methods.extend(vec![
                quote! { pub fn #method_gt(mut self, value: #field_type) -> Self { self.#args_ident.push(Box::new(value) as Box<dyn tokio_postgres::types::ToSql + Sync + Send>); self.#fragments_ident.push((format!("{} >", #field_col), self.#args_ident.len())); self } },
                quote! { pub fn #method_lt(mut self, value: #field_type) -> Self { self.#args_ident.push(Box::new(value) as Box<dyn tokio_postgres::types::ToSql + Send + Sync>); self.#fragments_ident.push((format!("{} <", #field_col), self.#args_ident.len())); self } },
                quote! { pub fn #method_gte(mut self, value: #field_type) -> Self { self.#args_ident.push(Box::new(value) as Box<dyn tokio_postgres::types::ToSql + Sync + Send>); self.#fragments_ident.push((format!("{} >=", #field_col), self.#args_ident.len())); self } },
                quote! { pub fn #method_lte(mut self, value: #field_type) -> Self { self.#args_ident.push(Box::new(value) as Box<dyn tokio_postgres::types::ToSql + Sync + Send>); self.#fragments_ident.push((format!("{} <=", #field_col), self.#args_ident.len())); self } },
            ]);
        }
        methods.into_iter()
    })
}

pub fn generate_set_methods<'a>(model: &'a Model, use_hashmap: bool, hashmap_field: &'a str, vec_field: Option<&'a str>, fragments_field: Option<&'a str>) -> impl Iterator<Item = TokenStream> + 'a {
    model.fields.iter().map(move |field| {
        let method_name = format_ident!("set_{}", to_snake_case(&field.name));
        let is_nullable = field.modifiers.iter().any(|m| matches!(m, Modifier::Nullable));
        let field_type = rust_type_from_schema(&field.type_name, is_nullable);
        let field_col = to_snake_case(&field.name);
        let is_enum = !is_builtin_type(&field.type_name);

        if use_hashmap {
            let map_ident = format_ident!("{}", hashmap_field);
            if is_enum {
                quote! {
                    pub fn #method_name(mut self, value: #field_type) -> Self {
                        self.#map_ident.insert(#field_col, Box::new(value.to_string()) as Box<dyn tokio_postgres::types::ToSql + Sync + Send>);
                        self
                    }
                }
            } else {
                quote! {
                    pub fn #method_name(mut self, value: #field_type) -> Self {
                        self.#map_ident.insert(#field_col, Box::new(value) as Box<dyn tokio_postgres::types::ToSql + Sync + Send>);
                        self
                    }
                }
            }
        } else {
            let vec_ident = format_ident!("{}", vec_field.unwrap_or("set_args"));
            let frags_ident = format_ident!("{}", fragments_field.unwrap_or("set_fragments"));
            if is_enum {
                quote! {
                    pub fn #method_name(mut self, value: #field_type) -> Self {
                        self.#vec_ident.push(Box::new(value.to_string()) as Box<dyn tokio_postgres::types::ToSql + Sync + Send>);
                        self.#frags_ident.push(#field_col);
                        self
                    }
                }
            } else {
                quote! {
                    pub fn #method_name(mut self, value: #field_type) -> Self {
                        self.#vec_ident.push(Box::new(value) as Box<dyn tokio_postgres::types::ToSql + Sync + Send>);
                        self.#frags_ident.push(#field_col);
                        self
                    }
                }
            }
        }
    })
}

pub fn generate_inc_methods<'a>(model: &'a Model, target_ops: &'a str, target_values: Option<&'a str>) -> impl Iterator<Item = TokenStream> + 'a {
    model.fields.iter()
        .filter(|f| is_numeric_type(&f.type_name))
        .map(move |field| {
            let field_col = to_snake_case(&field.name);
            let inc_method = format_ident!("inc_{}", field_col);
            let dec_method = format_ident!("dec_{}", field_col);
            let mul_method = format_ident!("mul_{}", field_col);
            let div_method = format_ident!("div_{}", field_col);
            let ops_ident = format_ident!("{}", target_ops);
            let values_ident = target_values.map(|v| format_ident!("{}", v));

            let inc_body = if let Some(values) = &values_ident {
                quote! { self.#ops_ident.insert(#field_col, ("inc", amount)); self.#values.insert(#field_col, Box::new(amount)); }
            } else {
                quote! { self.#ops_ident.push((#field_col, "inc", amount)); }
            };
            let dec_body = if let Some(values) = &values_ident {
                quote! { self.#ops_ident.insert(#field_col, ("dec", amount)); self.#values.insert(#field_col, Box::new(-amount)); }
            } else {
                quote! { self.#ops_ident.push((#field_col, "dec", amount)); }
            };
            let mul_body = if let Some(values) = &values_ident {
                quote! { self.#ops_ident.insert(#field_col, ("mul", factor)); self.#values.insert(#field_col, Box::new(0)); }
            } else {
                quote! { self.#ops_ident.push((#field_col, "mul", factor)); }
            };
            let div_body = if let Some(values) = &values_ident {
                quote! { self.#ops_ident.insert(#field_col, ("div", divisor)); self.#values.insert(#field_col, Box::new(0)); }
            } else {
                quote! { self.#ops_ident.push((#field_col, "div", divisor)); }
            };

            quote! {
                pub fn #inc_method(mut self, amount: i64) -> Self { #inc_body self }
                pub fn #dec_method(mut self, amount: i64) -> Self { #dec_body self }
                pub fn #mul_method(mut self, factor: i64) -> Self { #mul_body self }
                pub fn #div_method(mut self, divisor: i64) -> Self { #div_body self }
            }
        })
}

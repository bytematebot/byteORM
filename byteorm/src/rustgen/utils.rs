use quote::{quote, format_ident};
use proc_macro2::TokenStream;
use crate::Modifier;

pub fn rust_type_from_schema(type_name: &str, nullable: bool) -> TokenStream {
    let base_type = match type_name {
        "BigInt"      => quote! { i64 },
        "Int"         => quote! { i32 },
        "String"      => quote! { String },
        "JsonB"       => quote! { serde_json::Value },
        "TimestamptZ" | "Timestamp" => quote! { DateTime<Utc> },
        "Boolean"     => quote! { bool },
        "Float"       => quote! { f64 },
        "Serial"      => quote! { i32 },
        "Real"        => quote! { f32 },
        _             => quote! { String },
    };

    if nullable {
        quote! { Option<#base_type> }
    } else {
        base_type
    }
}

pub fn to_snake_case(s: &str) -> String {
    let mut result = String::new();
    for (i, ch) in s.chars().enumerate() {
        if ch.is_uppercase() && i > 0 {
            result.push('_');
        }
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

pub fn pk_args(model: &crate::Model) -> (Vec<proc_macro2::Ident>, Vec<proc_macro2::TokenStream>, Vec<String>, Vec<String>, Vec<proc_macro2::TokenStream>) {
    let pk_fields: Vec<_> = model.fields.iter()
        .filter(|f| f.modifiers.iter().any(|m| matches!(m, Modifier::PrimaryKey)))
        .collect();
    let pk_names = pk_fields.iter().map(|pk| format_ident!("{}", to_snake_case(&pk.name))).collect();
    let pk_types = pk_fields.iter().map(|pk| {
        let is_nullable = pk.modifiers.iter().any(|m| matches!(m, Modifier::Nullable));
        rust_type_from_schema(&pk.type_name, is_nullable)
    }).collect();
    let pk_cols: Vec<_> = pk_fields.iter().map(|pk| to_snake_case(&pk.name)).collect();
    let pk_placeholders: Vec<_> = (1..=pk_fields.len()).map(|i| format!("${}", i)).collect();
    let pk_arg_refs = pk_fields.iter().map(|pk| {
        let name = format_ident!("{}", to_snake_case(&pk.name));
        quote! { &#name }
    }).collect();
    (pk_names, pk_types, pk_cols, pk_placeholders, pk_arg_refs)
}
use syn::{DeriveInput, Data, Fields, Meta, Expr, Lit};
use crate::types::*;

pub fn parse_model(input: &DeriveInput) -> Model {
    let name = input.ident.to_string();
    let table_name = parse_table_name(input).unwrap_or_else(|| name.to_lowercase());
    let computed_fields = parse_computed_fields(input);

    let fields = match &input.data {
        Data::Struct(data) => match &data.fields {
            Fields::Named(named) => named.named.iter().map(parse_field).collect(),
            _ => panic!("ByteOrm only supports named fields"),
        },
        _ => panic!("ByteOrm can only be derived for structs"),
    };

    Model { name, fields, computed_fields, table_name }
}

fn parse_table_name(input: &DeriveInput) -> Option<String> {
    for attr in &input.attrs {
        if !attr.path().is_ident("byteorm") {
            continue;
        }
        let nested = attr.parse_args_with(
            syn::punctuated::Punctuated::<Meta, syn::Token![,]>::parse_terminated,
        ).ok()?;
        for meta in &nested {
            if let Meta::NameValue(nv) = meta {
                if nv.path.is_ident("table") {
                    if let Expr::Lit(lit) = &nv.value {
                        if let Lit::Str(s) = &lit.lit {
                            return Some(s.value());
                        }
                    }
                }
            }
        }
    }
    None
}

fn parse_computed_fields(input: &DeriveInput) -> Vec<ComputedField> {
    let mut computed = Vec::new();
    for attr in &input.attrs {
        if !attr.path().is_ident("byteorm") {
            continue;
        }
        let nested = attr.parse_args_with(
            syn::punctuated::Punctuated::<Meta, syn::Token![,]>::parse_terminated,
        );
        if let Ok(nested) = nested {
            for meta in &nested {
                if let Meta::List(list) = meta {
                    if list.path.is_ident("computed") {
                        let inner: syn::Result<syn::punctuated::Punctuated<Meta, syn::Token![,]>> =
                            list.parse_args_with(
                                syn::punctuated::Punctuated::parse_terminated,
                            );
                        if let Ok(inner) = inner {
                            let mut cf_name = None;
                            let mut cf_expr = None;
                            for m in &inner {
                                if let Meta::NameValue(nv) = m {
                                    if nv.path.is_ident("name") {
                                        if let Expr::Lit(lit) = &nv.value {
                                            if let Lit::Str(s) = &lit.lit {
                                                cf_name = Some(s.value());
                                            }
                                        }
                                    }
                                    if nv.path.is_ident("expr") {
                                        if let Expr::Lit(lit) = &nv.value {
                                            if let Lit::Str(s) = &lit.lit {
                                                cf_expr = Some(s.value());
                                            }
                                        }
                                    }
                                }
                            }
                            if let (Some(n), Some(e)) = (cf_name, cf_expr) {
                                computed.push(ComputedField { name: n, expression: e });
                            }
                        }
                    }
                }
            }
        }
    }
    computed
}

fn parse_field(field: &syn::Field) -> Field {
    let name = field.ident.as_ref().expect("Expected named field").to_string();
    let (type_name, is_option) = rust_type_to_schema_type(&field.ty);
    let mut modifiers = Vec::new();
    let mut attributes = Vec::new();

    if is_option {
        modifiers.push(Modifier::Nullable);
    }

    for attr in &field.attrs {
        if !attr.path().is_ident("byteorm") {
            continue;
        }
        let nested = attr.parse_args_with(
            syn::punctuated::Punctuated::<Meta, syn::Token![,]>::parse_terminated,
        );
        if let Ok(nested) = nested {
            for meta in &nested {
                match meta {
                    Meta::Path(path) => {
                        if path.is_ident("pk") {
                            modifiers.push(Modifier::PrimaryKey);
                        } else if path.is_ident("unique") {
                            modifiers.push(Modifier::Unique);
                        } else if path.is_ident("index") {
                            modifiers.push(Modifier::Index);
                        }
                    }
                    Meta::NameValue(nv) => {
                        if nv.path.is_ident("jsonb_default") {
                            if let Expr::Lit(lit) = &nv.value {
                                if let Lit::Str(s) = &lit.lit {
                                    attributes.push(Attribute {
                                        name: "jsonb_default".to_string(),
                                        args: Some(s.value()),
                                    });
                                }
                            }
                        } else if nv.path.is_ident("sql_default") {
                            if let Expr::Lit(lit) = &nv.value {
                                if let Lit::Str(s) = &lit.lit {
                                    attributes.push(Attribute {
                                        name: "default".to_string(),
                                        args: Some(s.value()),
                                    });
                                }
                            }
                        } else if nv.path.is_ident("enum_type") {
                            if let Expr::Lit(lit) = &nv.value {
                                if let Lit::Str(s) = &lit.lit {
                                    attributes.push(Attribute {
                                        name: "enum_type".to_string(),
                                        args: Some(s.value()),
                                    });
                                }
                            }
                        }
                    }
                    Meta::List(list) => {
                        if list.path.is_ident("fk") {
                            let inner: syn::Result<syn::punctuated::Punctuated<Meta, syn::Token![,]>> =
                                list.parse_args_with(
                                    syn::punctuated::Punctuated::parse_terminated,
                                );
                            if let Ok(inner) = inner {
                                let mut fk_model = None;
                                let mut fk_field = None;
                                for m in &inner {
                                    if let Meta::NameValue(nv) = m {
                                        if nv.path.is_ident("model") {
                                            if let Expr::Lit(lit) = &nv.value {
                                                if let Lit::Str(s) = &lit.lit {
                                                    fk_model = Some(s.value());
                                                }
                                            }
                                        }
                                        if nv.path.is_ident("field") {
                                            if let Expr::Lit(lit) = &nv.value {
                                                if let Lit::Str(s) = &lit.lit {
                                                    fk_field = Some(s.value());
                                                }
                                            }
                                        }
                                    }
                                }
                                if let Some(model) = fk_model {
                                    modifiers.push(Modifier::ForeignKey { model, field: fk_field });
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    let final_type_name = if let Some(enum_attr) = attributes.iter().find(|a| a.name == "enum_type") {
        enum_attr.args.clone().unwrap_or(type_name)
    } else {
        type_name
    };

    Field { name, type_name: final_type_name, modifiers, attributes }
}

fn rust_type_to_schema_type(ty: &syn::Type) -> (String, bool) {
    let type_str = quote::quote!(#ty).to_string();
    let type_str = type_str.replace(' ', "");

    if type_str.starts_with("Option<") {
        let inner = &type_str[7..type_str.len() - 1];
        let (schema_type, _) = map_inner_type(inner);
        return (schema_type, true);
    }

    let (schema_type, _) = map_inner_type(&type_str);
    (schema_type, false)
}

fn map_inner_type(ty: &str) -> (String, bool) {
    match ty {
        "i64" => ("BigInt".to_string(), false),
        "i32" => ("Int".to_string(), false),
        "String" => ("String".to_string(), false),
        "bool" => ("Boolean".to_string(), false),
        "f64" => ("Float".to_string(), false),
        "f32" => ("Real".to_string(), false),
        "serde_json::Value" => ("JsonB".to_string(), false),
        "DateTime<Utc>" => ("TimestamptZ".to_string(), false),
        "NaiveDate" => ("Date".to_string(), false),
        _ => (ty.to_string(), false),
    }
}

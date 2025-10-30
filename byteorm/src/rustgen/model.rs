use proc_macro2::TokenStream;
use quote::{format_ident, quote};
use crate::{Model, Modifier};
use crate::rustgen::{generate_query_builder_struct, generate_update_builder, generate_upsert_builder, pk_args, rust_type_from_schema, to_snake_case};

pub fn generate_model_with_query_builder(model: &Model) -> TokenStream {
    let model_struct = generate_model_struct(model);
    let query_builder_struct = generate_query_builder_struct(model);
    let update_builder = generate_update_builder(model);
    let upsert_builder = generate_upsert_builder(model);
    let model_impl = generate_model_impl(model);

    quote! {
        #model_struct
        #query_builder_struct
        #update_builder
        #upsert_builder
        #model_impl
    }
}

pub fn generate_model_struct(model: &Model) -> TokenStream {
    let name = format_ident!("{}", model.name);
    let fields = model.fields.iter().map(|field| {
        let field_name = format_ident!("{}", field.name);
        let is_nullable = field.modifiers.iter().any(|m| matches!(m, Modifier::Nullable));
        let field_type = rust_type_from_schema(&field.type_name, is_nullable);

        quote! {
            pub #field_name: #field_type
        }
    });

    quote! {
        #[derive(Debug, Clone, Serialize, Deserialize)]
        pub struct #name {
            #(#fields),*
        }
    }
}

fn generate_model_impl(model: &Model) -> TokenStream {
    let model_name = format_ident!("{}", model.name);
    let pk_fields: Vec<_> = model.fields.iter()
        .filter(|f| f.modifiers.iter().any(|m| matches!(m, Modifier::PrimaryKey)))
        .collect();

    let field_gets = model.fields.iter().enumerate().map(|(idx, field)| {
        let field_name = format_ident!("{}", field.name);
        quote! { #field_name: row.get(#idx) }
    });

    let find_by_id_impl = if !pk_fields.is_empty() {
        if pk_fields.len() == 1 {
            let pk = &pk_fields[0];
            let is_nullable = pk.modifiers.iter().any(|m| matches!(m, Modifier::Nullable));
            let pk_type = rust_type_from_schema(&pk.type_name, is_nullable);
            let pk_name = to_snake_case(&pk.name);

            quote! {
                pub async fn find_by_id(client: Arc<PgClient>, id: #pk_type)
                    -> Result<Option<#model_name>, Box<dyn std::error::Error + Send + Sync>>
                {
                    let sql = format!("SELECT * FROM {} WHERE {} = $1",
                        stringify!(#model_name).to_lowercase(), #pk_name);
                    let row_opt = client.query_opt(&sql, &[&id]).await?;
                    Ok(row_opt.map(|row| #model_name {
                        #(#field_gets),*
                    }))
                }
            }
        } else {
            let (pk_names, pk_types, _, _, pk_arg_refs) = pk_args(model);

            let pk_params = pk_names.iter().zip(pk_types.iter()).map(|(name, typ)| {
                quote! { #name: #typ }
            });

            let pk_conditions = pk_fields.iter().enumerate().map(|(i, pk)| {
                let pk_col = to_snake_case(&pk.name);
                let param_num = i + 1;
                format!("{} = ${}", pk_col, param_num)
            });
            let where_clause = pk_conditions.collect::<Vec<_>>().join(" AND ");

            quote! {
                pub async fn find_by_composite_pk(client: Arc<PgClient>, #(#pk_params),*)
                    -> Result<Option<#model_name>, Box<dyn std::error::Error + Send + Sync>>
                {
                    let sql = format!("SELECT * FROM {} WHERE {}",
                        stringify!(#model_name).to_lowercase(), #where_clause);
                    let row_opt = client.query_opt(&sql, &[#(#pk_arg_refs),*]).await?;
                    Ok(row_opt.map(|row| #model_name {
                        #(#field_gets),*
                    }))
                }
            }
        }
    } else {
        quote! {}
    };

    quote! {
        impl #model_name {
            #find_by_id_impl
        }
    }
}

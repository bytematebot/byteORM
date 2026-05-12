use crate::rustgen::{
    capitalize_first, generate_jsonb_sub_accessors, generate_select_columns, is_builtin_type,
    is_numeric_type, pk_args, rust_type_from_schema, to_snake_case,
};
use crate::{Field, Modifier, Schema};
use proc_macro2::TokenStream;
use quote::{format_ident, quote};
use std::collections::HashMap;

struct FieldCategories<'a> {
    pk_fields: Vec<&'a Field>,
    jsonb_fields: Vec<&'a Field>,
    numeric_fields: Vec<&'a Field>,
}

impl<'a> FieldCategories<'a> {
    fn from_model(model: &'a crate::Model) -> Self {
        let mut pk_fields = Vec::new();
        let mut jsonb_fields = Vec::new();
        let mut numeric_fields = Vec::new();

        for field in &model.fields {
            if field
                .modifiers
                .iter()
                .any(|m| matches!(m, Modifier::PrimaryKey))
            {
                pk_fields.push(field);
            }
            if field.type_name == "JsonB" {
                jsonb_fields.push(field);
            }
            if is_numeric_type(&field.type_name) {
                numeric_fields.push(field);
            }
        }

        Self {
            pk_fields,
            jsonb_fields,
            numeric_fields,
        }
    }
}

fn generate_find_unique(model_name: &proc_macro2::Ident, model: &crate::Model) -> TokenStream {
    let pk_fields: Vec<_> = model
        .fields
        .iter()
        .filter(|f| {
            f.modifiers
                .iter()
                .any(|m| matches!(m, Modifier::PrimaryKey))
        })
        .collect();

    if pk_fields.is_empty() {
        quote! {}
    } else if pk_fields.len() == 1 {
        let pk = &pk_fields[0];
        let is_nullable = pk.modifiers.iter().any(|m| matches!(m, Modifier::Nullable));
        let pk_type = rust_type_from_schema(&pk.type_name, is_nullable);
        quote! {
            pub async fn find_unique(&self, id: #pk_type)
                -> Result<Option<#model_name>, Box<dyn std::error::Error + Send + Sync>>
            {
                #model_name::find_by_id(self.pool.clone(), id).await
            }
        }
    } else {
        let pk_params = pk_fields.iter().map(|pk| {
            let name = format_ident!("{}", to_snake_case(&pk.name));
            let is_nullable = pk.modifiers.iter().any(|m| matches!(m, Modifier::Nullable));
            let pk_type = rust_type_from_schema(&pk.type_name, is_nullable);
            quote! { #name: #pk_type }
        });

        let pk_args = pk_fields.iter().map(|pk| {
            let name = format_ident!("{}", to_snake_case(&pk.name));
            quote! { #name }
        });

        quote! {
            pub async fn find_unique(&self, #(#pk_params),*)
                -> Result<Option<#model_name>, Box<dyn std::error::Error + Send + Sync>>
            {
                #model_name::find_by_composite_pk(self.pool.clone(), #(#pk_args),*).await
            }
        }
    }
}

fn generate_find_or_create(
    model_name: &proc_macro2::Ident,
    model: &crate::Model,
    table_name: &str,
) -> TokenStream {
    let pk_fields: Vec<_> = model
        .fields
        .iter()
        .filter(|f| {
            f.modifiers
                .iter()
                .any(|m| matches!(m, Modifier::PrimaryKey))
        })
        .collect();

    if pk_fields.is_empty() {
        quote! {}
    } else if pk_fields.len() == 1 {
        let pk = &pk_fields[0];
        let is_nullable = pk.modifiers.iter().any(|m| matches!(m, Modifier::Nullable));
        let pk_type = rust_type_from_schema(&pk.type_name, is_nullable);
        let pk_col = to_snake_case(&pk.name);
        let select_columns = generate_select_columns(model);
        quote! {
            pub async fn find_or_create(&self, id: #pk_type)
                -> Result<#model_name, Box<dyn std::error::Error + Send + Sync>>
            {
                let sql = format!("INSERT INTO {} ({}) VALUES ($1) ON CONFLICT ({}) DO NOTHING RETURNING {}", #table_name, #pk_col, #pk_col, #select_columns);
                debug::log_query(&sql, 1);
                let client = self.pool.get().await.map_err(|_| "Failed to get connection from pool")?;
                let rows = client.query(&sql, &[&id]).await?;
                if let Some(row) = rows.first() {
                    Ok(#model_name::from_row(row))
                } else {
                    self.find_unique(id).await?.ok_or("Record should exist after find_or_create".into())
                }
            }
        }
    } else {
        let (_, _, pk_cols, pk_placeholders, pk_arg_refs) = pk_args(model);

        let pk_cols_str = pk_cols.join(", ");
        let pk_placeholders_str = pk_placeholders.join(", ");

        let pk_params = pk_fields.iter().map(|pk| {
            let name = format_ident!("{}", to_snake_case(&pk.name));
            let is_nullable = pk.modifiers.iter().any(|m| matches!(m, Modifier::Nullable));
            let pk_type = rust_type_from_schema(&pk.type_name, is_nullable);
            quote! { #name: #pk_type }
        });

        let pk_args_call = pk_fields.iter().map(|pk| {
            let name = format_ident!("{}", to_snake_case(&pk.name));
            quote! { #name }
        });

        let select_columns = generate_select_columns(model);

        quote! {
            pub async fn find_or_create(&self, #(#pk_params),*)
                -> Result<#model_name, Box<dyn std::error::Error + Send + Sync>>
            {
                let sql = format!(
                    "INSERT INTO {} ({}) VALUES ({}) ON CONFLICT ({}) DO NOTHING RETURNING {}",
                    #table_name, #pk_cols_str, #pk_placeholders_str, #pk_cols_str, #select_columns
                );
                debug::log_query(&sql, #pk_cols_str.split(", ").count());
                let client = self.pool.get().await.map_err(|_| "Failed to get connection from pool")?;
                let rows = client.query(&sql, &[#(#pk_arg_refs),*]).await?;
                if let Some(row) = rows.first() {
                    Ok(#model_name::from_row(row))
                } else {
                    self.find_unique(#(#pk_args_call),*).await?.ok_or("Record should exist after find_or_create".into())
                }
            }
        }
    }
}

pub fn generate_accessor_for_model(
    model: &crate::Model,
    jsonb_defaults: &HashMap<(String, String), String>,
) -> TokenStream {
    let categories = FieldCategories::from_model(model);
    let model_name = format_ident!("{}", model.name);
    let accessor_struct = format_ident!("{}Accessor", model.name);
    let conflict_field = format_ident!("{}ConflictField", model.name);
    let conflict_selector = format_ident!("{}ConflictSelector", model.name);
    let conflict_target = format_ident!("{}ConflictTarget", model.name);
    let query_builder = format_ident!("{}Query", model.name);
    let update_builder = format_ident!("{}Update", model.name);
    let upsert_builder = format_ident!("{}Upsert", model.name);
    let create_builder = format_ident!("{}Create", model.name);
    let delete_builder = format_ident!("{}Delete", model.name);
    let table_name = model.name.to_lowercase();

    let find_unique = generate_find_unique(&model_name, model);
    let find_or_create = generate_find_or_create(&model_name, model, &table_name);
    let required_fields: Vec<String> = model
        .fields
        .iter()
        .filter(|field| {
            !field.attributes.iter().any(|a| a.name == "default")
                && !field
                    .modifiers
                    .iter()
                    .any(|m| matches!(m, Modifier::Nullable))
                && field.type_name != "Serial"
        })
        .map(|field| to_snake_case(&field.name))
        .collect();

    let enum_cast_entries: Vec<TokenStream> = model
        .fields
        .iter()
        .filter(|field| !is_builtin_type(&field.type_name))
        .map(|field| {
            let col_name = to_snake_case(&field.name);
            let type_name = field.type_name.to_lowercase();
            quote! { (#col_name, #type_name) }
        })
        .collect();

    let conflict_selector_fields = model.fields.iter().map(|field| {
        let field_name = format_ident!("{}", to_snake_case(&field.name));
        quote! { pub #field_name: #conflict_field }
    });

    let conflict_selector_inits = model.fields.iter().map(|field| {
        let field_name = format_ident!("{}", to_snake_case(&field.name));
        let column_name = to_snake_case(&field.name);
        quote! { #field_name: #conflict_field::new(#column_name) }
    });

    let conflict_target_tuple_impls = (2..=model.fields.len()).map(|arity| {
        let tuple_types = (0..arity).map(|_| quote! { #conflict_field });
        let tuple_fields: Vec<_> = (0..arity).map(|i| format_ident!("f{}", i)).collect();
        let tuple_values = tuple_fields.iter();

        quote! {
            impl #conflict_target for (#(#tuple_types),*) {
                fn columns(self) -> Vec<&'static str> {
                    let (#(#tuple_fields),*) = self;
                    vec![#(#tuple_values.name()),*]
                }
            }
        }
    });

    let jsonb_accessor_fields = categories.jsonb_fields.iter().map(|jsonb| {
        let jsonb_snake = to_snake_case(&jsonb.name);
        let sub_accessor_struct =
            format_ident!("{}{}Accessor", model.name, capitalize_first(&jsonb.name));
        let sub_accessor_field = format_ident!("{}", jsonb_snake);
        quote! { pub #sub_accessor_field: #sub_accessor_struct }
    });

    let jsonb_accessor_inits = categories.jsonb_fields.iter().map(|jsonb| {
        let jsonb_snake = to_snake_case(&jsonb.name);
        let sub_accessor_struct =
            format_ident!("{}{}Accessor", model.name, capitalize_first(&jsonb.name));
        let sub_accessor_field = format_ident!("{}", jsonb_snake);
        quote! { #sub_accessor_field: #sub_accessor_struct::new(pool.clone()) }
    });

    let jsonb_debug_fields = categories.jsonb_fields.iter().map(|jsonb| {
        let jsonb_snake = to_snake_case(&jsonb.name);
        let sub_accessor_field = format_ident!("{}", jsonb_snake);
        quote! { .field(stringify!(#sub_accessor_field), &self.#sub_accessor_field) }
    });

    let jsonb_sub_accessors = generate_jsonb_sub_accessors(model, jsonb_defaults);
    let where_builder = format_ident!("{}WhereBuilder", model.name);

    let typed_agg_methods = {
        let field_methods = categories.numeric_fields.iter().map(|field| {
            let col_snake = to_snake_case(&field.name);
            let sum_name = format_ident!("sum_{}", col_snake);
            match field.type_name.as_str() {
                "Int" | "Serial" => {
                    quote! {
                        pub async fn #sum_name<F>(&self, f: F) -> Result<i64, Box<dyn std::error::Error + Send + Sync>>
                        where
                            F: FnOnce(#where_builder) -> #where_builder,
                        {
                            let builder = f(#where_builder::new());
                            #query_builder::from_builder(self.pool.clone(), builder).sum::<i64>(#col_snake).await
                        }
                    }
                }
                "BigInt" => {
                    quote! {
                        pub async fn #sum_name<F>(&self, f: F) -> Result<i64, Box<dyn std::error::Error + Send + Sync>>
                        where
                            F: FnOnce(#where_builder) -> #where_builder,
                        {
                            let builder = f(#where_builder::new());
                            #query_builder::from_builder(self.pool.clone(), builder).sum_cast_i64(#col_snake).await
                        }
                    }
                }
                "Float" => {
                    quote! {
                        pub async fn #sum_name<F>(&self, f: F) -> Result<f64, Box<dyn std::error::Error + Send + Sync>>
                        where
                            F: FnOnce(#where_builder) -> #where_builder,
                        {
                            let builder = f(#where_builder::new());
                            #query_builder::from_builder(self.pool.clone(), builder).sum::<f64>(#col_snake).await
                        }
                    }
                }
                "Real" => {
                    quote! {
                        pub async fn #sum_name<F>(&self, f: F) -> Result<f32, Box<dyn std::error::Error + Send + Sync>>
                        where
                            F: FnOnce(#where_builder) -> #where_builder,
                        {
                            let builder = f(#where_builder::new());
                            #query_builder::from_builder(self.pool.clone(), builder).sum::<f32>(#col_snake).await
                        }
                    }
                }
                _ => quote! {},
            }
        });
        quote! { #(#field_methods)* }
    };

    quote! {
        #(#jsonb_sub_accessors)*

        #[derive(Clone, Copy)]
        pub struct #conflict_field(&'static str);

        impl #conflict_field {
            pub fn new(name: &'static str) -> Self {
                Self(name)
            }

            pub fn name(self) -> &'static str {
                self.0
            }
        }

        pub trait #conflict_target {
            fn columns(self) -> Vec<&'static str>;
        }

        impl #conflict_target for #conflict_field {
            fn columns(self) -> Vec<&'static str> {
                vec![self.name()]
            }
        }

        #(#conflict_target_tuple_impls)*

        #[derive(Clone, Copy)]
        pub struct #conflict_selector {
            #(#conflict_selector_fields),*
        }

        impl #conflict_selector {
            pub fn new() -> Self {
                Self {
                    #(#conflict_selector_inits),*
                }
            }
        }

        #[derive(Clone)]
        pub struct #accessor_struct {
            pool: ConnectionPool,
            #(#jsonb_accessor_fields),*
        }
        impl std::fmt::Debug for #accessor_struct {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                f.debug_struct(stringify!(#accessor_struct))
                    .field("pool", &"<ConnectionPool>")
                    #(#jsonb_debug_fields)*
                    .finish()
            }
        }

        impl #accessor_struct {
            pub fn new(pool: ConnectionPool) -> Self {
                Self {
                    pool: pool.clone(),
                    #(#jsonb_accessor_inits),*
                }
            }
            pub fn query(&self) -> #query_builder {
                #query_builder::new(self.pool.clone())
            }
            pub async fn find_many<F>(&self, f: F) -> Result<Vec<#model_name>, Box<dyn std::error::Error + Send + Sync>>
            where
                F: FnOnce(#where_builder) -> #where_builder,
            {
                let builder = f(#where_builder::new());
                #query_builder::from_builder(self.pool.clone(), builder).await
            }
            pub async fn find_first<F>(&self, f: F) -> Result<Option<#model_name>, Box<dyn std::error::Error + Send + Sync>>
            where
                F: FnOnce(#where_builder) -> #where_builder,
            {
                let builder = f(#where_builder::new());
                #query_builder::from_builder(self.pool.clone(), builder).first().await
            }
            pub async fn sum<F, T>(&self, f: F, field: &str) -> Result<T, Box<dyn std::error::Error + Send + Sync>>
            where
                F: FnOnce(#where_builder) -> #where_builder,
                T: for<'a> tokio_postgres::types::FromSql<'a> + Default + tokio_postgres::types::ToSql + Sync,
            {
                let builder = f(#where_builder::new());
                #query_builder::from_builder(self.pool.clone(), builder).sum(field).await
            }
            pub async fn count<F>(&self, f: F) -> Result<i64, Box<dyn std::error::Error + Send + Sync>>
            where
                F: FnOnce(#where_builder) -> #where_builder,
            {
                let builder = f(#where_builder::new());
                #query_builder::from_builder(self.pool.clone(), builder).count().await
            }
            pub async fn avg<F, T>(&self, f: F, field: &str) -> Result<Option<T>, Box<dyn std::error::Error + Send + Sync>>
            where
                F: FnOnce(#where_builder) -> #where_builder,
                T: for<'a> tokio_postgres::types::FromSql<'a>,
            {
                let builder = f(#where_builder::new());
                #query_builder::from_builder(self.pool.clone(), builder).avg::<T>(field).await
            }
            pub async fn min<F, T>(&self, f: F, field: &str) -> Result<Option<T>, Box<dyn std::error::Error + Send + Sync>>
            where
                F: FnOnce(#where_builder) -> #where_builder,
                T: for<'a> tokio_postgres::types::FromSql<'a>,
            {
                let builder = f(#where_builder::new());
                #query_builder::from_builder(self.pool.clone(), builder).min::<T>(field).await
            }
            pub async fn max<F, T>(&self, f: F, field: &str) -> Result<Option<T>, Box<dyn std::error::Error + Send + Sync>>
            where
                F: FnOnce(#where_builder) -> #where_builder,
                T: for<'a> tokio_postgres::types::FromSql<'a>,
            {
                let builder = f(#where_builder::new());
                #query_builder::from_builder(self.pool.clone(), builder).max::<T>(field).await
            }
            #typed_agg_methods
            pub fn update<F>(&self, f: F) -> #update_builder
            where
                F: FnOnce(#update_builder) -> #update_builder,
            {
                let builder = #update_builder::new(self.pool.clone());
                f(builder)
            }
            pub fn upsert<F>(&self, f: F) -> #upsert_builder
            where
                F: FnOnce(#upsert_builder) -> #upsert_builder,
            {
                let builder = #upsert_builder::new(self.pool.clone());
                f(builder)
            }
            pub fn create<F>(&self, f: F) -> #create_builder
            where
                F: FnOnce(#create_builder) -> #create_builder,
            {
                let builder = #create_builder::new(self.pool.clone());
                f(builder)
            }
            pub fn delete<F>(&self, f: F) -> #delete_builder
            where
                F: FnOnce(#delete_builder) -> #delete_builder,
            {
                let builder = #delete_builder::new(self.pool.clone());
                f(builder)
            }
            pub async fn create_many(&self, records: Vec<std::collections::HashMap<&'static str, Box<dyn tokio_postgres::types::ToSql + Sync + Send>>>)
                -> Result<u64, Box<dyn std::error::Error + Send + Sync>>
            {
                if records.is_empty() {
                    return Ok(0);
                }

                let client = self.pool.get().await.map_err(|_| "Failed to get connection from pool")?;

                let first = &records[0];
                let mut columns: Vec<&str> = first.keys().copied().collect();
                columns.sort();
                let columns_str = columns.join(", ");

                let mut all_values: Vec<String> = Vec::with_capacity(records.len());
                let mut all_params: Vec<Box<dyn tokio_postgres::types::ToSql + Sync + Send>> = Vec::new();
                let mut param_idx = 1;

                for record in records {
                    let placeholders: Vec<String> = columns.iter().map(|_| {
                        let p = format!("${}", param_idx);
                        param_idx += 1;
                        p
                    }).collect();
                    all_values.push(format!("({})", placeholders.join(", ")));
                    for col in &columns {
                        if let Some(val) = record.get(col) {
                            all_params.push(unsafe { std::ptr::read(val) });
                        }
                    }
                }

                let sql = format!(
                    "INSERT INTO {} ({}) VALUES {}",
                    #table_name, columns_str, all_values.join(", ")
                );

                debug::log_query(&sql, all_params.len());

                let params: Vec<&(dyn tokio_postgres::types::ToSql + Sync)> =
                    all_params.iter().map(|b| b.as_ref() as &(dyn tokio_postgres::types::ToSql + Sync)).collect();
                let result = client.execute(&sql, &params[..]).await?;
                Ok(result)
            }

            pub async fn upsert_many<T, K, FC, FU>(
                &self,
                records: Vec<T>,
                conflict: impl FnOnce(#conflict_selector) -> K,
                mut create: FC,
                mut update: FU,
            ) -> Result<u64, Box<dyn std::error::Error + Send + Sync>>
            where
                T: Clone,
                K: #conflict_target,
                FC: FnMut(#create_builder, T) -> #create_builder,
                FU: FnMut(#update_builder, T) -> #update_builder,
            {
                if records.is_empty() {
                    return Ok(0);
                }

                let conflict_columns = conflict(#conflict_selector::new()).columns();
                if conflict_columns.is_empty() {
                    return Err("Conflict target cannot be empty".into());
                }

                let mut seen_conflict_columns = std::collections::HashSet::new();
                for column in &conflict_columns {
                    if !seen_conflict_columns.insert(*column) {
                        return Err(format!("Duplicate conflict column: {}", column).into());
                    }
                }

                let client = self.pool.get().await.map_err(|_| "Failed to get connection from pool")?;
                client.execute("BEGIN", &[]).await?;

                let result = async {
                    let enum_casts: std::collections::HashMap<&str, &str> = [
                        #(#enum_cast_entries),*
                    ].into_iter().collect();
                    let required_fields: Vec<&str> = vec![#(#required_fields),*];
                    let conflict_clause = conflict_columns.join(", ");
                    let mut affected_rows = 0u64;

                    for record in records {
                        let create_builder = create(#create_builder::new(self.pool.clone()), record.clone());
                        let update_builder = update(#update_builder::new(self.pool.clone()), record);

                        let #create_builder {
                            where_fragments: create_where_fragments,
                            set_values: mut create_set_values,
                            ..
                        } = create_builder;

                        let #update_builder {
                            where_fragments: update_where_fragments,
                            set_fragments: update_set_fragments,
                            set_args: update_set_args,
                            inc_ops: update_inc_ops,
                            ..
                        } = update_builder;

                        if !create_where_fragments.is_empty() {
                            return Err("upsert_many does not support create where clauses".into());
                        }

                        if !update_where_fragments.is_empty() {
                            return Err("upsert_many does not support update where clauses".into());
                        }

                        for req in &required_fields {
                            if !create_set_values.contains_key(req) {
                                return Err(format!("Missing required field: {}", req).into());
                            }
                        }

                        if create_set_values.is_empty() {
                            return Err("No fields to upsert".into());
                        }

                        for conflict_col in &conflict_columns {
                            if !create_set_values.contains_key(conflict_col) {
                                return Err(format!("Missing conflict field in create builder: {}", conflict_col).into());
                            }
                        }

                        if update_set_fragments.len() != update_set_args.len() {
                            return Err("Invalid update builder state".into());
                        }

                        let mut insert_columns: Vec<&str> = create_set_values.keys().copied().collect();
                        insert_columns.sort();
                        let columns_str = insert_columns.join(", ");
                        let insert_placeholders: Vec<String> = insert_columns
                            .iter()
                            .enumerate()
                            .map(|(i, col)| {
                                let idx = i + 1;
                                if let Some(enum_type) = enum_casts.get(col) {
                                    format!("${}::TEXT::{}", idx, enum_type)
                                } else {
                                    format!("${}", idx)
                                }
                            })
                            .collect();
                        let placeholders_str = insert_placeholders.join(", ");

                        let mut all_params: Vec<Box<dyn tokio_postgres::types::ToSql + Sync + Send>> = vec![];
                        for col in &insert_columns {
                            all_params.push(create_set_values.remove(col).unwrap());
                        }

                        let mut update_clauses: Vec<String> = vec![];
                        let mut param_idx = all_params.len() + 1;

                        for (col, arg) in update_set_fragments.iter().zip(update_set_args.into_iter()) {
                            if let Some(enum_type) = enum_casts.get(col) {
                                update_clauses.push(format!("{} = ${}::TEXT::{}", col, param_idx, enum_type));
                            } else {
                                update_clauses.push(format!("{} = ${}", col, param_idx));
                            }
                            all_params.push(arg);
                            param_idx += 1;
                        }

                        for (field, op, value) in update_inc_ops {
                            let clause = match op {
                                "inc" => format!("{} = COALESCE({}.{}, 0) + ${}", field, #table_name, field, param_idx),
                                "dec" => format!("{} = COALESCE({}.{}, 0) - ${}", field, #table_name, field, param_idx),
                                "mul" => format!("{} = COALESCE({}.{}, 0) * ${}", field, #table_name, field, param_idx),
                                "div" => format!("{} = COALESCE({}.{}, 0) / ${}", field, #table_name, field, param_idx),
                                _ => return Err(format!("Unsupported update operation: {}", op).into()),
                            };
                            update_clauses.push(clause);
                            all_params.push(Box::new(value));
                            param_idx += 1;
                        }

                        let sql = if update_clauses.is_empty() {
                            format!(
                                "INSERT INTO {} ({}) VALUES ({}) ON CONFLICT ({}) DO NOTHING",
                                #table_name, columns_str, placeholders_str, conflict_clause
                            )
                        } else {
                            format!(
                                "INSERT INTO {} ({}) VALUES ({}) ON CONFLICT ({}) DO UPDATE SET {}",
                                #table_name, columns_str, placeholders_str, conflict_clause, update_clauses.join(", ")
                            )
                        };

                        debug::log_query(&sql, all_params.len());

                        let params: Vec<&(dyn tokio_postgres::types::ToSql + Sync)> =
                            all_params.iter().map(|b| b.as_ref() as &(dyn tokio_postgres::types::ToSql + Sync)).collect();
                        affected_rows += client.execute(&sql, &params[..]).await?;
                    }

                    Ok(affected_rows)
                }.await;

                match result {
                    Ok(affected_rows) => {
                        client.execute("COMMIT", &[]).await?;
                        Ok(affected_rows)
                    }
                    Err(err) => {
                        let _ = client.execute("ROLLBACK", &[]).await;
                        Err(err)
                    }
                }
            }
            #find_unique
            #find_or_create
            pub async fn get_client(&self) -> Result<PooledClient<'_>, tokio_postgres::Error> {
                self.pool.get().await
            }
            pub fn pool(&self) -> &ConnectionPool {
                &self.pool
            }
        }
    }
}

pub fn generate_client_struct(
    schema: &Schema,
    jsonb_defaults: &HashMap<(String, String), String>,
) -> TokenStream {
    let model_accessors = schema.models.iter().map(|model| {
        let accessor_name = format_ident!("{}", to_snake_case(&model.name));
        let accessor_struct = format_ident!("{}Accessor", model.name);
        quote! { pub #accessor_name: #accessor_struct }
    });

    let accessor_structs = schema
        .models
        .iter()
        .map(|model| generate_accessor_for_model(model, jsonb_defaults));

    let accessor_inits = schema.models.iter().map(|model| {
        let accessor_name = format_ident!("{}", to_snake_case(&model.name));
        let accessor_struct = format_ident!("{}Accessor", model.name);
        quote! { #accessor_name: #accessor_struct::new(pool.clone()) }
    });

    let debug_accessor_fields = schema.models.iter().map(|model| {
        let accessor_name = to_snake_case(&model.name);
        let accessor_name_ident = format_ident!("{}", accessor_name);
        quote! { .field(#accessor_name, &self.#accessor_name_ident) }
    });

    let accessor_inits_clone = schema.models.iter().map(|model| {
        let accessor_name = format_ident!("{}", to_snake_case(&model.name));
        let accessor_struct = format_ident!("{}Accessor", model.name);
        quote! { #accessor_name: #accessor_struct::new(pool.clone()) }
    });

    quote! {
        #(#accessor_structs)*

        /// Enum to support both TLS and NoTLS connection pools
        #[derive(Clone)]
        pub enum ConnectionPool {
            Tls(Arc<bb8::Pool<bb8_postgres::PostgresConnectionManager<tokio_postgres_rustls::MakeRustlsConnect>>>),
            NoTls(Arc<bb8::Pool<bb8_postgres::PostgresConnectionManager<tokio_postgres::NoTls>>>),
            Pinned(Arc<tokio_postgres::Client>),
        }

        impl ConnectionPool {
            pub async fn get(&self) -> Result<PooledClient, tokio_postgres::Error> {
                match self {
                    ConnectionPool::Tls(pool) => {
                        let conn = pool.get().await.map_err(|_| tokio_postgres::Error::__private_api_timeout())?;
                        Ok(PooledClient::Tls(conn))
                    }
                    ConnectionPool::NoTls(pool) => {
                        let conn = pool.get().await.map_err(|_| tokio_postgres::Error::__private_api_timeout())?;
                        Ok(PooledClient::NoTls(conn))
                    }
                    ConnectionPool::Pinned(client) => {
                        Ok(PooledClient::Pinned(client.clone()))
                    }
                }
            }
        }

        /// Wrapper for pooled connections that works with both TLS and NoTLS
        pub enum PooledClient<'a> {
            Tls(bb8::PooledConnection<'a, bb8_postgres::PostgresConnectionManager<tokio_postgres_rustls::MakeRustlsConnect>>),
            NoTls(bb8::PooledConnection<'a, bb8_postgres::PostgresConnectionManager<tokio_postgres::NoTls>>),
            Pinned(Arc<tokio_postgres::Client>),
        }

        impl<'a> PooledClient<'a> {
            pub async fn query(&self, sql: &str, params: &[&(dyn tokio_postgres::types::ToSql + Sync)]) -> Result<Vec<tokio_postgres::Row>, tokio_postgres::Error> {
                match self {
                    PooledClient::Tls(c) => c.query(sql, params).await,
                    PooledClient::NoTls(c) => c.query(sql, params).await,
                    PooledClient::Pinned(c) => c.query(sql, params).await,
                }
            }

            pub async fn query_one(&self, sql: &str, params: &[&(dyn tokio_postgres::types::ToSql + Sync)]) -> Result<tokio_postgres::Row, tokio_postgres::Error> {
                match self {
                    PooledClient::Tls(c) => c.query_one(sql, params).await,
                    PooledClient::NoTls(c) => c.query_one(sql, params).await,
                    PooledClient::Pinned(c) => c.query_one(sql, params).await,
                }
            }

            pub async fn query_opt(&self, sql: &str, params: &[&(dyn tokio_postgres::types::ToSql + Sync)]) -> Result<Option<tokio_postgres::Row>, tokio_postgres::Error> {
                match self {
                    PooledClient::Tls(c) => c.query_opt(sql, params).await,
                    PooledClient::NoTls(c) => c.query_opt(sql, params).await,
                    PooledClient::Pinned(c) => c.query_opt(sql, params).await,
                }
            }

            pub async fn execute(&self, sql: &str, params: &[&(dyn tokio_postgres::types::ToSql + Sync)]) -> Result<u64, tokio_postgres::Error> {
                match self {
                    PooledClient::Tls(c) => c.execute(sql, params).await,
                    PooledClient::NoTls(c) => c.execute(sql, params).await,
                    PooledClient::Pinned(c) => c.execute(sql, params).await,
                }
            }
        }

        #[derive(Clone)]
        pub struct Client {
            pool: ConnectionPool,
            connection_string: Option<String>,
            #(#model_accessors),*
        }
        impl std::fmt::Debug for Client {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                f.debug_struct("Client")
                    .field("pool", &"<ConnectionPool>")
                    #(#debug_accessor_fields)*
                    .finish()
            }
        }
        impl Client {
            pub async fn new(connection_string: &str) -> Result<Self, Error> {
                let is_local = connection_string.contains("localhost") || connection_string.contains("127.0.0.1");
                let requires_ssl = connection_string.contains("sslmode=require") || connection_string.contains("sslmode=verify");

                let pool = if is_local && !requires_ssl {
                    let manager = bb8_postgres::PostgresConnectionManager::new_from_stringlike(
                        connection_string,
                        tokio_postgres::NoTls,
                    )?;
                    let pool = bb8::Pool::builder()
                        .max_size(20)
                        .build(manager)
                        .await?;
                    ConnectionPool::NoTls(Arc::new(pool))
                } else {
                    let root_store = rustls::RootCertStore {
                        roots: webpki_roots::TLS_SERVER_ROOTS.iter().cloned().collect(),
                    };
                    let tls_config = rustls::ClientConfig::builder()
                        .with_root_certificates(root_store)
                        .with_no_client_auth();
                    let tls = tokio_postgres_rustls::MakeRustlsConnect::new(tls_config);
                    let manager = bb8_postgres::PostgresConnectionManager::new_from_stringlike(
                        connection_string,
                        tls,
                    )?;
                    let pool = bb8::Pool::builder()
                        .max_size(20)
                        .build(manager)
                        .await?;
                    ConnectionPool::Tls(Arc::new(pool))
                };

                Ok(Self {
                    pool: pool.clone(),
                    connection_string: Some(connection_string.to_string()),
                    #(#accessor_inits),*
                })
            }

            pub fn from_pool(pool: ConnectionPool) -> Self {
                Self {
                    pool: pool.clone(),
                    connection_string: None,
                    #(#accessor_inits_clone),*
                }
            }

            pub async fn get_client(&self) -> Result<PooledClient<'_>, Error> {
                self.pool.get().await
            }

            pub fn pool(&self) -> &ConnectionPool { &self.pool }

            pub async fn transaction<F, T, E>(&self, f: F) -> Result<T, E>
            where
                F: FnOnce(Transaction<'_>) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<T, E>> + Send + '_>> + Send,
                E: From<tokio_postgres::Error> + Send,
                T: Send,
            {
                let client = self.pool.get().await.map_err(|e| E::from(e))?;
                match client {
                    PooledClient::Tls(mut c) => {
                        let tx = c.transaction().await?;
                        let transaction = Transaction { inner: tx };
                        let result = f(transaction).await;
                        result
                    }
                    PooledClient::NoTls(mut c) => {
                        let tx = c.transaction().await?;
                        let transaction = Transaction { inner: tx };
                        let result = f(transaction).await;
                        result
                    }
                    PooledClient::Pinned(_) => {
                        Err(E::from(tokio_postgres::Error::__private_api_timeout()))
                    }
                }
            }

            pub async fn begin(&self) -> Result<TxClient, Error> {
                let conn_str = self.connection_string.as_deref()
                    .ok_or_else(|| tokio_postgres::Error::__private_api_timeout())?;
                let is_local = conn_str.contains("localhost") || conn_str.contains("127.0.0.1");
                let requires_ssl = conn_str.contains("sslmode=require") || conn_str.contains("sslmode=verify");
                let client = if is_local && !requires_ssl {
                    let (client, connection) = tokio_postgres::connect(conn_str, tokio_postgres::NoTls).await?;
                    tokio::spawn(async move {
                        if let Err(e) = connection.await {
                            eprintln!("Transaction connection error: {}", e);
                        }
                    });
                    client
                } else {
                    let root_store = rustls::RootCertStore {
                        roots: webpki_roots::TLS_SERVER_ROOTS.iter().cloned().collect(),
                    };
                    let tls_config = rustls::ClientConfig::builder()
                        .with_root_certificates(root_store)
                        .with_no_client_auth();
                    let tls = tokio_postgres_rustls::MakeRustlsConnect::new(tls_config);
                    let (client, connection) = tokio_postgres::connect(conn_str, tls).await?;
                    tokio::spawn(async move {
                        if let Err(e) = connection.await {
                            eprintln!("Transaction connection error: {}", e);
                        }
                    });
                    client
                };
                client.execute("BEGIN", &[]).await?;
                let pinned = Arc::new(client);
                let pool = ConnectionPool::Pinned(pinned.clone());
                let inner = Self::from_pool(pool);
                Ok(TxClient { inner, pinned })
            }

            pub async fn execute_raw(&self, sql: &str, params: &[&(dyn tokio_postgres::types::ToSql + Sync)]) -> Result<u64, Error> {
                let client = self.pool.get().await?;
                client.execute(sql, params).await
            }

            pub async fn query_raw(&self, sql: &str, params: &[&(dyn tokio_postgres::types::ToSql + Sync)]) -> Result<Vec<tokio_postgres::Row>, Error> {
                let client = self.pool.get().await?;
                client.query(sql, params).await
            }
        }

        pub struct TxClient {
            pub inner: Client,
            pinned: Arc<tokio_postgres::Client>,
        }

        impl TxClient {
            pub async fn commit(self) -> Result<(), Error> {
                self.pinned.execute("COMMIT", &[]).await?;
                Ok(())
            }

            pub async fn rollback(self) -> Result<(), Error> {
                self.pinned.execute("ROLLBACK", &[]).await?;
                Ok(())
            }
        }

        impl std::ops::Deref for TxClient {
            type Target = Client;
            fn deref(&self) -> &Self::Target {
                &self.inner
            }
        }

        pub struct Transaction<'a> {
            inner: tokio_postgres::Transaction<'a>,
        }

        impl<'a> Transaction<'a> {
            pub async fn execute(&self, sql: &str, params: &[&(dyn tokio_postgres::types::ToSql + Sync)]) -> Result<u64, Error> {
                self.inner.execute(sql, params).await
            }

            pub async fn query(&self, sql: &str, params: &[&(dyn tokio_postgres::types::ToSql + Sync)]) -> Result<Vec<tokio_postgres::Row>, Error> {
                self.inner.query(sql, params).await
            }

            pub async fn commit(self) -> Result<(), Error> {
                self.inner.commit().await
            }

            pub async fn rollback(self) -> Result<(), Error> {
                self.inner.rollback().await
            }
        }
    }
}

use crate::codegen::utils::*;
use crate::types::*;
use proc_macro2::TokenStream;
use quote::{format_ident, quote};

struct FieldCategories<'a> {
    pk_fields: Vec<&'a Field>,
    numeric_fields: Vec<&'a Field>,
}

impl<'a> FieldCategories<'a> {
    fn from_model(model: &'a Model) -> Self {
        let mut pk_fields = Vec::new();
        let mut numeric_fields = Vec::new();

        for field in &model.fields {
            if field
                .modifiers
                .iter()
                .any(|m| matches!(m, Modifier::PrimaryKey))
            {
                pk_fields.push(field);
            }
            if is_numeric_type(&field.type_name) {
                numeric_fields.push(field);
            }
        }

        Self {
            pk_fields,
            numeric_fields,
        }
    }
}

fn generate_find_unique(model_name: &proc_macro2::Ident, model: &Model) -> TokenStream {
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

        let pk_args_call = pk_fields.iter().map(|pk| {
            let name = format_ident!("{}", to_snake_case(&pk.name));
            quote! { #name }
        });

        quote! {
            pub async fn find_unique(&self, #(#pk_params),*)
                -> Result<Option<#model_name>, Box<dyn std::error::Error + Send + Sync>>
            {
                #model_name::find_by_composite_pk(self.pool.clone(), #(#pk_args_call),*).await
            }
        }
    }
}

fn generate_find_or_create(
    model_name: &proc_macro2::Ident,
    model: &Model,
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

pub fn generate_accessor(model: &Model) -> TokenStream {
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
    let where_builder = format_ident!("{}WhereBuilder", model.name);
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

    let jsonb_fields = crate::codegen::jsonb::generate_jsonb_accessor_fields(model);
    let jsonb_struct_fields: Vec<_> = jsonb_fields
        .iter()
        .map(|(name, typ)| {
            quote! { pub #name: #typ }
        })
        .collect();
    let jsonb_inits: Vec<_> = jsonb_fields
        .iter()
        .map(|(name, typ)| {
            quote! { #name: #typ::new(pool.clone()) }
        })
        .collect();

    quote! {
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
            #(#jsonb_struct_fields),*
        }

        impl std::fmt::Debug for #accessor_struct {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                f.debug_struct(stringify!(#accessor_struct))
                    .field("pool", &"<ConnectionPool>")
                    .finish()
            }
        }

        impl #accessor_struct {
            pub fn new(pool: ConnectionPool) -> Self {
                Self { pool: pool.clone(), #(#jsonb_inits),* }
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

                let enum_casts: std::collections::HashMap<&str, &str> = [
                    #(#enum_cast_entries),*
                ].into_iter().collect();
                let required_fields: Vec<&str> = vec![#(#required_fields),*];
                let conflict_clause = conflict_columns.join(", ");
                let mut insert_columns_template: Option<Vec<&str>> = None;
                let mut update_columns_template: Option<Vec<&str>> = None;
                let mut all_values: Vec<String> = Vec::with_capacity(records.len());
                let mut all_params: Vec<Box<dyn tokio_postgres::types::ToSql + Sync + Send>> = vec![];
                let mut param_idx = 1;

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

                    if !update_inc_ops.is_empty() {
                        return Err("upsert_many does not support increment operations".into());
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

                    if let Some(expected) = &insert_columns_template {
                        if expected != &insert_columns {
                            return Err("upsert_many requires the same create columns for every record".into());
                        }
                    } else {
                        insert_columns_template = Some(insert_columns.clone());
                    }

                    let mut seen_update_columns = std::collections::HashSet::new();
                    let mut update_columns: Vec<&str> = Vec::new();

                    for col in update_set_fragments {
                        if !seen_update_columns.insert(col) {
                            return Err(format!("Duplicate update field in upsert_many: {}", col).into());
                        }
                        if !insert_columns.contains(&col) {
                            return Err(format!("Update field '{}' must also be set in create builder", col).into());
                        }
                        update_columns.push(col);
                    }

                    update_columns.sort();

                    if let Some(expected) = &update_columns_template {
                        if expected != &update_columns {
                            return Err("upsert_many requires the same update columns for every record".into());
                        }
                    } else {
                        update_columns_template = Some(update_columns);
                    }

                    let placeholders: Vec<String> = insert_columns.iter().map(|col| {
                        let placeholder = if let Some(enum_type) = enum_casts.get(col) {
                            format!("${}::TEXT::{}", param_idx, enum_type)
                        } else {
                            format!("${}", param_idx)
                        };
                        param_idx += 1;
                        placeholder
                    }).collect();

                    all_values.push(format!("({})", placeholders.join(", ")));

                    for col in &insert_columns {
                        all_params.push(create_set_values.remove(col).unwrap());
                    }
                }

                let insert_columns = insert_columns_template.ok_or("No fields to upsert")?;
                let update_columns = update_columns_template.unwrap_or_default();
                let columns_str = insert_columns.join(", ");
                let sql = if update_columns.is_empty() {
                    format!(
                        "INSERT INTO {} ({}) VALUES {} ON CONFLICT ({}) DO NOTHING",
                        #table_name, columns_str, all_values.join(", "), conflict_clause
                    )
                } else {
                    let update_clauses: Vec<String> = update_columns
                        .iter()
                        .map(|col| format!("{} = EXCLUDED.{}", col, col))
                        .collect();
                    format!(
                        "INSERT INTO {} ({}) VALUES {} ON CONFLICT ({}) DO UPDATE SET {}",
                        #table_name, columns_str, all_values.join(", "), conflict_clause, update_clauses.join(", ")
                    )
                };

                debug::log_query(&sql, all_params.len());

                let client = self.pool.get().await.map_err(|_| "Failed to get connection from pool")?;
                let params: Vec<&(dyn tokio_postgres::types::ToSql + Sync)> =
                    all_params.iter().map(|b| b.as_ref() as &(dyn tokio_postgres::types::ToSql + Sync)).collect();
                let result = client.execute(&sql, &params[..]).await?;
                Ok(result)
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

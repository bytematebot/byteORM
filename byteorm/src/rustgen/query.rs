use crate::rustgen::{generate_field_gets, rust_type_from_schema, to_snake_case};
use crate::{Model, Modifier};
use proc_macro2::TokenStream;
use quote::{format_ident, quote};

pub fn generate_query_builder_struct(model: &Model) -> TokenStream {
    let model_name = format_ident!("{}", model.name);
    let builder_name = format_ident!("{}Query", model.name);
    let where_builder_name = format_ident!("{}WhereBuilder", model.name);
    let table_name = model.name.to_lowercase();

    let where_methods = model.fields.iter().flat_map(|field| {
        let method_name = format_ident!("where_{}", to_snake_case(&field.name));
        let method_in = format_ident!("where_{}_in", to_snake_case(&field.name));
        let is_nullable = field.modifiers.iter().any(|m| matches!(m, Modifier::Nullable));
        let field_type = rust_type_from_schema(&field.type_name, is_nullable);
        let field_col = to_snake_case(&field.name);

        let base_method = quote! {
            pub fn #method_name(mut self, value: #field_type) -> Self {
                let param_idx = self.args.len() + 1;
                self.where_clauses.push(format!("{} = ${}", #field_col, param_idx));
                self.args.push(Box::new(value) as Box<dyn tokio_postgres::types::ToSql + Sync + Send>);
                self
            }
        };

        let in_method = quote! {
            pub fn #method_in(mut self, values: Vec<#field_type>) -> Self {
                if values.is_empty() {
                    return self;
                }
                let start_idx = self.args.len() + 1;
                let placeholders: Vec<String> = (start_idx..start_idx + values.len())
                    .map(|i| format!("${}", i))
                    .collect();
                self.where_clauses.push(format!("{} IN ({})", #field_col, placeholders.join(", ")));
                for value in values {
                    self.args.push(Box::new(value) as Box<dyn tokio_postgres::types::ToSql + Sync + Send>);
                }
                self
            }
        };

        let null_methods = if is_nullable {
            let method_is_null = format_ident!("where_{}_is_null", to_snake_case(&field.name));
            let method_is_not_null = format_ident!("where_{}_is_not_null", to_snake_case(&field.name));
            vec![
                quote! {
                    pub fn #method_is_null(mut self) -> Self {
                        self.where_clauses.push(format!("{} IS NULL", #field_col));
                        self
                    }
                },
                quote! {
                    pub fn #method_is_not_null(mut self) -> Self {
                        self.where_clauses.push(format!("{} IS NOT NULL", #field_col));
                        self
                    }
                }
            ]
        } else {
            vec![]
        };

        let mut methods = vec![base_method, in_method];
        methods.extend(null_methods);

        if field.type_name == "TimestamptZ" {
            let method_gt = format_ident!("where_{}_gt", to_snake_case(&field.name));
            let method_lt = format_ident!("where_{}_lt", to_snake_case(&field.name));
            let method_gte = format_ident!("where_{}_gte", to_snake_case(&field.name));
            let method_lte = format_ident!("where_{}_lte", to_snake_case(&field.name));

            methods.extend(vec![
                quote! {
                    pub fn #method_gt(mut self, value: #field_type) -> Self {
                        let param_idx = self.args.len() + 1;
                        self.where_clauses.push(format!("{} > ${}", #field_col, param_idx));
                        self.args.push(Box::new(value) as Box<dyn tokio_postgres::types::ToSql + Sync + Send>);
                        self
                    }
                },
                quote! {
                    pub fn #method_lt(mut self, value: #field_type) -> Self {
                        let param_idx = self.args.len() + 1;
                        self.where_clauses.push(format!("{} < ${}", #field_col, param_idx));
                        self.args.push(Box::new(value) as Box<dyn tokio_postgres::types::ToSql + Sync + Send>);
                        self
                    }
                },
                quote! {
                    pub fn #method_gte(mut self, value: #field_type) -> Self {
                        let param_idx = self.args.len() + 1;
                        self.where_clauses.push(format!("{} >= ${}", #field_col, param_idx));
                        self.args.push(Box::new(value) as Box<dyn tokio_postgres::types::ToSql + Sync + Send>);
                        self
                    }
                },
                quote! {
                    pub fn #method_lte(mut self, value: #field_type) -> Self {
                        let param_idx = self.args.len() + 1;
                        self.where_clauses.push(format!("{} <= ${}", #field_col, param_idx));
                        self.args.push(Box::new(value) as Box<dyn tokio_postgres::types::ToSql + Sync + Send>);
                        self
                    }
                }
            ]);
        }

        methods
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

    let computed_where_methods = model.computed_fields.iter().map(|cf| {
        let snake = to_snake_case(&cf.name);
        let method_gt = format_ident!("where_{}_gt", snake);
        let method_lt = format_ident!("where_{}_lt", snake);
        let method_eq = format_ident!("where_{}_eq", snake);
        let expr = cf.expression.clone();

        quote! {
            pub fn #method_gt<V>(mut self, value: V) -> Self
            where V: tokio_postgres::types::ToSql + Sync + Send + 'static
            {
                let param_idx = self.args.len() + 1;
                self.where_clauses.push(format!("({}) > ${}", #expr, param_idx));
                self.args.push(Box::new(value) as Box<dyn tokio_postgres::types::ToSql + Sync + Send>);
                self
            }

            pub fn #method_lt<V>(mut self, value: V) -> Self
            where V: tokio_postgres::types::ToSql + Sync + Send + 'static
            {
                let param_idx = self.args.len() + 1;
                self.where_clauses.push(format!("({}) < ${}", #expr, param_idx));
                self.args.push(Box::new(value) as Box<dyn tokio_postgres::types::ToSql + Sync + Send>);
                self
            }

            pub fn #method_eq<V>(mut self, value: V) -> Self
            where V: tokio_postgres::types::ToSql + Sync + Send + 'static
            {
                let param_idx = self.args.len() + 1;
                self.where_clauses.push(format!("({}) = ${}", #expr, param_idx));
                self.args.push(Box::new(value) as Box<dyn tokio_postgres::types::ToSql + Sync + Send>);
                self
            }
        }
    });

    let computed_order_by_methods = model.computed_fields.iter().map(|cf| {
        let snake = to_snake_case(&cf.name);
        let asc_method = format_ident!("order_by_{}_asc", snake);
        let desc_method = format_ident!("order_by_{}_desc", snake);
        let expr = cf.expression.clone();

        quote! {
            pub fn #asc_method(mut self) -> Self {
                self.order_by.push((format!("({})", #expr), "ASC".to_string()));
                self
            }
            pub fn #desc_method(mut self) -> Self {
                self.order_by.push((format!("({})", #expr), "DESC".to_string()));
                self
            }
        }
    });

    let where_builder_struct = quote! {
        pub struct #where_builder_name {
            where_clauses: Vec<String>,
            order_by: Vec<(String, String)>,
            args: Vec<Box<dyn tokio_postgres::types::ToSql + Sync + Send>>,
            limit: Option<usize>,
            offset: Option<usize>,
        }

        impl #where_builder_name {
            pub fn new() -> Self {
                Self {
                    where_clauses: vec![],
                    order_by: vec![],
                    args: vec![],
                    limit: None,
                    offset: None,
                }
            }
            #(#where_methods)*
            #(#computed_where_methods)*
            #(#order_by_methods)*
            #(#computed_order_by_methods)*

            pub fn limit(mut self, limit: usize) -> Self {
                self.limit = Some(limit);
                self
            }

            pub fn offset(mut self, offset: usize) -> Self {
                self.offset = Some(offset);
                self
            }
        }
    };

    let field_gets = generate_field_gets(model);

    let builder_struct = quote! {
        pub struct #builder_name {
            pool: Arc<bb8::Pool<bb8_postgres::PostgresConnectionManager<tokio_postgres::NoTls>>>,
            table: String,
            where_clauses: Vec<String>,
            args: Vec<Box<dyn tokio_postgres::types::ToSql + Sync + Send>>,
            limit: Option<usize>,
            offset: Option<usize>,
            order_by: Vec<(String, String)>,
            fut: Option<std::pin::Pin<Box<dyn std::future::Future<Output = Result<Vec<#model_name>, Box<dyn std::error::Error + Send + Sync>>> + Send>>>,
        }

        unsafe impl Send for #builder_name {}

        impl #builder_name {
            pub fn new(pool: Arc<bb8::Pool<bb8_postgres::PostgresConnectionManager<tokio_postgres::NoTls>>>) -> Self {
                Self {
                    pool,
                    table: #table_name.to_string(),
                    where_clauses: vec![],
                    args: vec![],
                    limit: None,
                    offset: None,
                    order_by: vec![],
                    fut: None,
                }
            }

            pub fn from_builder(pool: Arc<bb8::Pool<bb8_postgres::PostgresConnectionManager<tokio_postgres::NoTls>>>, builder: #where_builder_name) -> Self {
                Self {
                    pool,
                    table: #table_name.to_string(),
                    where_clauses: builder.where_clauses,
                    args: builder.args,
                    limit: builder.limit,
                    offset: builder.offset,
                    order_by: builder.order_by,
                    fut: None,
                }
            }

            pub fn limit(mut self, limit: usize) -> Self {
                self.limit = Some(limit);
                self
            }

            pub fn offset(mut self, offset: usize) -> Self {
                self.offset = Some(offset);
                self
            }

            pub async fn first(self)
                -> Result<Option<#model_name>, Box<dyn std::error::Error + Send + Sync>>
            {
                let result = self.limit(1).await?;
                Ok(result.into_iter().next())
            }

            pub async fn count(self)
                -> Result<i64, Box<dyn std::error::Error + Send + Sync>>
            {
                let mut sql = format!("SELECT COUNT(*) FROM {}", self.table);
                let params: Vec<&(dyn tokio_postgres::types::ToSql + Sync)> =
                    self.args.iter().map(|b| b.as_ref() as &(dyn tokio_postgres::types::ToSql + Sync)).collect();

                if !self.where_clauses.is_empty() {
                    let where_clauses: Vec<String> = self.where_clauses.iter().map(|s| s.clone()).collect();
                    sql.push_str(" WHERE ");
                    sql.push_str(&where_clauses.join(" AND "));
                }

                debug::log_query(&sql, params.len());

                let client = self.pool.get().await.map_err(|_| "Failed to get connection from pool")?;
                let row = client.query_one(&sql, &params[..]).await?;
                Ok(row.get(0))
            }

            pub async fn aggregate<T>(self, field: &str, func: &str)
                -> Result<Option<T>, Box<dyn std::error::Error + Send + Sync>>
            where
                T: for<'a> tokio_postgres::types::FromSql<'a>,
            {
                let func_upper = func.to_uppercase();
                let mut sql = format!("SELECT {}({}) FROM {}", func_upper, field, self.table);
                let params: Vec<&(dyn tokio_postgres::types::ToSql + Sync)> =
                    self.args.iter().map(|b| b.as_ref() as &(dyn tokio_postgres::types::ToSql + Sync)).collect();

                if !self.where_clauses.is_empty() {
                    let where_clauses: Vec<String> = self.where_clauses.iter().map(|s| s.clone()).collect();
                    sql.push_str(" WHERE ");
                    sql.push_str(&where_clauses.join(" AND "));
                }

                debug::log_query(&sql, params.len());

                let client = self.pool.get().await.map_err(|_| "Failed to get connection from pool")?;
                let row = client.query_one(&sql, &params[..]).await?;
                Ok(row.get(0))
            }

            pub async fn sum<T>(self, field: &str)
                -> Result<T, Box<dyn std::error::Error + Send + Sync>>
            where
                T: for<'a> tokio_postgres::types::FromSql<'a> + Default + tokio_postgres::types::ToSql + Sync,
            {
                let default_val = T::default();
                let default_idx = self.args.len() + 1;
                let mut sql = format!("SELECT COALESCE(SUM({}), ${}) FROM {}", field, default_idx, self.table);
                let mut params: Vec<&(dyn tokio_postgres::types::ToSql + Sync)> =
                    self.args.iter().map(|b| b.as_ref() as &(dyn tokio_postgres::types::ToSql + Sync)).collect();

                if !self.where_clauses.is_empty() {
                    let where_clauses: Vec<String> = self.where_clauses.iter().map(|s| s.clone()).collect();
                    sql.push_str(" WHERE ");
                    sql.push_str(&where_clauses.join(" AND "));
                }

                params.push(&default_val);

                debug::log_query(&sql, params.len());

                let client = self.pool.get().await.map_err(|_| "Failed to get connection from pool")?;
                let row = client.query_one(&sql, &params[..]).await?;
                Ok(row.get(0))
            }

            pub async fn sum_cast_i64(self, field: &str)
                -> Result<i64, Box<dyn std::error::Error + Send + Sync>>
            {
                let default_val: i64 = 0;
                let default_idx = self.args.len() + 1;
                let mut sql = format!(
                    "SELECT COALESCE(CAST(SUM({}) AS BIGINT), ${}) FROM {}",
                    field, default_idx, self.table
                );
                let mut params: Vec<&(dyn tokio_postgres::types::ToSql + Sync)> =
                    self.args.iter().map(|b| b.as_ref() as &(dyn tokio_postgres::types::ToSql + Sync)).collect();

                if !self.where_clauses.is_empty() {
                    let where_clauses: Vec<String> = self.where_clauses.iter().map(|s| s.clone()).collect();
                    sql.push_str(" WHERE ");
                    sql.push_str(&where_clauses.join(" AND "));
                }

                params.push(&default_val);

                debug::log_query(&sql, params.len());

                let client = self.pool.get().await.map_err(|_| "Failed to get connection from pool")?;
                let row = client.query_one(&sql, &params[..]).await?;
                Ok(row.get(0))
            }

            pub async fn avg<T>(self, field: &str)
                -> Result<Option<T>, Box<dyn std::error::Error + Send + Sync>>
            where
                T: for<'a> tokio_postgres::types::FromSql<'a>,
            {
                self.aggregate(field, "AVG").await
            }

            pub async fn min<T>(self, field: &str)
                -> Result<Option<T>, Box<dyn std::error::Error + Send + Sync>>
            where
                T: for<'a> tokio_postgres::types::FromSql<'a>,
            {
                self.aggregate(field, "MIN").await
            }

            pub async fn max<T>(self, field: &str)
                -> Result<Option<T>, Box<dyn std::error::Error + Send + Sync>>
            where
                T: for<'a> tokio_postgres::types::FromSql<'a>,
            {
                self.aggregate(field, "MAX").await
            }
        }

        impl Future for #builder_name {
            type Output = Result<Vec<#model_name>, Box<dyn std::error::Error + Send + Sync>>;

            fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
                let me = &mut *self;

                if me.fut.is_none() {
                    let pool = me.pool.clone();
                    let table = me.table.clone();
                    let where_clauses = me.where_clauses.clone();
                    let limit = me.limit;
                    let offset = me.offset;
                    let order_by = me.order_by.clone();
                    let args = std::mem::take(&mut me.args);

                    let fut = async move {
                        let mut sql = format!("SELECT * FROM {}", table);
                        let params: Vec<&(dyn tokio_postgres::types::ToSql + Sync)> =
                            args.iter().map(|b| b.as_ref() as &(dyn tokio_postgres::types::ToSql + Sync)).collect();

                        if !where_clauses.is_empty() {
                            let where_parts: Vec<String> = where_clauses.iter().map(|s| s.clone()).collect();
                            sql.push_str(" WHERE ");
                            sql.push_str(&where_parts.join(" AND "));
                        }

                        if !order_by.is_empty() {
                            let order_clauses: Vec<String> = order_by.iter()
                                .map(|(col, dir)| format!("{} {}", col, dir))
                                .collect();
                            sql.push_str(" ORDER BY ");
                            sql.push_str(&order_clauses.join(", "));
                        }

                        if let Some(limit) = limit {
                            sql.push_str(&format!(" LIMIT {}", limit));
                        }

                        if let Some(offset) = offset {
                            sql.push_str(&format!(" OFFSET {}", offset));
                        }

                        debug::log_query(&sql, params.len());

                        let client = pool.get().await.map_err(|_| "Failed to get connection from pool")?;
                        let rows = client.query(&sql, &params[..]).await?;
                        Ok(rows.into_iter().map(|row| #model_name {
                            #(#field_gets),*
                        }).collect())
                    };
                    me.fut = Some(Box::pin(fut));
                }

                me.fut.as_mut().unwrap().as_mut().poll(cx)
            }
        }
    };

    quote! {
        #where_builder_struct

        #builder_struct
    }
}

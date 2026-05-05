use proc_macro2::TokenStream;
use quote::{format_ident, quote};
use crate::types::*;

pub fn generate_where_methods<'a>(
    model: &'a Model,
    target_args: &'a str,
    target_fragments: &'a str,
) -> impl Iterator<Item = TokenStream> + 'a {
    crate::codegen::utils::generate_where_methods_with_equals(model, target_args, target_fragments)
}

pub use crate::codegen::utils::{
    rust_type_from_schema,
    to_snake_case,
    capitalize_first,
    is_numeric_type,
    is_builtin_type,
    generate_select_columns,
    generate_from_row_impl,
    pk_args,
    generate_set_methods,
    generate_inc_methods,
};

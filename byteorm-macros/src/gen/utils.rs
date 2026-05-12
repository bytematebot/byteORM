use crate::types::*;
use proc_macro2::TokenStream;
use quote::{format_ident, quote};

pub fn generate_where_methods<'a>(
    model: &'a Model,
    target_args: &'a str,
    target_fragments: &'a str,
) -> impl Iterator<Item = TokenStream> + 'a {
    crate::codegen::utils::generate_where_methods_with_equals(model, target_args, target_fragments)
}

pub use crate::codegen::utils::{
    capitalize_first, generate_from_row_impl, generate_inc_methods, generate_select_columns,
    generate_set_methods, is_builtin_type, is_numeric_type, pk_args, rust_type_from_schema,
    to_snake_case,
};

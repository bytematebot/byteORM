extern crate proc_macro;

mod types;
mod parse;
mod codegen;

use proc_macro::TokenStream;
use quote::quote;
use syn::{parse_macro_input, DeriveInput};

#[proc_macro_derive(ByteOrm, attributes(byteorm))]
pub fn derive_byteorm(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    let model = parse::parse_model(&input);

    let from_row_impl = codegen::utils::generate_from_row_impl(&model);
    let model_impl = codegen::model::generate_model_impl(&model);
    let query_builder = codegen::query::generate_query_builder_struct(&model);
    let update_builder = codegen::update::generate_update_builder(&model);
    let create_builder = codegen::create::generate_create_builder(&model);
    let delete_builder = codegen::delete::generate_delete_builder(&model);
    let upsert_builder = codegen::upsert::generate_upsert_builder(&model);
    let jsonb_sub_accessors = codegen::jsonb::generate_jsonb_sub_accessors(&model);
    let accessor = codegen::client::generate_accessor(&model);

    let expanded = quote! {
        #from_row_impl
        #model_impl
        #query_builder
        #update_builder
        #create_builder
        #delete_builder
        #upsert_builder
        #(#jsonb_sub_accessors)*
        #accessor
    };

    TokenStream::from(expanded)
}

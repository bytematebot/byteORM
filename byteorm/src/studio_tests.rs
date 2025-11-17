#[cfg(test)]
mod tests {
    use super::*;
    use axum::{Router, routing::{get, post}};
    use byteorm_lib::{Schema, Model, Field, Modifier};
    use crate::studio::{run};

    #[test]
    fn compile_schema_types() {
        let schema = Schema { models: vec![ Model { name: "User".into(), fields: vec![ Field { name: "id".into(), type_name: "Int".into(), modifiers: vec![Modifier::PrimaryKey], attributes: vec![] }, Field { name: "name".into(), type_name: "String".into(), modifiers: vec![], attributes: vec![] } ], computed_fields: vec![], model_attributes: vec![] } ] };
        assert_eq!(schema.models.len(), 1);
    }
}

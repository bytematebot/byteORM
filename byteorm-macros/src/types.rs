#[derive(Debug, Clone)]
pub struct Model {
    pub name: String,
    pub fields: Vec<Field>,
    pub computed_fields: Vec<ComputedField>,
    pub table_name: String,
}

#[derive(Debug, Clone)]
pub struct Field {
    pub name: String,
    pub type_name: String,
    pub modifiers: Vec<Modifier>,
    pub attributes: Vec<Attribute>,
}

impl Field {
    pub fn get_jsonb_default_path(&self) -> Option<String> {
        if self.type_name == "JsonB" {
            for attr in &self.attributes {
                if attr.name == "jsonb_default" {
                    return attr.args.clone();
                }
            }
        }
        None
    }
}

#[derive(Debug, Clone)]
pub enum Modifier {
    PrimaryKey,
    Nullable,
    Unique,
    Index,
    ForeignKey {
        model: String,
        field: Option<String>,
    },
}

#[derive(Debug, Clone)]
pub struct Attribute {
    pub name: String,
    pub args: Option<String>,
}

#[derive(Debug, Clone)]
pub struct ComputedField {
    pub name: String,
    pub expression: String,
}

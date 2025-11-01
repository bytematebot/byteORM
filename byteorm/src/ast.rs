use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Schema {
    pub models: Vec<Model>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Model {
    pub name: String,
    pub fields: Vec<Field>,
    pub computed_fields: Vec<ComputedField>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Field {
    pub name: String,
    pub type_name: String,
    pub modifiers: Vec<Modifier>,
    pub attributes: Vec<Attribute>,
}

impl Field {
    pub fn get_default_value(&self) -> Option<String> {
        for attr in &self.attributes {
            if attr.name == "default" {
                return attr.args.as_ref().map(|args| {
                    let trimmed = if args.starts_with('(') && args.ends_with(')') {
                        &args[1..args.len()-1]
                    } else {
                        args
                    };

                    if (trimmed.starts_with('\'') && trimmed.ends_with('\'')) ||
                        (trimmed.starts_with('"') && trimmed.ends_with('"')) {
                        trimmed[1..trimmed.len()-1].to_string()
                    } else {
                        trimmed.to_string()
                    }
                });
            }
        }
        None
    }

    pub fn is_jsonb_default_file(&self) -> bool {
        self.get_default_value()
            .map(|v| v.ends_with(".json"))
            .unwrap_or(false)
    }

    pub fn get_jsonb_default_path(&self) -> Option<String> {
        if self.type_name == "JsonB" && self.is_jsonb_default_file() {
            self.get_default_value()
        } else {
            None
        }
    }

    pub fn get_audit_model(&self) -> Option<String> {
        for attr in &self.attributes {
            if attr.name == "audit" {
                return attr.args.as_ref().map(|args| {

                    let trimmed = if args.starts_with('(') && args.ends_with(')') {
                        &args[1..args.len()-1]
                    } else {
                        args
                    };
                    trimmed.trim().to_string()
                });
            }
        }
        None
    }

    pub fn is_sql_default(&self) -> bool {
        self.get_default_value().is_some() && !self.is_jsonb_default_file()
    }


    pub fn get_default_from_file(&self) -> Result<String, Box<dyn std::error::Error>> {
        if let Some(value) = self.get_default_value() {
            if value.starts_with("./") || value.starts_with("../") {
                let content = std::fs::read_to_string(&value)?;
                return Ok(content);
            }
            Ok(value)
        } else {
            Err("No default specified".into())
        }
    }
}



#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Modifier {
    PrimaryKey,
    NotNull,
    Nullable,
    Unique,
    ForeignKey { model: String, field: Option<String> },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Attribute {
    pub name: String,
    pub args: Option<String>,
}


#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ComputedField {
    pub name: String,
    pub expression: String,
}

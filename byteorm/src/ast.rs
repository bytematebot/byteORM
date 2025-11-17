use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Schema {
    pub models: Vec<Model>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Model {
    pub name: String,
    pub fields: Vec<Field>,
    pub computed_fields: Vec<ComputedField>,
    pub model_attributes: Vec<ModelAttribute>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ModelAttribute {
    pub name: String,
    pub args: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
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
                        &args[1..args.len() - 1]
                    } else {
                        args
                    };

                    if (trimmed.starts_with('\'') && trimmed.ends_with('\''))
                        || (trimmed.starts_with('"') && trimmed.ends_with('"'))
                    {
                        trimmed[1..trimmed.len() - 1].to_string()
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
                        &args[1..args.len() - 1]
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

    pub fn sql_default_literal(&self) -> Option<String> {
        if !self.is_sql_default() {
            return None;
        }

        let value = self.get_default_value()?;
        let raw_types = ["Boolean", "Real", "Float", "Int", "BigInt", "Serial"];
        if raw_types.contains(&self.type_name.as_str()) || value == "now()" {
            Some(value)
        } else {
            Some(format!("'{}'", value))
        }
    }

    pub fn is_nullable_column(&self) -> bool {
        if self
            .modifiers
            .iter()
            .any(|m| matches!(m, Modifier::NotNull))
        {
            false
        } else if self
            .modifiers
            .iter()
            .any(|m| matches!(m, Modifier::Nullable))
        {
            true
        } else {
            true
        }
    }

    pub fn has_index(&self) -> bool {
        self.modifiers.iter().any(|m| matches!(m, Modifier::Index))
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

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum Modifier {
    PrimaryKey,
    NotNull,
    Nullable,
    Unique,
    Index,
    ForeignKey {
        model: String,
        field: Option<String>,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Attribute {
    pub name: String,
    pub args: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ComputedField {
    pub name: String,
    pub expression: String,
}

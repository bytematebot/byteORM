use crate::ast::*;
use crate::{Rule, SchemaParser};
use pest::Parser;
use pest::iterators::Pair;

pub fn parse_schema(src: &str) -> Result<Schema, String> {
    SchemaParser::parse(Rule::schema, src)
        .map_err(|e| format!("Parse error: {}", e))
        .and_then(|mut pairs| {
            let schema_pair = pairs.next().ok_or("Empty schema")?;
            Ok(build_ast(schema_pair))
        })
}

pub(crate) fn build_ast(pair: Pair<Rule>) -> Schema {
    let mut models = Vec::new();
    let mut enums = Vec::new();
    for inner in pair.into_inner() {
        match inner.as_rule() {
            Rule::model_decl => models.push(parse_model(inner)),
            Rule::enum_decl => enums.push(parse_enum(inner)),
            _ => (),
        }
    }
    Schema { models, enums }
}

fn parse_enum(pair: Pair<Rule>) -> Enum {
    let mut pairs = pair.into_inner();
    let name = pairs.next().unwrap().as_str().to_string();
    let mut values = Vec::new();
    for pair in pairs {
        match pair.as_rule() {
            Rule::enum_value => {
                let value = pair.into_inner().next().unwrap().as_str().to_string();
                values.push(value);
            }
            _ => (),
        }
    }
    Enum { name, values }
}

fn parse_model(pair: Pair<Rule>) -> Model {
    let mut pairs = pair.into_inner();
    let name = pairs.next().unwrap().as_str().to_string();
    let mut fields = Vec::new();
    let mut computed_fields = Vec::new();
    let mut model_attributes = Vec::new();
    for pair in pairs {
        match pair.as_rule() {
            Rule::field_decl => fields.push(parse_field(pair)),
            Rule::computed_decl => computed_fields.push(parse_computed(pair)),
            Rule::model_attr => model_attributes.push(parse_model_attribute(pair)),
            Rule::model_member => {
                for inner in pair.into_inner() {
                    match inner.as_rule() {
                        Rule::field_decl => fields.push(parse_field(inner)),
                        Rule::computed_decl => computed_fields.push(parse_computed(inner)),
                        Rule::model_attr => model_attributes.push(parse_model_attribute(inner)),
                        _ => (),
                    }
                }
            }
            _ => (),
        }
    }
    Model {
        name,
        fields,
        computed_fields,
        model_attributes,
    }
}

fn parse_computed(pair: Pair<Rule>) -> ComputedField {
    // computed_decl -> computed_attr -> ident string
    let mut name = String::new();
    let mut expression = String::new();

    for inner in pair.into_inner() {
        for p in inner.into_inner() {
            match p.as_rule() {
                Rule::ident => name = p.as_str().to_string(),
                Rule::string => {
                    let s = p.as_str();
                    if s.len() >= 2 && s.starts_with('"') && s.ends_with('"') {
                        expression = s[1..s.len() - 1].to_string();
                    } else {
                        expression = s.to_string();
                    }
                }
                _ => (),
            }
        }
    }

    ComputedField { name, expression }
}

fn parse_field(pair: Pair<Rule>) -> Field {
    let mut pairs = pair.into_inner();
    let name = pairs.next().unwrap().as_str().to_string();
    let mut raw_type_name = pairs.next().unwrap().as_str().to_string();

    let mut modifiers = Vec::new();
    let mut attributes = Vec::new();

    let mut is_nullable = false;
    if raw_type_name.ends_with('?') {
        is_nullable = true;
        raw_type_name = raw_type_name.trim_end_matches('?').to_string();
    }

    for pair in pairs {
        match pair.as_rule() {
            Rule::modifier => {
                if let Ok(m) = parse_modifier(pair) {
                    modifiers.push(m);
                }
            }
            Rule::attr => attributes.push(parse_attribute(pair)),
            _ => (),
        }
    }

    if is_nullable {
        modifiers.push(Modifier::Nullable);
    } else if !modifiers
        .iter()
        .any(|m| matches!(m, Modifier::NotNull | Modifier::PrimaryKey))
    {
        modifiers.push(Modifier::NotNull);
    }

    Field {
        name,
        type_name: raw_type_name,
        modifiers,
        attributes,
    }
}

fn parse_modifier(pair: Pair<Rule>) -> Result<Modifier, String> {
    let mut inner = pair.into_inner();
    let mod_name = inner.next().unwrap().as_str();
    let args = inner.next().map(|p| {
        p.into_inner()
            .next()
            .map(|ac| ac.as_str().trim().to_string())
            .unwrap_or_default()
    });

    match mod_name {
        "PrimaryKey" => Ok(Modifier::PrimaryKey),
        "NotNull" => Ok(Modifier::NotNull),
        "Nullable" => Ok(Modifier::Nullable),
        "Unique" => Ok(Modifier::Unique),
        "Index" => Ok(Modifier::Index),
        "ForeignKey" => parse_foreign_key(args),
        _ => Err(format!("Unknown modifier: {}", mod_name)),
    }
}

fn parse_model_attribute(pair: Pair<Rule>) -> ModelAttribute {
    let mut inner = pair.into_inner();
    let name = inner.next().unwrap().as_str().to_string();
    let args = inner.next().map(|p| p.as_str().to_string());
    ModelAttribute { name, args }
}

fn parse_foreign_key(args: Option<String>) -> Result<Modifier, String> {
    let arg_str = args.ok_or("ForeignKey requires arguments")?;

    let mut on_delete = None;
    let mut reference = arg_str.as_str();

    if let Some(comma_pos) = arg_str.find(',') {
        reference = arg_str[..comma_pos].trim();
        let options = arg_str[comma_pos + 1..].trim();
        
        if options.contains("onDelete:") || options.contains("onDelete :") {
            if options.contains("cascade") {
                on_delete = Some(ForeignKeyAction::Cascade);
            } else if options.contains("no action") {
                on_delete = Some(ForeignKeyAction::NoAction);
            } else if options.contains("set null") {
                on_delete = Some(ForeignKeyAction::SetNull);
            } else if options.contains("restrict") {
                on_delete = Some(ForeignKeyAction::Restrict);
            }
        }
    }

    if let Some(dot_pos) = reference.find('.') {
        let model = reference[..dot_pos].trim().to_string();
        let field = reference[dot_pos + 1..].trim().to_string();
        return Ok(Modifier::ForeignKey {
            model,
            field: Some(field),
            on_delete,
        });
    }

    if let Some(paren_pos) = reference.find('(') {
        let model = reference[..paren_pos].trim().to_string();
        if let Some(close_pos) = reference.find(')') {
            let field = reference[paren_pos + 1..close_pos].trim().to_string();
            return Ok(Modifier::ForeignKey {
                model,
                field: Some(field),
                on_delete,
            });
        }
    }

    Ok(Modifier::ForeignKey {
        model: reference.to_string(),
        field: None,
        on_delete,
    })
}

fn parse_attribute(pair: Pair<Rule>) -> Attribute {
    let mut inner = pair.into_inner();
    let name = inner.next().unwrap().as_str().to_string();
    let args = inner.next().map(|p| p.as_str().to_string());
    Attribute { name, args }
}

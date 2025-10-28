// src/parser.rs
use pest::iterators::Pair;
use crate::ast::*;
use crate::{Rule, SchemaParser};
use pest::Parser;

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
    for inner in pair.into_inner() {
        match inner.as_rule() {
            Rule::model_decl => models.push(parse_model(inner)),
            _ => (),
        }
    }
    Schema { models }
}

fn parse_model(pair: Pair<Rule>) -> Model {
    let mut pairs = pair.into_inner();
    let name = pairs.next().unwrap().as_str().to_string();
    let mut fields = Vec::new();
    for pair in pairs {
        if matches!(pair.as_rule(), Rule::field_decl) {
            fields.push(parse_field(pair));
        }
    }
    Model { name, fields }
}

fn parse_field(pair: Pair<Rule>) -> Field {
    let mut pairs = pair.into_inner();
    let name = pairs.next().unwrap().as_str().to_string();
    let type_name = pairs.next().unwrap().as_str().to_string();

    let mut modifiers = Vec::new();
    let mut attributes = Vec::new();

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

    Field { name, type_name, modifiers, attributes }
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
        "ForeignKey" => parse_foreign_key(args),
        _ => Err(format!("Unknown modifier: {}", mod_name)),
    }
}

fn parse_foreign_key(args: Option<String>) -> Result<Modifier, String> {
    let arg_str = args.ok_or("ForeignKey requires arguments")?;

    // Parse "Model.field" or "Model(field)"
    if let Some(dot_pos) = arg_str.find('.') {
        let model = arg_str[..dot_pos].trim().to_string();
        let field = arg_str[dot_pos + 1..].trim().to_string();
        return Ok(Modifier::ForeignKey {
            model,
            field: Some(field),
        });
    }

    if let Some(paren_pos) = arg_str.find('(') {
        let model = arg_str[..paren_pos].trim().to_string();
        if let Some(close_pos) = arg_str.find(')') {
            let field = arg_str[paren_pos + 1..close_pos].trim().to_string();
            return Ok(Modifier::ForeignKey {
                model,
                field: Some(field),
            });
        }
    }

    Ok(Modifier::ForeignKey {
        model: arg_str,
        field: None,
    })
}

fn parse_attribute(pair: Pair<Rule>) -> Attribute {
    let mut inner = pair.into_inner();
    let name = inner.next().unwrap().as_str().to_string();
    let args = inner.next().map(|p| {
        p.as_str().to_string()
    });
    Attribute { name, args }
}


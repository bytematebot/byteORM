use proc_macro2::TokenStream;
use quote::quote;

type Error = Box<dyn std::error::Error + Send + Sync>;

pub fn generate_jsonb_ext() -> TokenStream {
    quote! {
        pub trait JsonbExt {
            fn get_value<T>(&self, key: &str) -> Result<T, Box<dyn std::error::Error + Send + Sync>>
            where T: serde::de::DeserializeOwned;

            fn get_string(&self, key: &str) -> Result<String, Box<dyn std::error::Error + Send + Sync>>;

            fn get_i64(&self, key: &str) -> Result<i64, Box<dyn std::error::Error + Send + Sync>>;
            fn get_bool(&self, key: &str) -> Result<bool, Box<dyn std::error::Error + Send + Sync>>;

            fn get_or_default<T>(&self, key: &str, default: T) -> T
            where T: serde::de::DeserializeOwned;

            fn has_key(&self, key: &str) -> bool;
        }

        impl JsonbExt for serde_json::Value {
            fn get_value<T>(&self, key: &str) -> Result<T, Box<dyn std::error::Error + Send + Sync>>
            where T: serde::de::DeserializeOwned {
                let value = self.get(key).ok_or_else(|| {
                    Box::<dyn std::error::Error + Send + Sync>::from(format!("Key '{}' not found", key))
                })?;
                serde_json::from_value(value.clone())
                    .map_err(|e| Box::<dyn std::error::Error + Send + Sync>::from(format!("Failed to parse key '{}': {}", key, e)))
            }

            fn get_string(&self, key: &str) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
                match self.get(key) {
                    Some(serde_json::Value::String(s)) => Ok(s.clone()),
                    Some(v) => serde_json::from_value(v.clone())
                        .map_err(|e| Box::<dyn std::error::Error + Send + Sync>::from(format!("Failed to parse '{}' as string: {}", key, e))),
                    None => Err(Box::<dyn std::error::Error + Send + Sync>::from(format!("Key '{}' not found", key))),
                }
            }

            fn get_i64(&self, key: &str) -> Result<i64, Box<dyn std::error::Error + Send + Sync>> {
                match self.get(key) {
                    Some(serde_json::Value::Number(n)) => n.as_i64().ok_or_else(|| {
                        Box::<dyn std::error::Error + Send + Sync>::from(format!("Key '{}' is not a valid i64", key))
                    }),
                    Some(v) => serde_json::from_value(v.clone())
                        .map_err(|e| Box::<dyn std::error::Error + Send + Sync>::from(format!("Failed to parse '{}' as i64: {}", key, e))),
                    None => Err(Box::<dyn std::error::Error + Send + Sync>::from(format!("Key '{}' not found", key))),
                }
            }

            fn get_bool(&self, key: &str) -> Result<bool, Box<dyn std::error::Error + Send + Sync>> {
                match self.get(key) {
                    Some(serde_json::Value::Bool(b)) => Ok(*b),
                    Some(v) => serde_json::from_value(v.clone())
                        .map_err(|e| Box::<dyn std::error::Error + Send + Sync>::from(format!("Failed to parse '{}' as bool: {}", key, e))),
                    None => Err(Box::<dyn std::error::Error + Send + Sync>::from(format!("Key '{}' not found", key))),
                }
            }

            fn get_or_default<T>(&self, key: &str, default: T) -> T
            where T: serde::de::DeserializeOwned {
                self.get_value(key).unwrap_or(default)
            }

            fn has_key(&self, key: &str) -> bool {
                self.get(key).is_some()
            }
        }
    }
}

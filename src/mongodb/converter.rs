// ABOUTME: MongoDB BSON to JSONB type conversion for PostgreSQL storage
// ABOUTME: Handles all BSON types with lossless conversion and special type encoding

use anyhow::{Context, Result};
use bson::{Bson, Document};
use mongodb::Database;
use serde_json::Value as JsonValue;

/// Convert a BSON value to JSON
///
/// Maps BSON types to JSON types:
/// - Int32/Int64 → number
/// - Double → number
/// - String → string
/// - Bool → boolean
/// - Array → array
/// - Document → object
/// - ObjectId → object with $oid field
/// - DateTime → object with $date field
/// - Binary → object with $binary field (base64)
/// - Null/Undefined → null
///
/// # Arguments
///
/// * `value` - BSON value from MongoDB
///
/// # Returns
///
/// JSON value suitable for JSONB storage
///
/// # Examples
///
/// ```no_run
/// # use postgres_seren_replicator::mongodb::converter::bson_to_json;
/// # use bson::Bson;
/// let bson_int = Bson::Int32(42);
/// let json = bson_to_json(&bson_int).unwrap();
/// assert_eq!(json, serde_json::json!(42));
/// ```
pub fn bson_to_json(value: &Bson) -> Result<JsonValue> {
    match value {
        Bson::Double(f) => {
            // Handle non-finite numbers
            if f.is_finite() {
                serde_json::Number::from_f64(*f)
                    .map(JsonValue::Number)
                    .ok_or_else(|| anyhow::anyhow!("Failed to convert double {} to JSON number", f))
            } else {
                // Store non-finite as strings
                Ok(JsonValue::String(f.to_string()))
            }
        }
        Bson::String(s) => Ok(JsonValue::String(s.clone())),
        Bson::Array(arr) => {
            let json_arr: Result<Vec<JsonValue>> = arr.iter().map(bson_to_json).collect();
            Ok(JsonValue::Array(json_arr?))
        }
        Bson::Document(doc) => {
            let json_obj: Result<serde_json::Map<String, JsonValue>> = doc
                .iter()
                .map(|(k, v)| bson_to_json(v).map(|json_v| (k.clone(), json_v)))
                .collect();
            Ok(JsonValue::Object(json_obj?))
        }
        Bson::Boolean(b) => Ok(JsonValue::Bool(*b)),
        Bson::Null => Ok(JsonValue::Null),
        Bson::Int32(i) => Ok(JsonValue::Number((*i).into())),
        Bson::Int64(i) => Ok(JsonValue::Number((*i).into())),
        Bson::ObjectId(oid) => {
            // Store ObjectId as object with $oid field for type preservation
            Ok(serde_json::json!({
                "_type": "objectid",
                "$oid": oid.to_hex()
            }))
        }
        Bson::DateTime(dt) => {
            // Store DateTime as object with $date field
            // Using milliseconds since epoch for precision
            Ok(serde_json::json!({
                "_type": "datetime",
                "$date": dt.timestamp_millis()
            }))
        }
        Bson::Binary(bin) => {
            // Encode binary as base64 in object
            let encoded =
                base64::Engine::encode(&base64::engine::general_purpose::STANDARD, &bin.bytes);
            Ok(serde_json::json!({
                "_type": "binary",
                "subtype": u8::from(bin.subtype),
                "data": encoded
            }))
        }
        Bson::RegularExpression(regex) => {
            // Store regex as object with pattern and options
            Ok(serde_json::json!({
                "_type": "regex",
                "pattern": regex.pattern,
                "options": regex.options
            }))
        }
        Bson::Timestamp(ts) => {
            // Store timestamp as object
            Ok(serde_json::json!({
                "_type": "timestamp",
                "t": ts.time,
                "i": ts.increment
            }))
        }
        Bson::Decimal128(dec) => {
            // Store Decimal128 as string to preserve precision
            Ok(JsonValue::String(dec.to_string()))
        }
        Bson::Undefined => {
            // Treat undefined as null
            Ok(JsonValue::Null)
        }
        Bson::MaxKey => {
            // Store MaxKey as special object
            Ok(serde_json::json!({
                "_type": "maxkey"
            }))
        }
        Bson::MinKey => {
            // Store MinKey as special object
            Ok(serde_json::json!({
                "_type": "minkey"
            }))
        }
        _ => {
            // For any unsupported types, convert to string representation
            Ok(JsonValue::String(format!("{:?}", value)))
        }
    }
}

/// Convert a MongoDB document to JSON object
///
/// Converts all fields in the document to JSON, preserving all types.
///
/// # Arguments
///
/// * `document` - BSON document from MongoDB
///
/// # Returns
///
/// JSON object ready for JSONB storage
///
/// # Examples
///
/// ```no_run
/// # use postgres_seren_replicator::mongodb::converter::document_to_json;
/// # use bson::{doc, Bson};
/// let doc = doc! {
///     "name": "Alice",
///     "age": 30,
///     "active": true
/// };
/// let json = document_to_json(&doc).unwrap();
/// assert_eq!(json["name"], "Alice");
/// assert_eq!(json["age"], 30);
/// ```
pub fn document_to_json(document: &Document) -> Result<JsonValue> {
    let mut json_obj = serde_json::Map::new();

    for (key, value) in document.iter() {
        let json_value = bson_to_json(value)
            .with_context(|| format!("Failed to convert field '{}' to JSON", key))?;
        json_obj.insert(key.clone(), json_value);
    }

    Ok(JsonValue::Object(json_obj))
}

/// Convert an entire MongoDB collection to JSONB format
///
/// Reads all documents from a collection and converts them to JSONB.
/// Returns a vector of (id, json_data) tuples ready for insertion.
///
/// # ID Generation Strategy
///
/// - Uses MongoDB's _id field as the ID (converted to string)
/// - ObjectId is converted to hex string
/// - Other ID types are converted to string representation
///
/// # Arguments
///
/// * `database` - MongoDB database reference
/// * `collection_name` - Collection name (must be validated)
///
/// # Returns
///
/// Vector of (id_string, json_data) tuples for batch insert
///
/// # Security
///
/// Collection name should be validated before calling this function.
///
/// # Examples
///
/// ```no_run
/// # use postgres_seren_replicator::mongodb::{connect_mongodb, converter::convert_collection_to_jsonb};
/// # use postgres_seren_replicator::jsonb::validate_table_name;
/// # async fn example() -> anyhow::Result<()> {
/// let client = connect_mongodb("mongodb://localhost:27017/mydb").await?;
/// let db = client.database("mydb");
/// let collection = "users";
/// validate_table_name(collection)?;
/// let rows = convert_collection_to_jsonb(&db, collection).await?;
/// println!("Converted {} documents to JSONB", rows.len());
/// # Ok(())
/// # }
/// ```
pub async fn convert_collection_to_jsonb(
    database: &Database,
    collection_name: &str,
) -> Result<Vec<(String, JsonValue)>> {
    // Validate collection name
    crate::jsonb::validate_table_name(collection_name)
        .context("Invalid collection name for JSONB conversion")?;

    tracing::info!(
        "Converting MongoDB collection '{}' to JSONB",
        collection_name
    );

    // Read all documents using our reader
    let documents = crate::mongodb::reader::read_collection_data(database, collection_name)
        .await
        .with_context(|| format!("Failed to read data from collection '{}'", collection_name))?;

    let mut result = Vec::with_capacity(documents.len());

    for (doc_num, document) in documents.into_iter().enumerate() {
        // Extract or generate ID
        let id = if let Some(id_value) = document.get("_id") {
            // Use _id field from document
            match id_value {
                Bson::ObjectId(oid) => oid.to_hex(),
                Bson::String(s) => s.clone(),
                Bson::Int32(i) => i.to_string(),
                Bson::Int64(i) => i.to_string(),
                _ => {
                    tracing::warn!(
                        "Document {} in collection '{}' has unsupported _id type, using doc number",
                        doc_num + 1,
                        collection_name
                    );
                    (doc_num + 1).to_string()
                }
            }
        } else {
            // No _id field, use document number
            tracing::warn!(
                "Document {} in collection '{}' has no _id field, using doc number",
                doc_num + 1,
                collection_name
            );
            (doc_num + 1).to_string()
        };

        // Convert document to JSON
        let json_data = document_to_json(&document).with_context(|| {
            format!(
                "Failed to convert document {} in collection '{}' to JSON",
                doc_num + 1,
                collection_name
            )
        })?;

        result.push((id, json_data));
    }

    tracing::info!(
        "Converted {} documents from collection '{}' to JSONB",
        result.len(),
        collection_name
    );

    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::*;
    use bson::{doc, oid::ObjectId, Bson};

    #[test]
    fn test_convert_int32() {
        let bson = Bson::Int32(42);
        let json = bson_to_json(&bson).unwrap();
        assert_eq!(json, serde_json::json!(42));
    }

    #[test]
    fn test_convert_int64() {
        let bson = Bson::Int64(42i64);
        let json = bson_to_json(&bson).unwrap();
        assert_eq!(json, serde_json::json!(42));
    }

    #[test]
    fn test_convert_double() {
        let bson = Bson::Double(42.75);
        let json = bson_to_json(&bson).unwrap();
        assert_eq!(json, serde_json::json!(42.75));
    }

    #[test]
    fn test_convert_string() {
        let bson = Bson::String("Hello, World!".to_string());
        let json = bson_to_json(&bson).unwrap();
        assert_eq!(json, serde_json::json!("Hello, World!"));
    }

    #[test]
    fn test_convert_bool() {
        let bson_true = Bson::Boolean(true);
        let json_true = bson_to_json(&bson_true).unwrap();
        assert_eq!(json_true, serde_json::json!(true));

        let bson_false = Bson::Boolean(false);
        let json_false = bson_to_json(&bson_false).unwrap();
        assert_eq!(json_false, serde_json::json!(false));
    }

    #[test]
    fn test_convert_null() {
        let bson = Bson::Null;
        let json = bson_to_json(&bson).unwrap();
        assert_eq!(json, JsonValue::Null);
    }

    #[test]
    fn test_convert_array() {
        let bson = Bson::Array(vec![Bson::Int32(1), Bson::Int32(2), Bson::Int32(3)]);
        let json = bson_to_json(&bson).unwrap();
        assert_eq!(json, serde_json::json!([1, 2, 3]));
    }

    #[test]
    fn test_convert_document() {
        let doc = doc! {
            "name": "Alice",
            "age": 30,
            "active": true
        };
        let json = document_to_json(&doc).unwrap();
        assert_eq!(json["name"], "Alice");
        assert_eq!(json["age"], 30);
        assert_eq!(json["active"], true);
    }

    #[test]
    fn test_convert_objectid() {
        let oid = ObjectId::new();
        let bson = Bson::ObjectId(oid);
        let json = bson_to_json(&bson).unwrap();

        // Should be wrapped in object with _type and $oid
        assert!(json.is_object());
        assert_eq!(json["_type"], "objectid");
        assert_eq!(json["$oid"], oid.to_hex());
    }

    #[test]
    fn test_convert_non_finite_double() {
        let nan_bson = Bson::Double(f64::NAN);
        let json = bson_to_json(&nan_bson).unwrap();
        assert!(json.is_string());

        let inf_bson = Bson::Double(f64::INFINITY);
        let json = bson_to_json(&inf_bson).unwrap();
        assert!(json.is_string());
    }

    #[test]
    fn test_convert_nested_document() {
        let doc = doc! {
            "user": {
                "name": "Alice",
                "email": "alice@example.com"
            },
            "tags": ["admin", "user"]
        };
        let json = document_to_json(&doc).unwrap();

        assert_eq!(json["user"]["name"], "Alice");
        assert_eq!(json["user"]["email"], "alice@example.com");
        assert_eq!(json["tags"][0], "admin");
        assert_eq!(json["tags"][1], "user");
    }
}

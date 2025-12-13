use serde_json::Value;

use crate::queue::{ChangeOperation, NewChange};

#[derive(Debug, Clone, PartialEq)]
pub struct RowChange {
    pub table_name: String,
    pub operation: ChangeOperation,
    pub primary_key: String,
    pub payload: Option<Value>,
    pub wal_frame: Option<String>,
    pub cursor: Option<String>,
}

impl RowChange {
    pub fn into_new_change(self) -> NewChange {
        let payload = self
            .payload
            .map(|value| serde_json::to_vec(&value).expect("row change payload serializes"));
        NewChange {
            table_name: self.table_name,
            operation: self.operation,
            primary_key: self.primary_key,
            payload,
            wal_frame: self.wal_frame,
            cursor: self.cursor,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn converts_to_new_change() {
        let row = RowChange {
            table_name: "prices".into(),
            operation: ChangeOperation::Update,
            primary_key: "pk1".into(),
            payload: Some(serde_json::json!({"foo": "bar"})),
            wal_frame: Some("frame-1".into()),
            cursor: Some("cursor".into()),
        };
        let change = row.into_new_change();
        assert_eq!(change.table_name, "prices");
        assert!(change.payload.unwrap().contains(&b'b'));
    }
}

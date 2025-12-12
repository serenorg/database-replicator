use crate::change::RowChange;
use crate::queue::ChangeOperation;
use crate::wal::WalEvent;
use serde_json::json;
use std::time::{SystemTime, UNIX_EPOCH};

/// Temporary decoder that turns WAL growth bytes into placeholder RowChange events.
/// Placeholder until row-level decoding is implemented.
#[derive(Debug, Default, Clone)]
pub struct WalGrowthDecoder;

impl WalGrowthDecoder {
    pub fn decode(&self, event: &WalEvent) -> Vec<RowChange> {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock should be >= UNIX epoch");
        vec![RowChange {
            table_name: "__wal__".to_string(),
            operation: ChangeOperation::Insert,
            primary_key: now.as_nanos().to_string(),
            payload: Some(json!({
                "kind": "wal_growth",
                "bytes_added": event.bytes_added,
                "current_size": event.current_size,
                "recorded_at": now.as_secs_f64(),
            })),
            wal_frame: None,
            cursor: None,
        }]
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn produces_placeholder_row_change() {
        let decoder = WalGrowthDecoder::default();
        let rows = decoder.decode(&WalEvent {
            bytes_added: 1024,
            current_size: 2048,
        });
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].table_name, "__wal__");
        assert_eq!(rows[0].operation, ChangeOperation::Insert);
    }
}

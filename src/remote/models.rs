// ABOUTME: Data structures for remote job specifications and responses
// ABOUTME: These are serialized to JSON for API communication

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JobSpec {
    #[serde(rename = "schema_version")]
    pub version: String,
    pub command: String, // "init" or "sync"
    pub source_url: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub target_url: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub target_project_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub target_branch_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub target_databases: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub seren_api_key: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub filter: Option<FilterSpec>,
    pub options: HashMap<String, serde_json::Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FilterSpec {
    pub include_databases: Option<Vec<String>>,
    pub exclude_databases: Option<Vec<String>>,
    pub include_tables: Option<Vec<String>>,
    pub exclude_tables: Option<Vec<String>>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct JobResponse {
    pub job_id: String,
    pub status: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct JobStatus {
    pub job_id: String,
    pub status: String, // "provisioning", "running", "completed", "failed"
    pub created_at: Option<String>,
    pub started_at: Option<String>,
    pub completed_at: Option<String>,
    pub progress: Option<ProgressInfo>,
    pub error: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ProgressInfo {
    pub current_database: Option<String>,
    pub databases_completed: usize,
    pub databases_total: usize,
    pub message: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_job_spec_serialization() {
        let mut options = HashMap::new();
        options.insert("drop_existing".to_string(), serde_json::Value::Bool(true));

        // Test with all fields populated
        let job_spec = JobSpec {
            version: "1.0".to_string(),
            command: "init".to_string(),
            source_url: "postgresql://source".to_string(),
            target_url: Some("postgresql://target".to_string()),
            target_project_id: Some("proj123".to_string()),
            target_branch_id: Some("brnch456".to_string()),
            target_databases: Some(vec!["db1".to_string()]),
            seren_api_key: Some("seren_key".to_string()),
            filter: Some(FilterSpec {
                include_databases: Some(vec!["db1".to_string()]),
                exclude_databases: None,
                include_tables: None,
                exclude_tables: None,
            }),
            options: options.clone(),
        };

        let parsed: serde_json::Value = serde_json::to_value(&job_spec).unwrap();
        assert_eq!(parsed["target_url"], "postgresql://target");
        assert_eq!(parsed["target_project_id"], "proj123");
        assert_eq!(parsed["target_branch_id"], "brnch456");
        assert_eq!(parsed["target_databases"], serde_json::json!(["db1"]));
        assert_eq!(parsed["seren_api_key"], "seren_key");
        assert_eq!(
            parsed["filter"],
            serde_json::json!({"include_databases": ["db1"], "exclude_databases": null, "include_tables": null, "exclude_tables": null})
        );
        assert_eq!(parsed["schema_version"], "1.0");

        // Test with optional fields as None
        let job_spec_none = JobSpec {
            version: "1.0".to_string(),
            command: "init".to_string(),
            source_url: "postgresql://source".to_string(),
            target_url: Some("postgresql://target".to_string()),
            target_project_id: None,
            target_branch_id: None,
            target_databases: None,
            seren_api_key: None,
            filter: None,
            options,
        };

        let parsed_none: serde_json::Value = serde_json::to_value(&job_spec_none).unwrap();
        assert_eq!(parsed_none["target_url"], "postgresql://target");
        assert!(parsed_none.get("target_project_id").is_none());
        assert!(parsed_none.get("target_branch_id").is_none());
        assert!(parsed_none.get("target_databases").is_none());
        assert!(parsed_none.get("seren_api_key").is_none());
        assert!(parsed_none.get("filter").is_none());
    }
}

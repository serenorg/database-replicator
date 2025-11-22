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
    pub target_url: String,
    pub filter: Option<FilterSpec>,
    pub options: HashMap<String, serde_json::Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FilterSpec {
    pub include_databases: Option<Vec<String>>,
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
}

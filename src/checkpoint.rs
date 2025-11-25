// ABOUTME: Persistent checkpoint tracking for long-running operations
// ABOUTME: Provides init command resume support with hashed identities

use anyhow::{bail, Context, Result};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::BTreeSet;
use std::fs;
use std::path::{Path, PathBuf};

const INIT_CHECKPOINT_VERSION: u32 = 1;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct InitCheckpointMetadata {
    pub source_hash: String,
    pub target_hash: String,
    pub filter_hash: String,
    pub drop_existing: bool,
    pub enable_sync: bool,
}

impl InitCheckpointMetadata {
    pub fn new(
        source_url: &str,
        target_url: &str,
        filter_hash: String,
        drop_existing: bool,
        enable_sync: bool,
    ) -> Self {
        Self {
            source_hash: hash_string(source_url),
            target_hash: hash_string(target_url),
            filter_hash,
            drop_existing,
            enable_sync,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct InitCheckpointData {
    version: u32,
    metadata: InitCheckpointMetadata,
    databases: Vec<String>,
    completed: BTreeSet<String>,
}

#[derive(Debug, Clone)]
pub struct InitCheckpoint {
    data: InitCheckpointData,
}

impl InitCheckpoint {
    pub fn new(metadata: InitCheckpointMetadata, databases: &[String]) -> Self {
        Self {
            data: InitCheckpointData {
                version: INIT_CHECKPOINT_VERSION,
                metadata,
                databases: databases.to_vec(),
                completed: BTreeSet::new(),
            },
        }
    }

    pub fn load(path: &Path) -> Result<Option<Self>> {
        if !path.exists() {
            return Ok(None);
        }

        let content = fs::read_to_string(path)
            .with_context(|| format!("Failed to read checkpoint at {}", path.display()))?;
        let data: InitCheckpointData = serde_json::from_str(&content)
            .with_context(|| format!("Failed to parse checkpoint JSON at {}", path.display()))?;

        if data.version != INIT_CHECKPOINT_VERSION {
            bail!(
                "Checkpoint version mismatch (found {}, expected {}). Run with --no-resume to start fresh.",
                data.version,
                INIT_CHECKPOINT_VERSION
            );
        }

        Ok(Some(Self { data }))
    }

    pub fn save(&self, path: &Path) -> Result<()> {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).with_context(|| {
                format!("Failed to create checkpoint directory {}", parent.display())
            })?;
        }

        let parent = path.parent().unwrap_or_else(|| Path::new("."));
        let mut tmp = tempfile::NamedTempFile::new_in(parent)
            .with_context(|| format!("Failed to create temp checkpoint in {}", parent.display()))?;

        serde_json::to_writer_pretty(tmp.as_file_mut(), &self.data)
            .with_context(|| format!("Failed to serialize checkpoint at {}", path.display()))?;

        tmp.persist(path)
            .with_context(|| format!("Failed to persist checkpoint at {}", path.display()))?;

        Ok(())
    }

    pub fn databases(&self) -> &[String] {
        &self.data.databases
    }

    pub fn metadata(&self) -> &InitCheckpointMetadata {
        &self.data.metadata
    }

    pub fn mark_completed(&mut self, db_name: &str) -> bool {
        self.data.completed.insert(db_name.to_string())
    }

    pub fn is_completed(&self, db_name: &str) -> bool {
        self.data.completed.contains(db_name)
    }

    pub fn completed_count(&self) -> usize {
        self.data.completed.len()
    }

    pub fn total_databases(&self) -> usize {
        self.data.databases.len()
    }

    pub fn validate(&self, metadata: &InitCheckpointMetadata, databases: &[String]) -> Result<()> {
        if self.data.metadata != *metadata {
            bail!(
                "Checkpoint metadata mismatch. Run with --no-resume to discard the previous state."
            );
        }

        if self.data.databases != databases {
            bail!(
                "Checkpoint database list differs from current discovery. Run with --no-resume to start fresh."
            );
        }

        Ok(())
    }
}

pub fn checkpoint_path(source_url: &str, target_url: &str) -> Result<PathBuf> {
    let base = std::env::temp_dir().join("postgres-seren-replicator-checkpoints");
    fs::create_dir_all(&base).with_context(|| {
        format!(
            "Failed to create checkpoint base directory {}",
            base.display()
        )
    })?;

    let mut hasher = Sha256::new();
    hasher.update(source_url.as_bytes());
    hasher.update(b"::");
    hasher.update(target_url.as_bytes());
    let digest = format!("{:x}", hasher.finalize());
    let short = &digest[..16.min(digest.len())];

    Ok(base.join(format!("init-{}.json", short)))
}

pub fn remove_checkpoint(path: &Path) -> Result<()> {
    if path.exists() {
        fs::remove_file(path)
            .with_context(|| format!("Failed to remove checkpoint at {}", path.display()))?;
    }
    Ok(())
}

fn hash_string(input: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(input.as_bytes());
    format!("{:x}", hasher.finalize())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn metadata_hash_changes_with_inputs() {
        let meta_a = InitCheckpointMetadata::new("src_a", "tgt", "filter".into(), true, false);
        let meta_b = InitCheckpointMetadata::new("src_b", "tgt", "filter".into(), true, false);
        assert_ne!(meta_a.source_hash, meta_b.source_hash);
    }

    #[test]
    fn checkpoint_roundtrip() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("cp.json");
        let metadata = InitCheckpointMetadata::new("src", "tgt", "filter".into(), false, true);
        let databases = vec!["db1".to_string(), "db2".to_string()];
        let mut checkpoint = InitCheckpoint::new(metadata.clone(), &databases);
        checkpoint.mark_completed("db1");
        checkpoint.save(&path).unwrap();

        let loaded = InitCheckpoint::load(&path).unwrap().unwrap();
        loaded.validate(&metadata, &databases).unwrap();
        assert!(loaded.is_completed("db1"));
        assert!(!loaded.is_completed("db2"));
    }

    #[test]
    fn checkpoint_path_is_deterministic() {
        let path_a = checkpoint_path("postgres://src/db", "postgres://tgt/db").unwrap();
        let path_b = checkpoint_path("postgres://src/db", "postgres://tgt/db").unwrap();
        assert_eq!(path_a, path_b);
    }
}

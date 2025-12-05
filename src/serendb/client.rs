// ABOUTME: HTTP client for SerenDB Console API
// ABOUTME: Manages project settings like logical replication

use anyhow::{Context, Result};
use reqwest::Client;
use serde::{Deserialize, Serialize};

/// Default SerenDB Console API base URL
pub const DEFAULT_CONSOLE_API_URL: &str = "https://console.serendb.com";

/// SerenDB Console API client
pub struct ConsoleClient {
    client: Client,
    api_base_url: String,
    api_key: String,
}

/// Project information from SerenDB Console API
#[derive(Debug, Deserialize)]
pub struct Project {
    pub id: String,
    pub name: String,
    pub enable_logical_replication: bool,
    #[serde(default)]
    pub organization_id: Option<String>,
}

/// Wrapper for API responses
#[derive(Debug, Deserialize)]
pub struct DataResponse<T> {
    pub data: T,
}

/// Request to update project settings
#[derive(Debug, Serialize)]
pub struct UpdateProjectRequest {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub enable_logical_replication: Option<bool>,
}

impl ConsoleClient {
    /// Create a new Console API client
    ///
    /// # Arguments
    ///
    /// * `api_base_url` - Optional base URL (defaults to https://console.serendb.com)
    /// * `api_key` - SerenDB API key (format: seren_<key_id>_<secret>)
    pub fn new(api_base_url: Option<&str>, api_key: String) -> Self {
        Self {
            client: Client::new(),
            api_base_url: api_base_url
                .unwrap_or(DEFAULT_CONSOLE_API_URL)
                .trim_end_matches('/')
                .to_string(),
            api_key,
        }
    }

    /// Get project information by ID
    ///
    /// # Arguments
    ///
    /// * `project_id` - UUID string of the project
    ///
    /// # Returns
    ///
    /// Project information including logical replication status
    pub async fn get_project(&self, project_id: &str) -> Result<Project> {
        let url = format!("{}/api/projects/{}", self.api_base_url, project_id);

        let response = self
            .client
            .get(&url)
            .header("Authorization", format!("Bearer {}", self.api_key))
            .header("Content-Type", "application/json")
            .send()
            .await
            .context("Failed to send request to SerenDB Console API")?;

        if response.status() == reqwest::StatusCode::UNAUTHORIZED {
            anyhow::bail!(
                "SerenDB API key is invalid or expired.\n\
                 Generate a new key at: https://console.serendb.com/api-keys"
            );
        }

        if response.status() == reqwest::StatusCode::NOT_FOUND {
            anyhow::bail!(
                "Project {} not found.\n\
                 Verify the project ID is correct and you have access to it.",
                project_id
            );
        }

        if !response.status().is_success() {
            let status = response.status();
            let body = response.text().await.unwrap_or_default();
            anyhow::bail!("SerenDB Console API returned error {}: {}", status, body);
        }

        let data: DataResponse<Project> = response
            .json()
            .await
            .context("Failed to parse project response from SerenDB Console API")?;

        Ok(data.data)
    }

    /// Enable logical replication for a project
    ///
    /// **Warning**: This action cannot be undone. Once enabled, logical replication
    /// cannot be disabled. Enabling will briefly suspend all active endpoints.
    ///
    /// # Arguments
    ///
    /// * `project_id` - UUID string of the project
    ///
    /// # Returns
    ///
    /// Updated project information
    pub async fn enable_logical_replication(&self, project_id: &str) -> Result<Project> {
        let url = format!("{}/api/projects/{}", self.api_base_url, project_id);

        let request = UpdateProjectRequest {
            enable_logical_replication: Some(true),
        };

        let response = self
            .client
            .patch(&url)
            .header("Authorization", format!("Bearer {}", self.api_key))
            .header("Content-Type", "application/json")
            .json(&request)
            .send()
            .await
            .context("Failed to send request to SerenDB Console API")?;

        if response.status() == reqwest::StatusCode::UNAUTHORIZED {
            anyhow::bail!(
                "SerenDB API key is invalid or expired.\n\
                 Generate a new key at: https://console.serendb.com/api-keys"
            );
        }

        if response.status() == reqwest::StatusCode::NOT_FOUND {
            anyhow::bail!(
                "Project {} not found.\n\
                 Verify the project ID is correct and you have access to it.",
                project_id
            );
        }

        if !response.status().is_success() {
            let status = response.status();
            let body = response.text().await.unwrap_or_default();
            anyhow::bail!(
                "Failed to enable logical replication. SerenDB Console API returned {}: {}",
                status,
                body
            );
        }

        let data: DataResponse<Project> = response
            .json()
            .await
            .context("Failed to parse project response from SerenDB Console API")?;

        Ok(data.data)
    }

    /// Check if logical replication is enabled for a project
    ///
    /// # Arguments
    ///
    /// * `project_id` - UUID string of the project
    ///
    /// # Returns
    ///
    /// true if logical replication is enabled, false otherwise
    pub async fn is_logical_replication_enabled(&self, project_id: &str) -> Result<bool> {
        let project = self.get_project(project_id).await?;
        Ok(project.enable_logical_replication)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_client_creation() {
        let client = ConsoleClient::new(None, "seren_test_key".to_string());
        assert_eq!(client.api_base_url, DEFAULT_CONSOLE_API_URL);
    }

    #[test]
    fn test_client_custom_url() {
        let client = ConsoleClient::new(
            Some("https://custom.serendb.com/"),
            "seren_test_key".to_string(),
        );
        assert_eq!(client.api_base_url, "https://custom.serendb.com");
    }

    #[test]
    fn test_update_request_serialization() {
        let request = UpdateProjectRequest {
            enable_logical_replication: Some(true),
        };
        let json = serde_json::to_string(&request).unwrap();
        assert!(json.contains("enable_logical_replication"));
        assert!(json.contains("true"));
    }
}

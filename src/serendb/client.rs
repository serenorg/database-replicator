// ABOUTME: HTTP client for SerenDB Console API
// ABOUTME: Manages project settings like logical replication

use anyhow::{Context, Result};
use reqwest::Client;
use serde::{Deserialize, Serialize};

use crate::utils::replace_database_in_connection_string;

/// Default SerenDB Console API base URL
pub const DEFAULT_CONSOLE_API_URL: &str = "https://api.serendb.com";

/// SerenDB Console API client
pub struct ConsoleClient {
    client: Client,
    api_base_url: String,
    api_key: String,
}

/// Project information from SerenDB Console API
#[derive(Debug, Clone, Deserialize)]
pub struct Project {
    pub id: String,
    pub name: String,
    pub enable_logical_replication: bool,
    #[serde(default)]
    pub organization_id: Option<String>,
}

/// Branch information from SerenDB Console API
#[allow(dead_code)]
#[derive(Debug, Clone, Deserialize)]
pub struct Branch {
    pub id: String,
    pub name: String,
    pub project_id: String,
    #[serde(default)]
    pub is_default: bool,
}

/// Database information from SerenDB Console API
#[allow(dead_code)]
#[derive(Debug, Clone, Deserialize)]
pub struct Database {
    pub id: String,
    pub name: String,
    pub branch_id: String,
}

/// Connection string response payload
#[allow(dead_code)]
#[derive(Debug, Deserialize)]
pub struct ConnectionStringResponse {
    pub connection_string: String,
}

/// Request payload to create a database
#[allow(dead_code)]
#[derive(Debug, Serialize)]
pub struct CreateDatabaseRequest {
    pub name: String,
}

/// Paginated response wrapper from the Console API
#[allow(dead_code)]
#[derive(Debug, Deserialize)]
pub struct PaginatedResponse<T> {
    pub data: Vec<T>,
    #[serde(default)]
    pub pagination: Option<Pagination>,
}

/// Pagination metadata returned by the Console API
#[allow(dead_code)]
#[derive(Debug, Deserialize, Default)]
pub struct Pagination {
    #[serde(default)]
    pub total: i64,
    #[serde(default)]
    pub page: i64,
    #[serde(default)]
    pub per_page: i64,
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
    /// * `api_base_url` - Optional base URL (defaults to https://api.serendb.com)
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

    /// List all projects accessible to the authenticated user
    ///
    /// # Returns
    ///
    /// Vector of projects the user has access to
    ///
    /// # Examples
    /// ```ignore
    /// let client = ConsoleClient::new(None, "seren_key".to_string());
    /// let projects = client.list_projects().await?;
    /// for project in projects {
    ///     println!("{}: {}", project.id, project.name);
    /// }
    /// ```
    pub async fn list_projects(&self) -> Result<Vec<Project>> {
        let url = format!("{}/api/projects", self.api_base_url);

        let response = self
            .client
            .get(&url)
            .header("Authorization", format!("Bearer {}", self.api_key))
            .header("Content-Type", "application/json")
            .send()
            .await
            .context("Failed to send request to SerenDB Console API")?;

        self.handle_common_errors(&response).await?;

        if !response.status().is_success() {
            let status = response.status();
            let body = response.text().await.unwrap_or_default();
            anyhow::bail!("SerenDB Console API returned error {}: {}", status, body);
        }

        let data: PaginatedResponse<Project> = response
            .json()
            .await
            .context("Failed to parse projects response from SerenDB Console API")?;

        Ok(data.data)
    }

    /// List all branches for a project
    pub async fn list_branches(&self, project_id: &str) -> Result<Vec<Branch>> {
        let url = format!("{}/api/projects/{}/branches", self.api_base_url, project_id);

        let response = self
            .client
            .get(&url)
            .header("Authorization", format!("Bearer {}", self.api_key))
            .header("Content-Type", "application/json")
            .send()
            .await
            .context("Failed to send request to SerenDB Console API")?;

        self.handle_common_errors(&response).await?;

        if !response.status().is_success() {
            let status = response.status();
            let body = response.text().await.unwrap_or_default();
            anyhow::bail!("SerenDB Console API returned error {}: {}", status, body);
        }

        let data: PaginatedResponse<Branch> = response
            .json()
            .await
            .context("Failed to parse branches response from SerenDB Console API")?;

        Ok(data.data)
    }

    /// Get the default branch for a project
    ///
    /// Returns the branch marked as default, or the first branch if none are marked.
    pub async fn get_default_branch(&self, project_id: &str) -> Result<Branch> {
        let branches = self.list_branches(project_id).await?;
        select_default_branch(project_id, branches)
    }

    /// List all databases within a SerenDB branch
    pub async fn list_databases(&self, project_id: &str, branch_id: &str) -> Result<Vec<Database>> {
        let url = format!(
            "{}/api/projects/{}/branches/{}/databases",
            self.api_base_url, project_id, branch_id
        );

        let response = self
            .client
            .get(&url)
            .header("Authorization", format!("Bearer {}", self.api_key))
            .header("Content-Type", "application/json")
            .send()
            .await
            .context("Failed to send request to SerenDB Console API")?;

        self.handle_common_errors(&response).await?;

        if !response.status().is_success() {
            let status = response.status();
            let body = response.text().await.unwrap_or_default();
            anyhow::bail!("SerenDB Console API returned error {}: {}", status, body);
        }

        let data: PaginatedResponse<Database> = response
            .json()
            .await
            .context("Failed to parse databases response from SerenDB Console API")?;

        Ok(data.data)
    }

    /// Create a new SerenDB database inside a branch
    pub async fn create_database(
        &self,
        project_id: &str,
        branch_id: &str,
        name: &str,
    ) -> Result<Database> {
        let url = format!(
            "{}/api/projects/{}/branches/{}/databases",
            self.api_base_url, project_id, branch_id
        );

        let request = CreateDatabaseRequest {
            name: name.to_string(),
        };

        let response = self
            .client
            .post(&url)
            .header("Authorization", format!("Bearer {}", self.api_key))
            .header("Content-Type", "application/json")
            .json(&request)
            .send()
            .await
            .context("Failed to send request to SerenDB Console API")?;

        self.handle_common_errors(&response).await?;

        if !response.status().is_success() {
            let status = response.status();
            let body = response.text().await.unwrap_or_default();
            anyhow::bail!(
                "Failed to create database '{}': {} - {}",
                name,
                status,
                body
            );
        }

        let data: DataResponse<Database> = response
            .json()
            .await
            .context("Failed to parse create database response from SerenDB Console API")?;

        Ok(data.data)
    }

    /// Get a connection string for a branch/database combination
    pub async fn get_connection_string(
        &self,
        project_id: &str,
        branch_id: &str,
        database: &str,
        pooled: bool,
    ) -> Result<String> {
        let url = format!(
            "{}/api/projects/{}/branches/{}/connection-string?pooled={}",
            self.api_base_url, project_id, branch_id, pooled
        );

        let response = self
            .client
            .get(&url)
            .header("Authorization", format!("Bearer {}", self.api_key))
            .header("Content-Type", "application/json")
            .send()
            .await
            .context("Failed to send request to SerenDB Console API")?;

        self.handle_common_errors(&response).await?;

        if !response.status().is_success() {
            let status = response.status();
            let body = response.text().await.unwrap_or_default();
            anyhow::bail!("SerenDB Console API returned error {}: {}", status, body);
        }

        let data: ConnectionStringResponse = response
            .json()
            .await
            .context("Failed to parse connection string response from SerenDB Console API")?;

        replace_database_in_connection_string(&data.connection_string, database)
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

        self.handle_common_errors_with_context(
            &response,
            Some(format!(
                "Project {} not found.\n\
                 Verify the project ID is correct and you have access to it.",
                project_id
            )),
        )
        .await?;

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

        self.handle_common_errors_with_context(
            &response,
            Some(format!(
                "Project {} not found.\n\
                 Verify the project ID is correct and you have access to it.",
                project_id
            )),
        )
        .await?;

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

    async fn handle_common_errors(&self, response: &reqwest::Response) -> Result<()> {
        self.handle_common_errors_with_context(response, None).await
    }

    async fn handle_common_errors_with_context(
        &self,
        response: &reqwest::Response,
        not_found_message: Option<String>,
    ) -> Result<()> {
        if response.status() == reqwest::StatusCode::UNAUTHORIZED {
            anyhow::bail!(
                "SerenDB API key is invalid or expired.\n\
                 Generate a new key at: https://console.serendb.com/api-keys"
            );
        }

        if response.status() == reqwest::StatusCode::NOT_FOUND {
            if let Some(message) = not_found_message {
                anyhow::bail!(message);
            } else {
                anyhow::bail!("Resource not found. Verify the ID is correct and you have access.");
            }
        }

        Ok(())
    }
}

fn select_default_branch(project_id: &str, branches: Vec<Branch>) -> Result<Branch> {
    if branches.is_empty() {
        anyhow::bail!("Project {} has no branches", project_id);
    }

    if let Some(default_branch) = branches.iter().find(|branch| branch.is_default) {
        return Ok(default_branch.clone());
    }

    Ok(branches.into_iter().next().expect("branches is not empty"))
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

    #[test]
    fn test_branch_deserialization() {
        let json = r#"{"id": "abc", "name": "main", "project_id": "xyz", "is_default": true}"#;
        let branch: Branch = serde_json::from_str(json).unwrap();
        assert_eq!(branch.name, "main");
        assert!(branch.is_default);
    }

    #[test]
    fn test_database_deserialization() {
        let json = r#"{"id": "db1", "name": "myapp", "branch_id": "br1"}"#;
        let db: Database = serde_json::from_str(json).unwrap();
        assert_eq!(db.name, "myapp");
        assert_eq!(db.branch_id, "br1");
    }

    #[test]
    fn test_select_default_branch_prefers_flagged_branch() {
        let branches = vec![
            Branch {
                id: "br1".into(),
                name: "preview".into(),
                project_id: "proj".into(),
                is_default: false,
            },
            Branch {
                id: "br2".into(),
                name: "main".into(),
                project_id: "proj".into(),
                is_default: true,
            },
        ];

        let default = select_default_branch("proj", branches).unwrap();
        assert_eq!(default.id, "br2");
        assert_eq!(default.name, "main");
    }

    #[test]
    fn test_select_default_branch_falls_back_to_first() {
        let branches = vec![
            Branch {
                id: "br1".into(),
                name: "alpha".into(),
                project_id: "proj".into(),
                is_default: false,
            },
            Branch {
                id: "br2".into(),
                name: "beta".into(),
                project_id: "proj".into(),
                is_default: false,
            },
        ];

        let default = select_default_branch("proj", branches).unwrap();
        assert_eq!(default.id, "br1");
        assert_eq!(default.name, "alpha");
    }

    #[test]
    fn test_select_default_branch_errors_when_empty() {
        let err = select_default_branch("proj", Vec::new()).unwrap_err();
        assert!(format!("{err}").contains("has no branches"));
    }

    #[test]
    fn test_replace_database_in_connection_string() {
        let original =
            "postgresql://user:pass@host.serendb.com:5432/serendb?sslmode=require&foo=bar";
        let updated =
            replace_database_in_connection_string(original, "myapp").expect("replace succeeds");
        assert!(updated.contains("/myapp?"));
        assert!(updated.starts_with("postgresql://user:pass@host.serendb.com:5432/"));
        assert!(updated.ends_with("sslmode=require&foo=bar"));
    }
}

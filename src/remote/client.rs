// ABOUTME: HTTP client for communicating with remote execution API
// ABOUTME: Handles job submission, status polling, and error handling

use anyhow::{Context, Result};
use reqwest::Client;
use std::time::Duration;

use super::models::{JobResponse, JobSpec, JobStatus};

pub struct RemoteClient {
    client: Client,
    api_base_url: String,
}

impl RemoteClient {
    pub fn new(api_base_url: String) -> Result<Self> {
        let client = Client::builder()
            .timeout(Duration::from_secs(30))
            .build()
            .context("Failed to create HTTP client")?;

        Ok(Self {
            client,
            api_base_url,
        })
    }

    pub async fn submit_job(&self, spec: &JobSpec) -> Result<JobResponse> {
        let url = format!("{}/jobs", self.api_base_url);

        let response = self
            .client
            .post(&url)
            .json(spec)
            .send()
            .await
            .context("Failed to submit job to remote service. If the service is unavailable, you can use --local to run replication on your machine instead")?;

        if !response.status().is_success() {
            let status = response.status();
            let body = response.text().await.unwrap_or_default();
            anyhow::bail!("Job submission failed with status {}: {}. If the remote service is unavailable, you can use --local to run replication on your machine instead", status, body);
        }

        let job_response: JobResponse = response
            .json()
            .await
            .context("Failed to parse job response")?;

        Ok(job_response)
    }

    pub async fn get_job_status(&self, job_id: &str) -> Result<JobStatus> {
        let url = format!("{}/jobs/{}", self.api_base_url, job_id);

        let response = self.client.get(&url).send().await.context(
            "Failed to get job status from remote service. The remote service may be unavailable",
        )?;

        if !response.status().is_success() {
            let status = response.status();
            let body = response.text().await.unwrap_or_default();
            anyhow::bail!(
                "Failed to get job status {}: {}. The remote service may be experiencing issues",
                status,
                body
            );
        }

        let job_status: JobStatus = response
            .json()
            .await
            .context("Failed to parse job status")?;

        Ok(job_status)
    }

    pub async fn poll_until_complete(
        &self,
        job_id: &str,
        callback: impl Fn(&JobStatus),
    ) -> Result<JobStatus> {
        loop {
            let status = self.get_job_status(job_id).await?;
            callback(&status);

            match status.status.as_str() {
                "completed" | "failed" => return Ok(status),
                _ => {
                    tokio::time::sleep(Duration::from_secs(5)).await;
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_client_creation() {
        let client = RemoteClient::new("https://api.example.com".to_string());
        assert!(client.is_ok());
    }
}

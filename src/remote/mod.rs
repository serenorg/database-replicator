// ABOUTME: Remote execution module for running replication jobs on AWS
// ABOUTME: Handles job submission, status polling, and log retrieval

pub mod client;
pub mod models;

pub use client::RemoteClient;
pub use models::{FilterSpec, JobResponse, JobSpec, JobStatus};

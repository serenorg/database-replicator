// ABOUTME: Pre-flight validation checks for replication prerequisites
// ABOUTME: Validates local environment, network connectivity, and database permissions

use anyhow::Result;

/// Individual check result
#[derive(Debug, Clone)]
pub struct CheckResult {
    pub name: String,
    pub passed: bool,
    pub message: String,
    pub details: Option<String>,
}

impl CheckResult {
    pub fn pass(name: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            passed: true,
            message: message.into(),
            details: None,
        }
    }

    pub fn fail(name: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            passed: false,
            message: message.into(),
            details: None,
        }
    }

    pub fn with_details(mut self, details: impl Into<String>) -> Self {
        self.details = Some(details.into());
        self
    }
}

/// Issue with suggested fixes
#[derive(Debug, Clone)]
pub struct PreflightIssue {
    pub title: String,
    pub explanation: String,
    pub fixes: Vec<String>,
}

/// Complete pre-flight results
#[derive(Debug, Default)]
pub struct PreflightResult {
    pub local_env: Vec<CheckResult>,
    pub network: Vec<CheckResult>,
    pub source_permissions: Vec<CheckResult>,
    pub target_permissions: Vec<CheckResult>,
    pub issues: Vec<PreflightIssue>,
    /// True if pg_dump version < source server version
    pub tool_version_incompatible: bool,
    pub local_pg_version: Option<u32>,
    pub source_pg_version: Option<u32>,
}

impl PreflightResult {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn all_passed(&self) -> bool {
        self.issues.is_empty()
    }

    pub fn failed_count(&self) -> usize {
        self.issues.len()
    }

    /// Print formatted output
    pub fn print(&self) {
        println!();
        println!("Pre-flight Checks");
        println!("{}", "═".repeat(61));
        println!();

        if !self.local_env.is_empty() {
            println!("Local Environment:");
            for check in &self.local_env {
                let icon = if check.passed { "✓" } else { "✗" };
                println!("  {} {}", icon, check.message);
                if let Some(ref details) = check.details {
                    println!("      {}", details);
                }
            }
            println!();
        }

        if !self.network.is_empty() {
            println!("Network Connectivity:");
            for check in &self.network {
                let icon = if check.passed { "✓" } else { "✗" };
                println!("  {} {}", icon, check.message);
                if let Some(ref details) = check.details {
                    println!("      {}", details);
                }
            }
            println!();
        }

        if !self.source_permissions.is_empty() {
            println!("Source Permissions:");
            for check in &self.source_permissions {
                let icon = if check.passed { "✓" } else { "✗" };
                println!("  {} {}", icon, check.message);
                if let Some(ref details) = check.details {
                    println!("      {}", details);
                }
            }
            println!();
        }

        if !self.target_permissions.is_empty() {
            println!("Target Permissions:");
            for check in &self.target_permissions {
                let icon = if check.passed { "✓" } else { "✗" };
                println!("  {} {}", icon, check.message);
                if let Some(ref details) = check.details {
                    println!("      {}", details);
                }
            }
            println!();
        }

        println!("{}", "═".repeat(61));
        if self.all_passed() {
            println!("PASSED: All pre-flight checks successful");
        } else {
            println!("FAILED: {} issue(s) must be resolved", self.failed_count());
            println!();
            for (i, issue) in self.issues.iter().enumerate() {
                println!("Issue {}: {}", i + 1, issue.title);
                println!("  {}", issue.explanation);
                println!();
                println!("  Fix options:");
                for fix in &issue.fixes {
                    println!("    • {}", fix);
                }
                println!();
            }
        }
    }
}

/// Run all pre-flight checks
///
/// # Arguments
///
/// * `source_url` - PostgreSQL connection string for source
/// * `target_url` - PostgreSQL connection string for target
/// * `databases` - Optional list of databases to check permissions for
///
/// # Returns
///
/// PreflightResult containing all check results
pub async fn run_preflight_checks(
    source_url: &str,
    target_url: &str,
    _databases: Option<&[String]>,
) -> Result<PreflightResult> {
    let mut result = PreflightResult::new();

    // 1. Check local environment (pg_dump, pg_restore, etc.)
    check_local_environment(&mut result);

    // 2. Check network connectivity
    check_network_connectivity(&mut result, source_url, target_url).await;

    // 3. Check version compatibility (only if we could connect and have local version)
    if result.local_pg_version.is_some() && result.source_pg_version.is_some() {
        check_version_compatibility(&mut result);
    }

    // 4. Check source permissions
    if result
        .network
        .iter()
        .any(|c| c.name == "source" && c.passed)
    {
        check_source_permissions(&mut result, source_url).await;
    }

    // 5. Check target permissions
    if result
        .network
        .iter()
        .any(|c| c.name == "target" && c.passed)
    {
        check_target_permissions(&mut result, target_url).await;
    }

    Ok(result)
}

fn check_local_environment(result: &mut PreflightResult) {
    let tools = ["pg_dump", "pg_dumpall", "pg_restore", "psql"];
    let mut missing = Vec::new();

    for tool in tools {
        match which::which(tool) {
            Ok(path) => {
                let path_str = path.display().to_string();
                match crate::utils::get_pg_tool_version(tool) {
                    Ok(version) => {
                        if tool == "pg_dump" {
                            result.local_pg_version = Some(version);
                        }
                        result.local_env.push(
                            CheckResult::pass(tool, format!("{} found", tool))
                                .with_details(format!("{} ({})", path_str, version)),
                        );
                    }
                    Err(_) => {
                        result.local_env.push(
                            CheckResult::pass(tool, format!("{} found", tool))
                                .with_details(path_str),
                        );
                    }
                }
            }
            Err(_) => {
                missing.push(tool);
                result.local_env.push(CheckResult::fail(
                    tool,
                    format!("{} not found in PATH", tool),
                ));
            }
        }
    }

    if !missing.is_empty() {
        result.issues.push(PreflightIssue {
            title: "Missing PostgreSQL client tools".to_string(),
            explanation: format!("Required tools not found: {}", missing.join(", ")),
            fixes: vec![
                "Ubuntu: sudo apt install postgresql-client-17".to_string(),
                "macOS: brew install postgresql@17".to_string(),
                "RHEL: sudo dnf install postgresql17".to_string(),
            ],
        });
    }
}

async fn check_network_connectivity(
    result: &mut PreflightResult,
    source_url: &str,
    target_url: &str,
) {
    // Check source
    match crate::postgres::connect_with_retry(source_url).await {
        Ok(client) => {
            // Also get server version while connected
            if let Ok(row) = client.query_one("SHOW server_version", &[]).await {
                let version_str: String = row.get(0);
                if let Ok(version) = crate::utils::parse_pg_version_string(&version_str) {
                    result.source_pg_version = Some(version);
                }
            }
            result
                .network
                .push(CheckResult::pass("source", "Source database reachable"));
        }
        Err(e) => {
            result.network.push(CheckResult::fail(
                "source",
                format!("Cannot connect to source: {}", e),
            ));
            result.issues.push(PreflightIssue {
                title: "Source database unreachable".to_string(),
                explanation: e.to_string(),
                fixes: vec![
                    "Verify connection string is correct".to_string(),
                    "Check network connectivity to database host".to_string(),
                    "Ensure firewall allows PostgreSQL port (5432)".to_string(),
                ],
            });
        }
    }

    // Check target
    match crate::postgres::connect_with_retry(target_url).await {
        Ok(_) => {
            result
                .network
                .push(CheckResult::pass("target", "Target database reachable"));
        }
        Err(e) => {
            result.network.push(CheckResult::fail(
                "target",
                format!("Cannot connect to target: {}", e),
            ));
            result.issues.push(PreflightIssue {
                title: "Target database unreachable".to_string(),
                explanation: e.to_string(),
                fixes: vec![
                    "Verify connection string is correct".to_string(),
                    "Check network connectivity to database host".to_string(),
                ],
            });
        }
    }
}

fn check_version_compatibility(result: &mut PreflightResult) {
    let local = result.local_pg_version.unwrap();
    let server = result.source_pg_version.unwrap();

    if local < server {
        result.tool_version_incompatible = true;
        result.local_env.push(CheckResult::fail(
            "version",
            format!("pg_dump version {} < source server {}", local, server),
        ));
        result.issues.push(PreflightIssue {
            title: "PostgreSQL version mismatch".to_string(),
            explanation: format!(
                "Local pg_dump ({}) cannot dump from server ({})",
                local, server
            ),
            fixes: vec![
                format!("Install PostgreSQL {} client tools:", server),
                format!("  Ubuntu: sudo apt install postgresql-client-{}", server),
                format!("  macOS: brew install postgresql@{}", server),
                "Or use SerenAI cloud execution (recommended for SerenDB targets)".to_string(),
            ],
        });
    } else {
        result.local_env.push(CheckResult::pass(
            "version",
            format!("pg_dump version {} >= source server {}", local, server),
        ));
    }
}

async fn check_source_permissions(result: &mut PreflightResult, source_url: &str) {
    if let Ok(client) = crate::postgres::connect_with_retry(source_url).await {
        // Check REPLICATION privilege
        match crate::postgres::check_source_privileges(&client).await {
            Ok(privs) => {
                if privs.has_replication || privs.is_superuser {
                    result.source_permissions.push(CheckResult::pass(
                        "replication",
                        "Has REPLICATION privilege",
                    ));
                } else {
                    result.source_permissions.push(CheckResult::fail(
                        "replication",
                        "Missing REPLICATION privilege",
                    ));
                    result.issues.push(PreflightIssue {
                        title: "Missing REPLICATION privilege".to_string(),
                        explanation: "Required for continuous sync".to_string(),
                        fixes: vec!["Run: ALTER USER <username> REPLICATION;".to_string()],
                    });
                }
            }
            Err(e) => {
                result.source_permissions.push(CheckResult::fail(
                    "privileges",
                    format!("Failed to check: {}", e),
                ));
            }
        }

        // Check table SELECT permissions
        match crate::postgres::check_table_select_permissions(&client).await {
            Ok(perms) => {
                if perms.all_accessible() {
                    result.source_permissions.push(CheckResult::pass(
                        "select",
                        format!("Has SELECT on all {} tables", perms.accessible_tables.len()),
                    ));
                } else {
                    let inaccessible = &perms.inaccessible_tables;
                    let count = inaccessible.len();
                    let preview: Vec<&str> =
                        inaccessible.iter().take(5).map(|s| s.as_str()).collect();
                    let details = if count > 5 {
                        format!("{}, ... ({} more)", preview.join(", "), count - 5)
                    } else {
                        preview.join(", ")
                    };

                    result.source_permissions.push(
                        CheckResult::fail("select", format!("Missing SELECT on {} tables", count))
                            .with_details(details),
                    );
                    result.issues.push(PreflightIssue {
                        title: "Missing table permissions".to_string(),
                        explanation: format!("User needs SELECT on {} tables", count),
                        fixes: vec![
                            "Run: GRANT SELECT ON ALL TABLES IN SCHEMA public TO <username>;"
                                .to_string(),
                        ],
                    });
                }
            }
            Err(e) => {
                result.source_permissions.push(CheckResult::fail(
                    "select",
                    format!("Failed to check table permissions: {}", e),
                ));
            }
        }
    }
}

async fn check_target_permissions(result: &mut PreflightResult, target_url: &str) {
    if let Ok(client) = crate::postgres::connect_with_retry(target_url).await {
        match crate::postgres::check_target_privileges(&client).await {
            Ok(privs) => {
                if privs.has_create_db || privs.is_superuser {
                    result
                        .target_permissions
                        .push(CheckResult::pass("createdb", "Can create databases"));
                } else {
                    result
                        .target_permissions
                        .push(CheckResult::fail("createdb", "Cannot create databases"));
                    result.issues.push(PreflightIssue {
                        title: "Missing CREATEDB privilege".to_string(),
                        explanation: "Cannot create databases on target".to_string(),
                        fixes: vec!["Run: ALTER USER <username> CREATEDB;".to_string()],
                    });
                }

                if privs.is_superuser || privs.has_replication {
                    result.target_permissions.push(CheckResult::pass(
                        "subscription",
                        "Can create subscriptions",
                    ));
                } else {
                    result.target_permissions.push(CheckResult::fail(
                        "subscription",
                        "Cannot create subscriptions",
                    ));
                }
            }
            Err(e) => {
                result.target_permissions.push(CheckResult::fail(
                    "privileges",
                    format!("Failed to check: {}", e),
                ));
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_check_result_pass() {
        let check = CheckResult::pass("test", "Test passed");
        assert!(check.passed);
        assert_eq!(check.name, "test");
    }

    #[test]
    fn test_check_result_fail() {
        let check = CheckResult::fail("test", "Test failed");
        assert!(!check.passed);
    }

    #[test]
    fn test_check_result_with_details() {
        let check = CheckResult::pass("test", "Test passed").with_details("Some details");
        assert_eq!(check.details, Some("Some details".to_string()));
    }

    #[test]
    fn test_preflight_result_empty_passes() {
        let result = PreflightResult::new();
        assert!(result.all_passed());
        assert_eq!(result.failed_count(), 0);
    }

    #[test]
    fn test_preflight_result_with_issues() {
        let mut result = PreflightResult::new();
        result.issues.push(PreflightIssue {
            title: "Test issue".to_string(),
            explanation: "Test".to_string(),
            fixes: vec![],
        });
        assert!(!result.all_passed());
        assert_eq!(result.failed_count(), 1);
    }

    #[test]
    fn test_preflight_issue_multiple_fixes() {
        let issue = PreflightIssue {
            title: "Test".to_string(),
            explanation: "Details".to_string(),
            fixes: vec!["Fix 1".to_string(), "Fix 2".to_string()],
        };
        assert_eq!(issue.fixes.len(), 2);
    }
}

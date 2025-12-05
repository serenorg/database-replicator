use std::fs;
use std::process::Command;
use tempfile::tempdir;

#[test]
fn test_target_command() {
    let temp_dir = tempdir().unwrap();
    let home_dir = temp_dir.path();
    let state_dir = home_dir.join(".database-replicator");
    let state_file = state_dir.join("state.json");

    let bin_path = env!("CARGO_BIN_EXE_database-replicator");

    // Test `target get` when state is not set
    let output = Command::new(bin_path)
        .arg("target")
        .arg("get")
        .env("HOME", home_dir)
        .output()
        .expect("Failed to execute command");
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("Target database URL is not set."));

    // Test `target set`
    let target_url = "postgres://user:pass@host:5432/db";
    let output = Command::new(bin_path)
        .arg("target")
        .arg("set")
        .arg(target_url)
        .env("HOME", home_dir)
        .output()
        .expect("Failed to execute command");
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains(&format!("Target database URL set to: {}", target_url)));

    // Verify state file content
    let state_content = fs::read_to_string(&state_file).unwrap();
    assert!(state_content.contains(target_url));

    // Test `target get` when state is set
    let output = Command::new(bin_path)
        .arg("target")
        .arg("get")
        .env("HOME", home_dir)
        .output()
        .expect("Failed to execute command");
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains(&format!("Current target database URL: {}", target_url)));

    // Test `target unset`
    let output = Command::new(bin_path)
        .arg("target")
        .arg("unset")
        .env("HOME", home_dir)
        .output()
        .expect("Failed to execute command");
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("Target database URL unset."));

    // Verify state file content
    let state_content = fs::read_to_string(&state_file).unwrap();
    assert!(!state_content.contains(target_url));
}

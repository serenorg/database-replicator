use std::process::Command;
use tempfile::tempdir;

#[test]
fn test_remote_to_local_fallback() {
    let temp_dir = tempdir().unwrap();
    let source_db_path = temp_dir.path().join("source.db");
    let target_db_path = temp_dir.path().join("target.db");

    // Create dummy database files
    std::fs::write(&source_db_path, "").unwrap();
    std::fs::write(&target_db_path, "").unwrap();

    let bin_path = env!("CARGO_BIN_EXE_database-replicator");

    let output = Command::new(bin_path)
        .arg("init")
        .arg("--source")
        .arg(source_db_path.to_str().unwrap())
        .arg("--target")
        .arg(target_db_path.to_str().unwrap())
        .arg("--seren")
        .arg("--no-interactive")
        .output()
        .expect("Failed to execute command");

    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);

    println!("stdout: {}", stdout);
    println!("stderr: {}", stderr);
    dbg!(&output);

    assert!(stderr.contains("--seren flag is only compatible with SerenDB targets."));
}

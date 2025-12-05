use std::fs;
use std::process::Command;
use tempfile::tempdir;

#[tokio::test]
#[ignore] // This test requires manual interaction due to inquire prompts
async fn test_interactive_serendb_selection() {
    let temp_dir = tempdir().unwrap();
    let home_dir = temp_dir.path();
    let state_dir = home_dir.join(".database-replicator");
    let _ = fs::create_dir_all(&state_dir);

    let bin_path = env!("CARGO_BIN_EXE_database-replicator");

    println!("\n--- Starting interactive SerenDB selection test ---");
    println!("This test requires manual interaction. Please follow the prompts.");
    println!("Ensure SEREN_API_KEY is set in your environment or be ready to enter it.");

    // This command will trigger the interactive selection. We cannot assert stdout directly
    // due to the interactive nature, but we can verify it doesn't crash and potentially
    // manually observe the prompts.
    let output = Command::new(bin_path)
        .arg("init")
        .arg("--source")
        .arg("sqlite:///tmp/dummy.db") // Dummy source, won't be connected
        .arg("--seren")
        .env("HOME", home_dir) // Use temp home for state file
        .output()
        .expect("Failed to execute command");

    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);

    println!("stdout: {}", stdout);
    println!("stderr: {}", stderr);

    // If the interactive selection was successful and a target was chosen, it should be saved
    // We can't automate the interaction, so this test mainly verifies the flow doesn't panic.
    // A more sophisticated integration test would use expect-test or similar for interactive prompts.
    assert!(output.status.success() || stderr.contains("Target database URL not provided"));
}

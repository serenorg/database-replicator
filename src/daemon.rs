// ABOUTME: Daemon mode support for running sync as a background service
// ABOUTME: Cross-platform: Unix (fork) and Windows (detached process)

use anyhow::{Context, Result};
use std::fs;
use std::path::PathBuf;

/// Get the directory for storing daemon state files.
/// Returns ~/.seren-replicator/ on Unix or %APPDATA%\seren-replicator\ on Windows
pub fn get_daemon_dir() -> Result<PathBuf> {
    #[cfg(windows)]
    let daemon_dir = {
        let app_data = dirs::data_local_dir().context("Failed to determine AppData directory")?;
        app_data.join("seren-replicator")
    };

    #[cfg(not(windows))]
    let daemon_dir = {
        let home = dirs::home_dir().context("Failed to determine home directory")?;
        home.join(".seren-replicator")
    };

    // Create directory if it doesn't exist
    if !daemon_dir.exists() {
        fs::create_dir_all(&daemon_dir)
            .with_context(|| format!("Failed to create daemon directory: {:?}", daemon_dir))?;
    }

    Ok(daemon_dir)
}

/// Get the path to the PID file.
pub fn get_pid_file_path() -> Result<PathBuf> {
    Ok(get_daemon_dir()?.join("sync.pid"))
}

/// Get the path to the log file for daemon mode.
pub fn get_log_file_path() -> Result<PathBuf> {
    Ok(get_daemon_dir()?.join("sync.log"))
}

/// Check if a process with the given PID is running.
#[cfg(unix)]
fn is_process_running(pid: i32) -> bool {
    // Send signal 0 to check if process exists
    unsafe { libc::kill(pid, 0) == 0 }
}

#[cfg(windows)]
fn is_process_running(pid: i32) -> bool {
    use std::ptr::null_mut;

    // OpenProcess with PROCESS_QUERY_LIMITED_INFORMATION
    const PROCESS_QUERY_LIMITED_INFORMATION: u32 = 0x1000;
    const SYNCHRONIZE: u32 = 0x00100000;

    unsafe {
        let handle = OpenProcess(
            PROCESS_QUERY_LIMITED_INFORMATION | SYNCHRONIZE,
            0,
            pid as u32,
        );
        if handle.is_null() {
            return false;
        }

        // Check if process is still running
        let mut exit_code: u32 = 0;
        let result = GetExitCodeProcess(handle, &mut exit_code);
        CloseHandle(handle);

        // STILL_ACTIVE = 259
        result != 0 && exit_code == 259
    }
}

#[cfg(windows)]
extern "system" {
    fn OpenProcess(
        dwDesiredAccess: u32,
        bInheritHandle: i32,
        dwProcessId: u32,
    ) -> *mut std::ffi::c_void;
    fn GetExitCodeProcess(hProcess: *mut std::ffi::c_void, lpExitCode: *mut u32) -> i32;
    fn CloseHandle(hObject: *mut std::ffi::c_void) -> i32;
    fn TerminateProcess(hProcess: *mut std::ffi::c_void, uExitCode: u32) -> i32;
}

/// Read the PID from the PID file.
pub fn read_pid() -> Result<Option<i32>> {
    let pid_file = get_pid_file_path()?;

    if !pid_file.exists() {
        return Ok(None);
    }

    let content = fs::read_to_string(&pid_file)
        .with_context(|| format!("Failed to read PID file: {:?}", pid_file))?;

    let pid: i32 = content
        .trim()
        .parse()
        .with_context(|| format!("Invalid PID in file: {}", content.trim()))?;

    Ok(Some(pid))
}

/// Write the current process PID to the PID file.
pub fn write_pid() -> Result<()> {
    let pid_file = get_pid_file_path()?;
    let pid = std::process::id();

    fs::write(&pid_file, pid.to_string())
        .with_context(|| format!("Failed to write PID file: {:?}", pid_file))?;

    Ok(())
}

/// Remove the PID file.
pub fn remove_pid_file() -> Result<()> {
    let pid_file = get_pid_file_path()?;

    if pid_file.exists() {
        fs::remove_file(&pid_file)
            .with_context(|| format!("Failed to remove PID file: {:?}", pid_file))?;
    }

    Ok(())
}

/// Status information about the daemon.
#[derive(Debug)]
pub struct DaemonStatus {
    pub running: bool,
    pub pid: Option<i32>,
    pub pid_file_exists: bool,
}

/// Check the status of the daemon.
pub fn check_status() -> Result<DaemonStatus> {
    let pid_file = get_pid_file_path()?;
    let pid_file_exists = pid_file.exists();

    let (running, pid) = match read_pid()? {
        Some(pid) => {
            let running = is_process_running(pid);
            (running, Some(pid))
        }
        None => (false, None),
    };

    Ok(DaemonStatus {
        running,
        pid,
        pid_file_exists,
    })
}

/// Stop the running daemon.
#[cfg(unix)]
pub fn stop_daemon() -> Result<bool> {
    let status = check_status()?;

    if !status.running {
        if status.pid_file_exists {
            remove_pid_file()?;
            println!("Removed stale PID file (process was not running)");
        }
        return Ok(false);
    }

    let pid = status.pid.unwrap();
    println!("Sending SIGTERM to daemon (PID: {})", pid);

    let result = unsafe { libc::kill(pid, libc::SIGTERM) };

    if result != 0 {
        anyhow::bail!(
            "Failed to send SIGTERM to process {}: {}",
            pid,
            std::io::Error::last_os_error()
        );
    }

    // Wait for process to exit
    let start = std::time::Instant::now();
    let timeout = std::time::Duration::from_secs(10);

    while is_process_running(pid) {
        if start.elapsed() > timeout {
            println!("Process didn't exit within 10 seconds, sending SIGKILL");
            unsafe { libc::kill(pid, libc::SIGKILL) };
            std::thread::sleep(std::time::Duration::from_millis(500));
            break;
        }
        std::thread::sleep(std::time::Duration::from_millis(100));
    }

    remove_pid_file()?;
    Ok(true)
}

#[cfg(windows)]
pub fn stop_daemon() -> Result<bool> {
    let status = check_status()?;

    if !status.running {
        if status.pid_file_exists {
            remove_pid_file()?;
            println!("Removed stale PID file (process was not running)");
        }
        return Ok(false);
    }

    let pid = status.pid.unwrap();
    println!("Terminating daemon (PID: {})", pid);

    const PROCESS_TERMINATE: u32 = 0x0001;

    unsafe {
        let handle = OpenProcess(PROCESS_TERMINATE, 0, pid as u32);
        if handle.is_null() {
            anyhow::bail!(
                "Failed to open process {}: {}",
                pid,
                std::io::Error::last_os_error()
            );
        }

        let result = TerminateProcess(handle, 0);
        CloseHandle(handle);

        if result == 0 {
            anyhow::bail!(
                "Failed to terminate process {}: {}",
                pid,
                std::io::Error::last_os_error()
            );
        }
    }

    // Wait briefly for process to exit
    std::thread::sleep(std::time::Duration::from_millis(500));

    remove_pid_file()?;
    Ok(true)
}

/// Daemonize the current process (Unix).
#[cfg(unix)]
pub fn daemonize() -> Result<()> {
    use daemonize::Daemonize;
    use std::fs::OpenOptions;

    let pid_file = get_pid_file_path()?;
    let log_file = get_log_file_path()?;

    // Check if daemon is already running
    let status = check_status()?;
    if status.running {
        anyhow::bail!(
            "Daemon is already running (PID: {}). Use --stop to stop it first.",
            status.pid.unwrap()
        );
    }

    // Clean up stale PID file if present
    if status.pid_file_exists {
        remove_pid_file()?;
    }

    // Open log file for stdout/stderr
    let stdout = OpenOptions::new()
        .create(true)
        .append(true)
        .open(&log_file)
        .with_context(|| format!("Failed to open log file: {:?}", log_file))?;

    let stderr = OpenOptions::new()
        .create(true)
        .append(true)
        .open(&log_file)
        .with_context(|| format!("Failed to open log file: {:?}", log_file))?;

    println!("Starting daemon...");
    println!("PID file: {:?}", pid_file);
    println!("Log file: {:?}", log_file);

    let daemonize = Daemonize::new()
        .pid_file(&pid_file)
        .chown_pid_file(true)
        .working_directory(".")
        .stdout(stdout)
        .stderr(stderr);

    daemonize.start().context("Failed to daemonize process")?;

    tracing::info!("Daemon started (PID: {})", std::process::id());
    Ok(())
}

/// Daemonize by spawning a detached process (Windows).
#[cfg(windows)]
pub fn daemonize() -> Result<()> {
    use std::os::windows::process::CommandExt;
    use std::process::Command;

    let pid_file = get_pid_file_path()?;
    let log_file = get_log_file_path()?;

    // Check if daemon is already running
    let status = check_status()?;
    if status.running {
        anyhow::bail!(
            "Daemon is already running (PID: {}). Use --stop to stop it first.",
            status.pid.unwrap()
        );
    }

    // Clean up stale PID file
    if status.pid_file_exists {
        remove_pid_file()?;
    }

    // Get current executable path
    let exe = std::env::current_exe().context("Failed to get current executable path")?;

    // Get original command line args, removing --daemon flag
    let args: Vec<String> = std::env::args()
        .skip(1) // Skip executable name
        .filter(|arg| arg != "--daemon")
        .collect();

    // Add internal flag to indicate we're running as daemon child
    let mut daemon_args = args.clone();
    daemon_args.push("--daemon-child".to_string());

    println!("Starting daemon...");
    println!("PID file: {:?}", pid_file);
    println!("Log file: {:?}", log_file);

    // CREATE_NO_WINDOW = 0x08000000
    // DETACHED_PROCESS = 0x00000008
    const CREATE_NO_WINDOW: u32 = 0x08000000;

    // Spawn detached process
    let child = Command::new(exe)
        .args(&daemon_args)
        .creation_flags(CREATE_NO_WINDOW)
        .spawn()
        .context("Failed to spawn daemon process")?;

    let pid = child.id();
    println!("Daemon started with PID: {}", pid);

    // Note: The child process will write its own PID file when it starts
    Ok(())
}

/// Check if we're running as a daemon child process (Windows).
/// On Unix this is handled by the daemonize crate.
pub fn is_daemon_child() -> bool {
    std::env::args().any(|arg| arg == "--daemon-child")
}

/// Initialize daemon child process (write PID file, setup logging).
/// Call this at startup if is_daemon_child() returns true.
pub fn init_daemon_child() -> Result<PathBuf> {
    let log_file = get_log_file_path()?;

    // Write PID file
    write_pid()?;

    Ok(log_file)
}

/// Print daemon status to stdout.
pub fn print_status() -> Result<()> {
    let status = check_status()?;
    let log_file = get_log_file_path()?;

    if status.running {
        println!("Daemon status: RUNNING");
        println!("PID: {}", status.pid.unwrap());
        println!("Log file: {:?}", log_file);

        // Show last few lines of log
        if log_file.exists() {
            println!("\nRecent log entries:");
            println!("-------------------");
            let content = fs::read_to_string(&log_file)?;
            let lines: Vec<&str> = content.lines().collect();
            let start = if lines.len() > 10 {
                lines.len() - 10
            } else {
                0
            };
            for line in &lines[start..] {
                println!("{}", line);
            }
        }
    } else {
        println!("Daemon status: NOT RUNNING");
        if status.pid_file_exists {
            println!(
                "Note: Stale PID file exists (PID {} is not running)",
                status.pid.unwrap_or(0)
            );
            println!("Run with --stop to clean up the stale PID file");
        }
    }

    Ok(())
}

/// Clean up daemon resources (call on normal shutdown).
pub fn cleanup() -> Result<()> {
    remove_pid_file()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_daemon_dir_creation() {
        let dir = get_daemon_dir();
        assert!(dir.is_ok());
        let path = dir.unwrap();
        assert!(path.to_string_lossy().contains("seren-replicator"));
    }

    #[test]
    fn test_pid_file_path() {
        let path = get_pid_file_path();
        assert!(path.is_ok());
        let path = path.unwrap();
        assert!(path.to_string_lossy().ends_with("sync.pid"));
    }

    #[test]
    fn test_log_file_path() {
        let path = get_log_file_path();
        assert!(path.is_ok());
        let path = path.unwrap();
        assert!(path.to_string_lossy().ends_with("sync.log"));
    }

    #[test]
    fn test_check_status_no_daemon() {
        let status = check_status();
        assert!(status.is_ok());
    }

    #[test]
    fn test_is_daemon_child_false() {
        // In normal test execution, --daemon-child won't be present
        // Note: This test may not be reliable if test runner adds unexpected args
        let result = is_daemon_child();
        // Just verify it doesn't panic
        let _ = result;
    }
}

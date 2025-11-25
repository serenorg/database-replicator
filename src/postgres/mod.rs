// ABOUTME: PostgreSQL utilities module
// ABOUTME: Exports connection management and common database operations

pub mod connection;
pub mod extensions;
pub mod privileges;

pub use connection::{add_keepalive_params, connect, connect_with_retry};
pub use extensions::{
    get_available_extensions, get_installed_extensions, get_preloaded_libraries, requires_preload,
    AvailableExtension, Extension,
};
pub use privileges::{
    check_source_privileges, check_target_privileges, check_wal_level, PrivilegeCheck,
};

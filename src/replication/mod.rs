// ABOUTME: Replication utilities module
// ABOUTME: Handles PostgreSQL logical replication setup and monitoring

pub mod monitor;
pub mod publication;
pub mod subscription;

pub use monitor::{
    get_replication_lag, get_subscription_status, is_replication_caught_up, SourceReplicationStats,
    SubscriptionStats,
};
pub use publication::{create_publication, drop_publication, list_publications};
pub use subscription::{
    create_subscription, detect_subscription_state, drop_subscription, list_subscriptions,
    wait_for_sync, SubscriptionState,
};

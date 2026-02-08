//! Retry Manager - Smart exponential backoff for connection recovery
//!
//! Manages retry attempts with increasing delays: 5s, 10s, 15s, 30s, 60s, 120s, 180s, 300s...

use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Notify;
use tracing::{debug, info, warn};

/// Default backoff schedule in seconds
const DEFAULT_BACKOFF_SCHEDULE: &[u64] = &[5, 10, 15, 30, 60, 120, 180, 300];

/// Manages retry attempts with exponential backoff
pub struct RetryManager {
    backoff_schedule: Vec<u64>,
    current_attempt: AtomicUsize,
    is_retrying: AtomicBool,
    stop_signal: Arc<Notify>,
}

impl RetryManager {
    /// Create a new retry manager with default backoff schedule
    pub fn new() -> Self {
        Self {
            backoff_schedule: DEFAULT_BACKOFF_SCHEDULE.to_vec(),
            current_attempt: AtomicUsize::new(0),
            is_retrying: AtomicBool::new(false),
            stop_signal: Arc::new(Notify::new()),
        }
    }

    /// Create with custom backoff schedule
    pub fn with_schedule(schedule: Vec<u64>) -> Self {
        Self {
            backoff_schedule: schedule,
            current_attempt: AtomicUsize::new(0),
            is_retrying: AtomicBool::new(false),
            stop_signal: Arc::new(Notify::new()),
        }
    }

    /// Get the current attempt number
    pub fn current_attempt(&self) -> usize {
        self.current_attempt.load(Ordering::Relaxed)
    }

    /// Get the next delay duration based on current attempt
    pub fn next_delay(&self) -> Duration {
        let attempt = self.current_attempt.fetch_add(1, Ordering::Relaxed);
        let index = attempt.min(self.backoff_schedule.len() - 1);
        let seconds = self.backoff_schedule[index];
        Duration::from_secs(seconds)
    }

    /// Reset the retry counter (call on successful connection)
    pub fn reset(&self) {
        self.current_attempt.store(0, Ordering::Relaxed);
        self.is_retrying.store(false, Ordering::Relaxed);
    }

    /// Check if currently in retry loop
    pub fn is_retrying(&self) -> bool {
        self.is_retrying.load(Ordering::Relaxed)
    }

    /// Stop the retry loop
    pub fn stop(&self) {
        self.stop_signal.notify_waiters();
    }

    /// Spawn a background retry loop
    /// Returns a handle that can be used to check if retry is active
    pub fn spawn_retry_loop<F, Fut, S>(
        self: Arc<Self>,
        dest_id: i32,
        health_check: F,
        on_success: S,
    ) -> tokio::task::JoinHandle<()>
    where
        F: Fn() -> Fut + Send + Sync + 'static,
        Fut: std::future::Future<Output = bool> + Send,
        S: FnOnce() + Send + 'static,
    {
        // Prevent multiple retry loops
        if self.is_retrying.swap(true, Ordering::Relaxed) {
            debug!("Retry loop already running for dest {}", dest_id);
            return tokio::spawn(async {});
        }

        let stop_signal = self.stop_signal.clone();
        let retry_manager = self.clone();

        tokio::spawn(async move {
            info!("Starting retry loop for destination {}", dest_id);

            loop {
                let delay = retry_manager.next_delay();
                let attempt = retry_manager.current_attempt();

                info!(
                    "Dest {}: Retry attempt {} in {:?}",
                    dest_id, attempt, delay
                );

                // Wait for delay or stop signal
                tokio::select! {
                    _ = tokio::time::sleep(delay) => {},
                    _ = stop_signal.notified() => {
                        info!("Retry loop stopped for dest {}", dest_id);
                        retry_manager.is_retrying.store(false, Ordering::Relaxed);
                        return;
                    }
                }

                // Try health check
                if health_check().await {
                    info!("Dest {}: Connection recovered after {} attempts!", dest_id, attempt);
                    retry_manager.reset();
                    on_success();
                    return;
                } else {
                    warn!("Dest {}: Health check failed, will retry", dest_id);
                }
            }
        })
    }
}

impl Default for RetryManager {
    fn default() -> Self {
        Self::new()
    }
}

impl Clone for RetryManager {
    fn clone(&self) -> Self {
        Self {
            backoff_schedule: self.backoff_schedule.clone(),
            current_attempt: AtomicUsize::new(self.current_attempt.load(Ordering::Relaxed)),
            is_retrying: AtomicBool::new(self.is_retrying.load(Ordering::Relaxed)),
            stop_signal: self.stop_signal.clone(),
        }
    }
}

/// Check if an error message indicates a connection error
pub fn is_connection_error(error: &str) -> bool {
    let error_lower = error.to_lowercase();
    
    // Connection-related patterns
    let patterns = [
        "connection refused",
        "connection reset",
        "connection closed",
        "connection timed out",
        "timeout",
        "network",
        "broken pipe",
        "no route to host",
        "host unreachable",
        "connection aborted",
        "socket",
        "eof",
        "end of file",
        "i/o error",
        "io error",
        "connect error",
        "failed to connect",
        "unable to connect",
        "could not connect",
        "dns",
        "resolve",
        "ssl",
        "tls",
        "handshake",
    ];

    patterns.iter().any(|p| error_lower.contains(p))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_backoff_schedule() {
        let manager = RetryManager::new();
        
        assert_eq!(manager.next_delay(), Duration::from_secs(5));
        assert_eq!(manager.next_delay(), Duration::from_secs(10));
        assert_eq!(manager.next_delay(), Duration::from_secs(15));
        assert_eq!(manager.next_delay(), Duration::from_secs(30));
        assert_eq!(manager.next_delay(), Duration::from_secs(60));
        assert_eq!(manager.next_delay(), Duration::from_secs(120));
        assert_eq!(manager.next_delay(), Duration::from_secs(180));
        assert_eq!(manager.next_delay(), Duration::from_secs(300));
        // Should stay at max
        assert_eq!(manager.next_delay(), Duration::from_secs(300));
        assert_eq!(manager.next_delay(), Duration::from_secs(300));
    }

    #[test]
    fn test_reset() {
        let manager = RetryManager::new();
        
        manager.next_delay();
        manager.next_delay();
        manager.reset();
        
        assert_eq!(manager.current_attempt(), 0);
        assert_eq!(manager.next_delay(), Duration::from_secs(5));
    }

    #[test]
    fn test_connection_error_detection() {
        assert!(is_connection_error("Connection refused"));
        assert!(is_connection_error("timeout occurred"));
        assert!(is_connection_error("Network unreachable"));
        assert!(is_connection_error("broken pipe"));
        
        // Non-connection errors
        assert!(!is_connection_error("syntax error"));
        assert!(!is_connection_error("constraint violation"));
        assert!(!is_connection_error("permission denied"));
    }
}

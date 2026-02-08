// src/monitor.rs
use sqlx::PgPool;
use std::time::Duration;
use sysinfo::System;

// This function spawns the task and returns immediately.
// It takes a PgPool clone.
pub fn start(pool: PgPool) {
    // We spawn inside here, so the main thread doesn't block
    tokio::spawn(async move {
        run_loop(pool).await;
    });
}

// Private function containing the actual loop logic
async fn run_loop(pool: PgPool) {
    let mut sys = System::new_all();
    let mut interval = tokio::time::interval(Duration::from_secs(5));

    loop {
        interval.tick().await;
        sys.refresh_all();

        let global_cpu = sys.global_cpu_usage();
        let used_mem = sys.used_memory();
        let total_mem = sys.total_memory();
        let used_swap = sys.used_swap();
        let total_swap = sys.total_swap();

        // Save to DB (truncate first to keep only latest data)
        let truncate_result = sqlx::query("DELETE FROM system_metrics")
            .execute(&pool)
            .await;

        if let Err(e) = truncate_result {
            eprintln!("❌ Monitor Truncate Error: {}", e);
            continue; // Skip insert if truncate fails
        }

        let result = sqlx::query(
            r#"
            INSERT INTO system_metrics (cpu_usage, used_memory, total_memory, used_swap, total_swap)
            VALUES ($1, $2, $3, $4, $5)
            "#,
        )
        .bind(global_cpu)
        .bind(used_mem as i64)
        .bind(total_mem as i64)
        .bind(used_swap as i64)
        .bind(total_swap as i64)
        .execute(&pool)
        .await;

        if let Err(e) = result {
            eprintln!("❌ Monitor Error: {}", e);
        }
    }
}
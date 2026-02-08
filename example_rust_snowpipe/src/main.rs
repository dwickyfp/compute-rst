mod monitor;

use anyhow::Result;
use dotenv::dotenv;
use figlet_rs::FIGfont;
use rosetta::manager::PipelineManager;
use tracing::info;
use sqlx::postgres::PgPoolOptions;

#[tokio::main]
async fn main() -> Result<()> {
    dotenv().ok();
    tracing_subscriber::fmt::init();

    if let Ok(font) = FIGfont::from_file("assets/fonts/Slant.flf") {
        if let Some(figure) = font.convert("Rosetta") {
            println!("{}", figure);
        }
    }

    // Config DB URL should be in environment
    let database_url = std::env::var("CONFIG_DATABASE_URL")
        .expect("CONFIG_DATABASE_URL environment variable must be set");

    info!("Starting Rosetta Pipeline Manager...");
   
    // Create database pool for monitor
    let pool = PgPoolOptions::new()
        .max_connections(10)
        .connect(&database_url)
        .await?;

    // Start monitor in background before starting manager
    monitor::start(pool.clone());
    info!("System monitor started in background");

    // Start the pipeline manager (this will block)
    let manager = PipelineManager::new(&database_url).await?;
    manager.run().await?;

    Ok(())
}

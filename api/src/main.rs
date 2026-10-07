pub mod routes;


use axum::{routing::{get,post}, Router, Extension};
use shared::db::get_db_pool;
use sqlx::PgPool;
use std::net::SocketAddr;
use tracing::{info,error};
use tokio::net::TcpListener;
use lapin::{options::*, types::FieldTable, BasicProperties, Connection, ConnectionProperties};
use std::time::Duration;

#[derive(Clone)]
struct AppState {
    db: PgPool,
}
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .json()
        .with_max_level(tracing::Level::INFO)
        .with_target(false)
        .init();

    tokio::fs::create_dir_all("./uploads").await?;

    let db_url = std::env::var("DATABASE_URL")
    .unwrap_or_else(|_| "postgres://myuser:mypassword@localhost:5432/csv_processor".to_string());
    let pool = get_db_pool(&db_url).await?;
    let state_pool = pool.clone();

    
    let app = Router::new()
        .route("/health", get(routes::health_check))
        .route("/imports", post(routes::upload_csv))
        .route("/imports/:id", get(routes::get_import_status))
        .route("/imports/:id/errors", get(routes::get_import_errors))
        .route("/imports/:id/reports", post(routes::request_report))
        .route("/reports/:id", get(routes::get_report))
        .layer(Extension(state_pool));

    let rabbit_url = std::env::var("RABBITMQ_URL")
    .unwrap_or_else(|_| "amqp://guest:guest@rabbitmq:5672".to_string());
    let rabbit_conn = Connection::connect(&rabbit_url, ConnectionProperties::default()).await?;
    let channel = rabbit_conn.create_channel().await?;

    channel.queue_declare("import_jobs", QueueDeclareOptions::default(), FieldTable::default()).await?;
    channel.queue_declare("report_jobs", QueueDeclareOptions::default(), FieldTable::default()).await?;

    // --- OUTBOX RELAY ---
    let relay_pool = pool.clone();
    tokio::spawn(async move {
        loop {
            let events = sqlx::query!(
                "SELECT id, aggregate_type, payload FROM outbox_events WHERE status = 'PENDING' FOR UPDATE SKIP LOCKED"
            )
            .fetch_all(&relay_pool)
            .await
            .unwrap_or_default();

            for event in events {
                let queue = match event.aggregate_type.as_str() {
                    "ImportJob" => "import_jobs",
                    "ReportJob" => "report_jobs",
                    _ => continue,
                };

                let payload_bytes = serde_json::to_vec(&event.payload).unwrap();
                
                let publish_result = channel.basic_publish(
                    "", queue, BasicPublishOptions::default(), &payload_bytes, BasicProperties::default()
                ).await;

                if publish_result.is_ok() {
                    let _ = sqlx::query!("UPDATE outbox_events SET status = 'PUBLISHED' WHERE id = $1", event.id)
                        .execute(&relay_pool)
                        .await;
                }
            }
            tokio::time::sleep(Duration::from_secs(2)).await;
        }
    });

    // --- STALE JOB SWEEPER ---
    let sweeper_pool = pool.clone();
    tokio::spawn(async move {
        loop {
            let _ = sqlx::query!(
                r#"
                UPDATE imports 
                SET status = 'PENDING', error = 'Worker crashed mid-job, recovering...'
                WHERE status = 'PROCESSING' AND started_at < NOW() - INTERVAL '15 minutes'
                "#
            ).execute(&sweeper_pool).await;

            let _ = sqlx::query!(
                r#"
                UPDATE reports 
                SET status = 'PENDING', error = 'Worker crashed mid-job, recovering...'
                WHERE status = 'PROCESSING' AND started_at < NOW() - INTERVAL '15 minutes'
                "#
            ).execute(&sweeper_pool).await;

            tokio::time::sleep(Duration::from_secs(300)).await;
        }
    });
    
    let addr = SocketAddr::from(([0, 0, 0, 0], 3000));
    let listener = TcpListener::bind(addr).await?;
    info!(event = "api_started", address = %addr, "API listening for requests");
    
    axum::serve(listener, app).await?;

    Ok(())
}

async fn health_check(Extension(state): Extension<AppState>) -> &'static str {
    match sqlx::query("SELECT 1").execute(&state.db).await {
        Ok(_) => "OK - Database Connected",
        Err(_) => "ERROR - Database Disconnected",
    }
}

pub mod routes;


use axum::{routing::{get,post}, Router, Extension};
use shared::db::get_db_pool;
use sqlx::PgPool;
use std::net::SocketAddr;
use tokio::net::TcpListener;
use lapin::{options::*, types::FieldTable, BasicProperties, Connection, ConnectionProperties};
use std::time::Duration;

#[derive(Clone)]
struct AppState {
    db: PgPool,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tokio::fs::create_dir_all("./uploads").await?;

    let db_url = "postgres://myuser:mypassword@localhost:5432/csv_processor";
    let pool = get_db_pool(db_url).await?;
    let relay_pool = pool.clone();
    let app = Router::new()
        .route("/imports", post(routes::upload_csv))
        .route("/imports/:id", get(routes::get_import_status))
        .route("/imports/:id/errors", get(routes::get_import_errors))
        .route("/imports/:id/reports", post(routes::request_report))
        .route("/reports/:id", get(routes::get_report))
        .layer(Extension(relay_pool));

    let addr = SocketAddr::from(([0, 0, 0, 0], 3000));
    let listener = TcpListener::bind(addr).await?;
    println!("API listening on {}", addr);

    let rabbit_conn = Connection::connect(
    "amqp://guest:guest@localhost:5672",
    ConnectionProperties::default(),
)
.await?;
let channel = rabbit_conn.create_channel().await?;

// Declare the queues
channel.queue_declare("import_jobs", QueueDeclareOptions::default(), FieldTable::default()).await?;
channel.queue_declare("report_jobs", QueueDeclareOptions::default(), FieldTable::default()).await?;

let relay_pool = pool.clone();
tokio::spawn(async move {
    loop {
        // Find pending outbox events
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
            
            // Publish to RabbitMQ
            let publish_result = channel.basic_publish(
                "", queue, BasicPublishOptions::default(), &payload_bytes, BasicProperties::default()
            ).await;

            if publish_result.is_ok() {
                // Mark as published in the same loop
                let _ = sqlx::query!("UPDATE outbox_events SET status = 'PUBLISHED' WHERE id = $1", event.id)
                    .execute(&relay_pool)
                    .await;
            }
        }
        tokio::time::sleep(Duration::from_secs(2)).await;
    }
});
    
    axum::serve(listener, app).await?;

    Ok(())
}

async fn health_check(Extension(state): Extension<AppState>) -> &'static str {
    match sqlx::query("SELECT 1").execute(&state.db).await {
        Ok(_) => "OK - Database Connected",
        Err(_) => "ERROR - Database Disconnected",
    }
}

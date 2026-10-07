pub mod routes;


use axum::{routing::{get,post}, Router, Extension};
use shared::db::get_db_pool;
use sqlx::PgPool;
use std::net::SocketAddr;
use tokio::net::TcpListener;

#[derive(Clone)]
struct AppState {
    db: PgPool,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tokio::fs::create_dir_all("./uploads").await?;

    let db_url = "postgres://myuser:mypassword@localhost:5432/csv_processor";
    let pool = get_db_pool(db_url).await?;

    let app = Router::new()
        .route("/imports", post(routes::upload_csv))
        .route("/imports/:id", get(routes::get_import_status))
        .route("/imports/:id/errors", get(routes::get_import_errors))
        .route("/imports/:id/reports", post(routes::request_report))
        .route("/reports/:id", get(routes::get_report))
        .layer(Extension(pool));

    let addr = SocketAddr::from(([0, 0, 0, 0], 3000));
    let listener = TcpListener::bind(addr).await?;
    println!("API listening on {}", addr);
    
    axum::serve(listener, app).await?;

    Ok(())
}

async fn health_check(Extension(state): Extension<AppState>) -> &'static str {
    match sqlx::query("SELECT 1").execute(&state.db).await {
        Ok(_) => "OK - Database Connected",
        Err(_) => "ERROR - Database Disconnected",
    }
}

use lapin::{options::*, types::FieldTable, Connection, ConnectionProperties};
use serde::Serialize;
use shared::db::get_db_pool;
use sqlx::PgPool;
use uuid::Uuid;
use tracing::{info, error};
use futures_lite::stream::StreamExt;

#[derive(Serialize)]
struct ReportRecord {
    user_id: String,
    total_orders: i64,
    total_quantity: i64,
    total_amount: sqlx::types::Decimal,
}
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .json()
        .with_max_level(tracing::Level::INFO)
        .with_target(false)
        .init();

    let db_url = std::env::var("DATABASE_URL")
    .unwrap_or_else(|_| "postgres://myuser:mypassword@localhost:5432/csv_processor".to_string());
let pool = get_db_pool(&db_url).await?;

let rabbit_url = std::env::var("RABBITMQ_URL")
    .unwrap_or_else(|_| "amqp://guest:guest@rabbitmq:5672".to_string());
let conn = Connection::connect(&rabbit_url, ConnectionProperties::default()).await?;
    
    let channel = conn.create_channel().await?;
    channel.basic_qos(1, BasicQosOptions::default()).await?;

    let mut consumer = channel
        .basic_consume("report_jobs", "report_worker", BasicConsumeOptions::default(), FieldTable::default())
        .await?;

    info!(event = "worker_started", worker = "report_worker", "Report Worker listening for jobs");

    while let Some(delivery) = consumer.next().await {
        if let Ok(delivery) = delivery {
            let payload: serde_json::Value = serde_json::from_slice(&delivery.data)?;
            let report_id = Uuid::parse_str(payload["report_id"].as_str().unwrap())?;
            let import_id = Uuid::parse_str(payload["import_id"].as_str().unwrap())?;

            match process_report(&pool, report_id, import_id).await {
                Ok(_) => {
                    delivery.ack(BasicAckOptions::default()).await?;
                    info!(event = "report_completed", report_id = %report_id, import_id = %import_id, "Successfully generated report");
                }
                Err(e) => {
                    error!(event = "report_failed", report_id = %report_id, error = %e, "Failed to process report");
                    let _ = sqlx::query!("UPDATE reports SET status = 'FAILED', error = $1 WHERE id = $2", e.to_string(), report_id)
                        .execute(&pool).await;
                    delivery.ack(BasicAckOptions::default()).await?; 
                }
            }
        }
    }
    Ok(())
}

async fn process_report(pool: &PgPool, report_id: Uuid, import_id: Uuid) -> Result<(), Box<dyn std::error::Error>> {
    let claimed = sqlx::query!(
        "UPDATE reports SET status = 'PROCESSING', started_at = NOW(), attempts = attempts + 1 WHERE id = $1 AND status = 'PENDING' RETURNING id",
        report_id
    )
    .fetch_optional(pool).await?;

    if claimed.is_none() { return Ok(()); }

    // Execute aggregation natively in Postgres for performance
    let records = sqlx::query_as!(
        ReportRecord,
        r#"
        SELECT 
            user_id, 
            COUNT(order_id) as "total_orders!", 
            SUM(quantity) as "total_quantity!", 
            SUM(unit_price * quantity) as "total_amount!"
        FROM import_rows 
        WHERE import_id = $1 
        GROUP BY user_id
        ORDER BY user_id
        "#,
        import_id
    )
    .fetch_all(pool)
    .await?;

    let result_json = serde_json::to_value(records)?;

    sqlx::query!(
        "UPDATE reports SET status = 'COMPLETED', result = $1, completed_at = NOW() WHERE id = $2",
        result_json, report_id
    )
    .execute(pool)
    .await?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use shared::db::get_db_pool;
    use uuid::Uuid;

    #[tokio::test]
    async fn test_process_report_aggregation() {
        let pool = get_db_pool("postgres://myuser:mypassword@localhost:5432/csv_processor").await.unwrap();
        let import_id = Uuid::new_v4();
        let report_id = Uuid::new_v4();

        // 1. Setup mock import and raw data
        sqlx::query!(
            "INSERT INTO imports (id, file_name, status) VALUES ($1, 'test.csv', 'COMPLETED')",
            import_id
        ).execute(&pool).await.unwrap();

        // Insert two orders for U001
        sqlx::query!(
            "INSERT INTO import_rows (import_id, user_id, order_id, product, quantity, unit_price, status) 
             VALUES ($1, 'U001', 'O1001', 'Laptop', 2, 750.00, 'completed')",
            import_id
        ).execute(&pool).await.unwrap();

        sqlx::query!(
            "INSERT INTO import_rows (import_id, user_id, order_id, product, quantity, unit_price, status) 
             VALUES ($1, 'U001', 'O1002', 'Mouse', 3, 25.00, 'completed')",
            import_id
        ).execute(&pool).await.unwrap();

        // 2. Setup pending report job
        sqlx::query!(
            "INSERT INTO reports (id, import_id, status) VALUES ($1, $2, 'PENDING')",
            report_id, import_id
        ).execute(&pool).await.unwrap();

        // 3. Execute the worker function
        process_report(&pool, report_id, import_id).await.unwrap();

        // 4. Assert the report was generated and aggregated correctly
        let report = sqlx::query!("SELECT status, result FROM reports WHERE id = $1", report_id)
            .fetch_one(&pool).await.unwrap();

        assert_eq!(report.status, "COMPLETED");
        
        let result_json = report.result.expect("Report result should not be null");
        let records: Vec<serde_json::Value> = serde_json::from_value(result_json).unwrap();
        
        assert_eq!(records.len(), 1, "Should aggregate into one user record");
        
        let u001_record = &records[0];
        assert_eq!(u001_record["user_id"], "U001");
        assert_eq!(u001_record["total_orders"], 2);
        assert_eq!(u001_record["total_quantity"], 5);
        
        // 2 * 750 + 3 * 25 = 1575
        let total_amount = u001_record["total_amount"].as_str().unwrap().parse::<f64>().unwrap();
        assert_eq!(total_amount, 1575.00);

        // Cleanup
        sqlx::query!("DELETE FROM imports WHERE id = $1", import_id).execute(&pool).await.unwrap();
    }
}
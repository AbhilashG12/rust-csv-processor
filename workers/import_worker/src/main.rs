use lapin::{options::*, types::FieldTable, Connection, ConnectionProperties};
use serde::Deserialize;
use shared::db::get_db_pool;
use sqlx::PgPool;
use uuid::Uuid;
use futures_lite::stream::StreamExt;

#[derive(Deserialize, Debug)]
struct CsvRow {
    user_id: String,
    order_id: String,
    product: String,
    quantity: i32,
    unit_price: sqlx::types::Decimal,
    status: String,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let pool = get_db_pool("postgres://myuser:mypassword@localhost:5432/csv_processor").await?;
    let conn = Connection::connect("amqp://guest:guest@localhost:5672", ConnectionProperties::default()).await?;
    let channel = conn.create_channel().await?;

    // Prefetch count of 1 ensures fair dispatch (worker doesn't hoard messages)
    channel.basic_qos(1, BasicQosOptions::default()).await?;

    let mut consumer = channel
        .basic_consume("import_jobs", "import_worker", BasicConsumeOptions::default(), FieldTable::default())
        .await?;

    println!("Import Worker listening for jobs...");

    while let Some(delivery) = consumer.next().await {
        if let Ok(delivery) = delivery {
            let payload: serde_json::Value = serde_json::from_slice(&delivery.data)?;
            let import_id = Uuid::parse_str(payload["import_id"].as_str().unwrap())?;
            let file_path = payload["file_path"].as_str().unwrap().to_string();

            match process_import(&pool, import_id, &file_path).await {
                Ok(_) => {
                    delivery.ack(BasicAckOptions::default()).await?;
                    println!("Successfully processed import: {}", import_id);
                }
                Err(e) => {
                    println!("Failed to process import {}: {:?}", import_id, e);
                    // Nack with requeue=false if we want it to go to a DLQ, or true to retry.
                    // For this assignment, we mark DB as FAILED and ack the message to remove it from the queue.
                    let _ = sqlx::query!("UPDATE imports SET status = 'FAILED', error = $1 WHERE id = $2", e.to_string(), import_id)
                        .execute(&pool).await;
                    delivery.ack(BasicAckOptions::default()).await?; 
                }
            }
        }
    }
    Ok(())
}

async fn process_import(pool: &PgPool, import_id: Uuid, file_path: &str) -> Result<(), Box<dyn std::error::Error>> {
    // 1. Claim Job safely
    let claimed = sqlx::query!(
        "UPDATE imports SET status = 'PROCESSING', started_at = NOW(), attempts = attempts + 1 WHERE id = $1 AND status = 'PENDING' RETURNING id",
        import_id
    )
    .fetch_optional(pool).await?;

    if claimed.is_none() {
        return Ok(()); // Job was already picked up by another worker or is done
    }

    // 2. Read CSV Streaming
    let mut rdr = csv::Reader::from_path(file_path)?;
    let mut valid_count = 0;
    let mut invalid_count = 0;
    let mut row_number = 1;

    for result in rdr.deserialize::<CsvRow>() {
        row_number += 1;
        match result {
            Ok(row) => {
                // Insert valid row (ON CONFLICT DO NOTHING handles duplicate processing if worker crashed mid-job previously)
                sqlx::query!(
                    r#"
                    INSERT INTO import_rows (import_id, user_id, order_id, product, quantity, unit_price, status)
                    VALUES ($1, $2, $3, $4, $5, $6, $7)
                    ON CONFLICT (import_id, order_id) DO NOTHING
                    "#,
                    import_id, row.user_id, row.order_id, row.product, row.quantity, row.unit_price, row.status
                ).execute(pool).await?;
                valid_count += 1;
            }
            Err(e) => {
                // Insert invalid row
                let raw_record = e.to_string(); // In a production app, we'd extract the raw StringRecord from the error
                sqlx::query!(
                    "INSERT INTO invalid_rows (import_id, row_number, raw_data, reason) VALUES ($1, $2, $3, $4)",
                    import_id, row_number, raw_record, "Parse Error: Invalid format or missing fields"
                ).execute(pool).await?;
                invalid_count += 1;
            }
        }
    }

    // 3. Complete Job
    let final_status = if invalid_count > 0 { "COMPLETED_WITH_ERRORS" } else { "COMPLETED" };
    
    sqlx::query!(
        "UPDATE imports SET status = $1, total_rows = $2, valid_rows = $3, invalid_rows = $4, completed_at = NOW() WHERE id = $5",
        final_status, valid_count + invalid_count, valid_count, invalid_count, import_id
    ).execute(pool).await?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use shared::db::get_db_pool;
    use std::fs;
    use uuid::Uuid;

    #[tokio::test]
    async fn test_process_import_handles_invalid_rows() {
        let pool = get_db_pool("postgres://myuser:mypassword@localhost:5432/csv_processor").await.unwrap();
        let import_id = Uuid::new_v4();
        let mut temp_path = std::env::temp_dir();
        temp_path.push(format!("test_import_{}.csv", import_id));
        let file_path = temp_path.to_str().unwrap().to_string();

        // 1. Create mock CSV data with one valid and one invalid row (non-numeric price)
        let csv_content = "user_id,order_id,product,quantity,unit_price,status\n\
                           U001,O1001,Laptop,2,750.00,completed\n\
                           U002,O1002,Keyboard,1,abc,completed";
        fs::write(&file_path, csv_content).unwrap();

        // 2. Setup database state
        sqlx::query!(
            "INSERT INTO imports (id, file_name, status) VALUES ($1, $2, 'PENDING')",
            import_id,
            "test.csv"
        )
        .execute(&pool)
        .await
        .unwrap();

        // 3. Execute the worker function
        process_import(&pool, import_id, &file_path).await.unwrap();

        // 4. Assert Job Status
        let job = sqlx::query!("SELECT status, total_rows, valid_rows, invalid_rows FROM imports WHERE id = $1", import_id)
            .fetch_one(&pool).await.unwrap();
        
        assert_eq!(job.status, "COMPLETED_WITH_ERRORS");
        assert_eq!(job.total_rows, 2);
        assert_eq!(job.valid_rows, 1);
        assert_eq!(job.invalid_rows, 1);

        // 5. Assert Database State
        let valid_rows_count = sqlx::query!("SELECT count(*) FROM import_rows WHERE import_id = $1", import_id)
            .fetch_one(&pool).await.unwrap().count.unwrap();
        assert_eq!(valid_rows_count, 1);

        let invalid_rows_count = sqlx::query!("SELECT count(*) FROM invalid_rows WHERE import_id = $1", import_id)
            .fetch_one(&pool).await.unwrap().count.unwrap();
        assert_eq!(invalid_rows_count, 1);

        // Cleanup
        let _ = fs::remove_file(&file_path);
        sqlx::query!("DELETE FROM imports WHERE id = $1", import_id).execute(&pool).await.unwrap();
    }
}
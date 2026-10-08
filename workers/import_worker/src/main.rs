use lapin::{options::*, types::FieldTable, Connection, ConnectionProperties};
use serde::Deserialize;
use tracing::{info, error};
use shared::db::get_db_pool;
use sqlx::PgPool;
use uuid::Uuid;
use futures_lite::stream::StreamExt;
use std::time::Duration;

#[derive(Deserialize, Debug)]
struct CsvRow {
    user_id: String,
    order_id: String,
    product: String,
    quantity: i32,
    unit_price: sqlx::types::Decimal, // Matches NUMERIC in Postgres
    status: String,
}

const MAX_ATTEMPTS: i32 = 3;
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
        // Default to the Docker hostname if the env var is missing
        .unwrap_or_else(|_| "amqp://guest:guest@rabbitmq:5672".to_string());

    // Robust connection retry loop
    let rabbit_conn = loop {
        match lapin::Connection::connect(&rabbit_url, lapin::ConnectionProperties::default()).await {
            Ok(conn) => {
                println!("Successfully connected to RabbitMQ!");
                break conn;
            }
            Err(e) => {
                println!("Waiting for RabbitMQ... ({})", e);
                tokio::time::sleep(std::time::Duration::from_secs(2)).await;
            }
        }
    };
    let channel = rabbit_conn.create_channel().await?;
    channel.basic_qos(1, BasicQosOptions::default()).await?;

    let mut consumer = channel
        .basic_consume("import_jobs", "import_worker", BasicConsumeOptions::default(), FieldTable::default())
        .await?;

    info!(event = "worker_started", worker = "import_worker", "Import Worker listening for jobs");

    while let Some(delivery) = consumer.next().await {
        if let Ok(delivery) = delivery {
            let payload: serde_json::Value = serde_json::from_slice(&delivery.data)?;
            let import_id = Uuid::parse_str(payload["import_id"].as_str().unwrap())?;
            let file_path = payload["file_path"].as_str().unwrap().to_string();

            let job = sqlx::query!("SELECT attempts FROM imports WHERE id = $1", import_id)
                .fetch_optional(&pool).await?;
            
            let current_attempts = job.map(|j| j.attempts).unwrap_or(0);

            match process_import(&pool, import_id, &file_path).await {
                Ok(true) => {
                    delivery.ack(BasicAckOptions::default()).await?;
                    info!(event = "import_completed", job_id = %import_id, "Successfully processed import");
                }
                Ok(false) => {
                    delivery.ack(BasicAckOptions::default()).await?;
                }
                Err(e) => {
                    error!(event = "import_failed", job_id = %import_id, attempt = current_attempts, error = %e, "Failed to process import");
                    
                    if current_attempts >= MAX_ATTEMPTS {
                        let _ = sqlx::query!(
                            "UPDATE imports SET status = 'FAILED', error = $1 WHERE id = $2", 
                            format!("Max attempts reached. Last error: {}", e), import_id
                        ).execute(&pool).await;
                        
                        delivery.ack(BasicAckOptions::default()).await?; 
                    } else {
                        let backoff_secs = 2_u64.pow((current_attempts + 1) as u32);
                        tokio::time::sleep(Duration::from_secs(backoff_secs)).await;
                        delivery.nack(BasicNackOptions { multiple: false, requeue: true }).await?;
                    }
                }
            }
        }
    }
    Ok(())
}

async fn process_import(pool: &PgPool, import_id: Uuid, file_path: &str) -> Result<bool, Box<dyn std::error::Error>> {
    // 1. Transactional Claiming
    // The RETURNING id ensures that if two workers query this concurrently, only one gets the row.
    let claimed = sqlx::query!(
        "UPDATE imports SET status = 'PROCESSING', started_at = NOW(), attempts = attempts + 1 WHERE id = $1 AND status = 'PENDING' RETURNING id",
        import_id
    )
    .fetch_optional(pool).await?;

    if claimed.is_none() {
        return Ok(false); // Job was already picked up by another worker or is already done
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
                // Duplicate row processing: ON CONFLICT DO NOTHING handles duplicates 
                // if a worker crashed midway and is retrying this file.
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
                let raw_record = e.to_string();
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

    Ok(true) // Successfully processed and committed
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
        
        // Generate cross-platform temp path for Windows compatibility
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
        let result = process_import(&pool, import_id, &file_path).await.unwrap();
        assert_eq!(result, true, "Worker should successfully claim and process the job");

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

#[cfg(test)]
mod csv_tests {
    use super::*;
    use csv::ReaderBuilder;

    #[test]
    fn test_parse_valid_row() {
        let data = "user_id,order_id,product,quantity,unit_price,status\n\
                    U001,O1001,Laptop,2,750.00,completed";
        let mut rdr = ReaderBuilder::new().from_reader(data.as_bytes());
        let result: Result<CsvRow, _> = rdr.deserialize().next().unwrap();
        
        assert!(result.is_ok());
        let row = result.unwrap();
        assert_eq!(row.user_id, "U001");
        assert_eq!(row.order_id, "O1001");
        assert_eq!(row.quantity, 2);
    }

    #[test]
    fn test_parse_invalid_unit_price() {
        // Matches the assignment's specific invalid data requirement
        let data = "user_id,order_id,product,quantity,unit_price,status\n\
                    0006,O1011,Keyboard,1,abc,completed";
        let mut rdr = ReaderBuilder::new().from_reader(data.as_bytes());
        let result: Result<CsvRow, _> = rdr.deserialize().next().unwrap();
        
        assert!(result.is_err(), "Non-numeric unit_price should fail deserialization");
    }

    #[test]
    fn test_parse_invalid_quantity() {
        let data = "user_id,order_id,product,quantity,unit_price,status\n\
                    U007,O1012,Monitor,not_a_number,250.00,completed";
        let mut rdr = ReaderBuilder::new().from_reader(data.as_bytes());
        let result: Result<CsvRow, _> = rdr.deserialize().next().unwrap();
        
        assert!(result.is_err(), "Non-integer quantity should fail deserialization");
    }
}
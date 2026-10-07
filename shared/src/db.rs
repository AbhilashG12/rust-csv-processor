use sqlx::{postgres::PgPoolOptions, PgPool};
use std::time::Duration;

pub async fn get_db_pool(database_url: &str) -> Result<PgPool, sqlx::Error> {
    let pool = PgPoolOptions::new()
        .max_connections(5)
        .acquire_timeout(Duration::from_secs(3))
        .connect(database_url)
        .await?;

    sqlx::migrate!("../migrations").run(&pool).await?;

    Ok(pool)
}

#[cfg(test)]
mod tests {
    use super::*;
    use uuid::Uuid;

    #[tokio::test]
    async fn test_database_idempotency() {
        let db_url = "postgres://myuser:mypassword@localhost:5432/csv_processor";
        let pool = get_db_pool(db_url).await.expect("Failed to connect to DB");

        let import_id = Uuid::new_v4();

        // 1. Create a mock import job
        sqlx::query!(
            "INSERT INTO imports (id, file_name, status) VALUES ($1, $2, $3)",
            import_id,
            "test_file.csv",
            "PROCESSING"
        )
        .execute(&pool)
        .await
        .unwrap();

        // 2. Insert a valid row
        let insert_row_query = r#"
            INSERT INTO import_rows (import_id, user_id, order_id, product, quantity, unit_price, status)
            VALUES ($1, $2, $3, $4, $5, $6, $7)
            ON CONFLICT (import_id, order_id) DO NOTHING
        "#;

        let result1 = sqlx::query(insert_row_query)
            .bind(import_id)
            .bind("U001")
            .bind("O1001")
            .bind("Laptop")
            .bind(2)
            .bind(750.00)
            .bind("completed")
            .execute(&pool)
            .await
            .unwrap();

        assert_eq!(result1.rows_affected(), 1, "First insert should succeed");

        // 3. Attempt to insert the exact same row (Simulating a worker crash and retry)
        let result2 = sqlx::query(insert_row_query)
            .bind(import_id)
            .bind("U001")
            .bind("O1001") // Same order_id
            .bind("Laptop")
            .bind(2)
            .bind(750.00)
            .bind("completed")
            .execute(&pool)
            .await
            .unwrap();

        assert_eq!(result2.rows_affected(), 0, "Second insert should be ignored due to ON CONFLICT DO NOTHING");
        
        // 4. Cleanup
        sqlx::query!("DELETE FROM imports WHERE id = $1", import_id).execute(&pool).await.unwrap();
    }
}

#[cfg(test)]
mod phase5_tests {
    use super::*;
    use uuid::Uuid;

    #[tokio::test]
    async fn test_duplicate_worker_claim() {
        let db_url = "postgres://myuser:mypassword@localhost:5432/csv_processor";
        let pool = get_db_pool(db_url).await.expect("Failed to connect to DB");
        let import_id = Uuid::new_v4();

        // 1. Insert a PENDING job
        sqlx::query!("INSERT INTO imports (id, file_name, status) VALUES ($1, 'test.csv', 'PENDING')", import_id)
            .execute(&pool).await.unwrap();

        // 2. Worker A claims the job
        let worker_a_claim = sqlx::query!(
            "UPDATE imports SET status = 'PROCESSING' WHERE id = $1 AND status = 'PENDING' RETURNING id",
            import_id
        ).fetch_optional(&pool).await.unwrap();
        assert!(worker_a_claim.is_some(), "Worker A should successfully claim the PENDING job");

        // 3. Worker B attempts to claim the exact same job concurrently
        let worker_b_claim = sqlx::query!(
            "UPDATE imports SET status = 'PROCESSING' WHERE id = $1 AND status = 'PENDING' RETURNING id",
            import_id
        ).fetch_optional(&pool).await.unwrap();
        assert!(worker_b_claim.is_none(), "Worker B must receive None because the status is no longer PENDING");

        sqlx::query!("DELETE FROM imports WHERE id = $1", import_id).execute(&pool).await.unwrap();
    }

    #[tokio::test]
    async fn test_stale_job_sweeper_recovery() {
        let db_url = "postgres://myuser:mypassword@localhost:5432/csv_processor";
        let pool = get_db_pool(db_url).await.expect("Failed to connect to DB");
        let import_id = Uuid::new_v4();

        // 1. Simulate a worker that crashed 20 minutes ago
        sqlx::query!(
            "INSERT INTO imports (id, file_name, status, started_at) VALUES ($1, 'crash.csv', 'PROCESSING', NOW() - INTERVAL '20 minutes')",
            import_id
        ).execute(&pool).await.unwrap();

        // 2. Run the exact sweeper query from the API
        let rows_affected = sqlx::query!(
            "UPDATE imports SET status = 'PENDING', error = 'Worker crashed mid-job, recovering...' WHERE status = 'PROCESSING' AND started_at < NOW() - INTERVAL '15 minutes'"
        ).execute(&pool).await.unwrap().rows_affected();
        
        assert_eq!(rows_affected, 1, "Sweeper should have recovered 1 stale job");

        // 3. Verify the state is ready for retry
        let job = sqlx::query!("SELECT status FROM imports WHERE id = $1", import_id).fetch_one(&pool).await.unwrap();
        assert_eq!(job.status, "PENDING", "Job must be reset to PENDING");

        sqlx::query!("DELETE FROM imports WHERE id = $1", import_id).execute(&pool).await.unwrap();
    }
}
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

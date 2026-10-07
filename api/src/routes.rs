use axum::{
    extract::{Extension, Multipart, Path},
    http::StatusCode,
    response::IntoResponse,
    Json,
};
use serde_json::json;
use sqlx::PgPool;
use tokio::{fs::File, io::AsyncWriteExt};
use serde::Serialize;
use lapin::{Connection, ConnectionProperties};
use uuid::Uuid;


#[derive(Serialize)]
pub struct HealthResponse {
    status: &'static str,
    database: &'static str,
    queue: &'static str,
}



pub async fn health_check(
    Extension(db): Extension<PgPool>,
) -> Json<HealthResponse> {
    let db_status = match sqlx::query("SELECT 1").execute(&db).await {
        Ok(_) => "ok",
        Err(_) => "degraded",
    };

    // Lightweight queue ping
    let queue_status = match Connection::connect("amqp://guest:guest@rabbitmq:5672", ConnectionProperties::default()).await {
        Ok(_) => "ok",
        Err(_) => "degraded",
    };

    let overall = if db_status == "ok" && queue_status == "ok" { "ok" } else { "degraded" };

    Json(HealthResponse {
        status: overall,
        database: db_status,
        queue: queue_status,
    })
}

// --- 1. Upload CSV ---
pub async fn upload_csv(
    Extension(db): Extension<PgPool>,
    mut multipart: Multipart,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    let mut file_name = String::new();
    let import_id = Uuid::new_v4();
    let file_path = format!("./uploads/{}.csv", import_id);

    while let Some(mut field) = multipart.next_field().await.unwrap_or(None) {
        if field.name() == Some("file") {
            file_name = field.file_name().unwrap_or("unknown.csv").to_string();
            
            // Simplified reading: load the segment into bytes
            let data = field.bytes().await.map_err(|e| {
                (StatusCode::BAD_REQUEST, format!("Failed to read file: {}", e))
            })?;

            let mut file = File::create(&file_path).await.map_err(|e| {
                (StatusCode::INTERNAL_SERVER_ERROR, format!("Failed to create file: {}", e))
            })?;

            file.write_all(&data).await.map_err(|e| {
                (StatusCode::INTERNAL_SERVER_ERROR, format!("Failed to write file: {}", e))
            })?;
        }
    }

    if file_name.is_empty() {
        return Err((StatusCode::BAD_REQUEST, "No file uploaded".to_string()));
    }

    // Begin Transaction: Ensure Job and Outbox Event are created together
    let mut tx = db.begin().await.map_err(|e| {
        (StatusCode::INTERNAL_SERVER_ERROR, e.to_string())
    })?;

    sqlx::query!(
        "INSERT INTO imports (id, file_name, status) VALUES ($1, $2, 'PENDING')",
        import_id,
        file_name
    )
    .execute(&mut *tx)
    .await
    .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    let event_payload = json!({ "import_id": import_id, "file_path": file_path });
    
    sqlx::query!(
        "INSERT INTO outbox_events (id, aggregate_type, aggregate_id, event_type, payload) VALUES ($1, 'ImportJob', $2, 'ImportRequested', $3)",
        Uuid::new_v4(),
        import_id,
        event_payload
    )
    .execute(&mut *tx)
    .await
    .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    tx.commit().await.map_err(|e| {
        (StatusCode::INTERNAL_SERVER_ERROR, e.to_string())
    })?;

    Ok((
        StatusCode::ACCEPTED,
        Json(json!({ "import_id": import_id, "status": "PENDING" })),
    ))
}

// --- 2. Import Status ---
pub async fn get_import_status(
    Extension(db): Extension<PgPool>,
    Path(id): Path<Uuid>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    let record = sqlx::query!(
        "SELECT id, status, total_rows, processed_rows, valid_rows, invalid_rows FROM imports WHERE id = $1",
        id
    )
    .fetch_optional(&db)
    .await
    .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    match record {
        Some(row) => Ok(Json(json!({
            "id": row.id, "status": row.status, "total_rows": row.total_rows,
            "processed_rows": row.processed_rows, "valid_rows": row.valid_rows, "invalid_rows": row.invalid_rows
        }))),
        None => Err((StatusCode::NOT_FOUND, "Import not found".to_string())),
    }
}

// --- 3. Invalid Rows ---
pub async fn get_import_errors(
    Extension(db): Extension<PgPool>,
    Path(id): Path<Uuid>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    let records = sqlx::query!(
        "SELECT row_number as row, reason FROM invalid_rows WHERE import_id = $1 ORDER BY row_number ASC",
        id
    )
    .fetch_all(&db)
    .await
    .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    let errors: Vec<_> = records.into_iter().map(|r| json!({ "row": r.row, "reason": r.reason })).collect();
    Ok(Json(errors))
}

// --- 4. Request Report ---
pub async fn request_report(
    Extension(db): Extension<PgPool>,
    Path(import_id): Path<Uuid>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    let report_id = Uuid::new_v4();
    let mut tx = db.begin().await.map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    sqlx::query!(
        "INSERT INTO reports (id, import_id, status) VALUES ($1, $2, 'PENDING')",
        report_id,
        import_id
    )
    .execute(&mut *tx)
    .await
    .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    let event_payload = json!({ "report_id": report_id, "import_id": import_id });
    
    sqlx::query!(
        "INSERT INTO outbox_events (id, aggregate_type, aggregate_id, event_type, payload) VALUES ($1, 'ReportJob', $2, 'ReportRequested', $3)",
        Uuid::new_v4(),
        report_id,
        event_payload
    )
    .execute(&mut *tx)
    .await
    .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    tx.commit().await.map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    Ok((
        StatusCode::ACCEPTED,
        Json(json!({ "report_id": report_id, "status": "PENDING" })),
    ))
}

// --- 5. Report Retrieval ---
pub async fn get_report(
    Extension(db): Extension<PgPool>,
    Path(id): Path<Uuid>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    let record = sqlx::query!(
        "SELECT status, result FROM reports WHERE id = $1",
        id
    )
    .fetch_optional(&db)
    .await
    .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    match record {
        Some(row) => {
            if row.status == "COMPLETED" {
                Ok(Json(json!({ "status": row.status, "data": row.result })))
            } else {
                Ok(Json(json!({ "status": row.status })))
            }
        }
        None => Err((StatusCode::NOT_FOUND, "Report not found".to_string())),
    }
}

#[cfg(test)]
mod api_tests {
    use super::*;
    use shared::db::get_db_pool;
    
    // Note: This requires a running local Postgres instance to pass
    #[tokio::test]
    async fn test_api_outbox_insertion() {
        let pool = get_db_pool("postgres://myuser:mypassword@localhost:5432/csv_processor").await.unwrap();
        let import_id = Uuid::new_v4();
        
        // 1. Simulate the transaction logic from upload_csv
        let mut tx = pool.begin().await.unwrap();
        
        sqlx::query!(
            "INSERT INTO imports (id, file_name, status) VALUES ($1, $2, 'PENDING')",
            import_id,
            "test_upload.csv"
        ).execute(&mut *tx).await.unwrap();

        let payload = json!({ "import_id": import_id, "file_path": "/tmp/test.csv" });
        
        sqlx::query!(
            "INSERT INTO outbox_events (id, aggregate_type, aggregate_id, event_type, payload) VALUES ($1, 'ImportJob', $2, 'ImportRequested', $3)",
            Uuid::new_v4(),
            import_id,
            payload
        ).execute(&mut *tx).await.unwrap();

        tx.commit().await.unwrap();

        // 2. Assert both records exist
        let import_record = sqlx::query!("SELECT status FROM imports WHERE id = $1", import_id)
            .fetch_one(&pool).await.unwrap();
        assert_eq!(import_record.status, "PENDING");

        let outbox_record = sqlx::query!("SELECT status FROM outbox_events WHERE aggregate_id = $1", import_id)
            .fetch_one(&pool).await.unwrap();
        assert_eq!(outbox_record.status, "PENDING");
        
        // Cleanup
        sqlx::query!("DELETE FROM imports WHERE id = $1", import_id).execute(&pool).await.unwrap();
    }
}
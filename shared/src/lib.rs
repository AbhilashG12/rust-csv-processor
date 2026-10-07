pub mod db;
pub mod models;



use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sqlx::{FromRow, Type};
use uuid::Uuid;

#[derive(Type, Serialize, Deserialize, Debug, Clone, PartialEq)]
#[sqlx(type_name = "job_status", rename_all = "SCREAMING_SNAKE_CASE")]
pub enum JobStatus {
    Pending,
    Processing,
    Completed,
    CompletedWithErrors,
    Failed,
}

#[derive(Type, Serialize, Deserialize, Debug, Clone, PartialEq)]
#[sqlx(type_name = "outbox_status", rename_all = "SCREAMING_SNAKE_CASE")]
pub enum OutboxStatus {
    Pending,
    Published,
}

#[derive(FromRow, Serialize, Deserialize, Debug, Clone)]
pub struct ImportJob {
    pub id: Uuid,
    pub status: JobStatus,
    pub file_name: String,
    pub total_rows: i32,
    pub processed_rows: i32,
    pub valid_rows: i32,
    pub invalid_rows: i32,
    pub attempts: i32,
    pub error: Option<String>,
    pub created_at: DateTime<Utc>,
    pub started_at: Option<DateTime<Utc>>,
    pub completed_at: Option<DateTime<Utc>>,
}

#[derive(FromRow, Serialize, Deserialize, Debug, Clone)]
pub struct ReportJob {
    pub id: Uuid,
    pub import_id: Uuid,
    pub status: JobStatus,
    pub attempts: i32,
    pub error: Option<String>,
    pub result: Option<serde_json::Value>,
    pub created_at: DateTime<Utc>,
    pub started_at: Option<DateTime<Utc>>,
    pub completed_at: Option<DateTime<Utc>>,
}

#[derive(FromRow, Serialize, Deserialize, Debug, Clone)]
pub struct ImportedRow {
    pub id: Uuid,
    pub import_id: Uuid,
    pub user_id: String,
    pub order_id: String,
    pub product: String,
    pub quantity: i32,
    pub unit_price: sqlx::types::Decimal,
    pub status: String,
    pub created_at: DateTime<Utc>,
}

#[derive(FromRow, Serialize, Deserialize, Debug, Clone)]
pub struct InvalidRow {
    pub id: Uuid,
    pub import_id: Uuid,
    pub row_number: i32,
    pub raw_data: String,
    pub reason: String,
    pub created_at: DateTime<Utc>,
}

#[derive(FromRow, Serialize, Deserialize, Debug, Clone)]
pub struct OutboxEvent {
    pub id: Uuid,
    pub aggregate_type: String,
    pub aggregate_id: Uuid,
    pub event_type: String,
    pub payload: serde_json::Value,
    pub status: OutboxStatus,
    pub created_at: DateTime<Utc>,
}

# Rust Background Processing System

A resilient, distributed background processing system built in Rust. It handles concurrent CSV imports and report generation while maintaining data consistency, idempotency, and graceful recovery from worker failures.

## Architecture

The system uses a distributed architecture with the **Transactional Outbox Pattern** to reliably deliver messages between the API and background workers.

- **API (Axum):** Handles HTTP requests and CSV uploads. It does not process CSVs synchronously. Job state and an `outbox_event` are written to PostgreSQL in a single transaction.
- **RabbitMQ:** Provides reliable asynchronous message delivery.
- **Import Worker:** Streams CSV files to keep memory usage low. Valid rows are persisted while malformed rows are recorded separately.
- **Report Worker:** Performs aggregation directly in PostgreSQL using `GROUP BY` and stores the generated report as JSON.

### Core Design Principle

**The database is the source of truth.**

RabbitMQ indicates that work needs to be processed, while PostgreSQL tracks the actual state of each job. Workers remain stateless and can be horizontally scaled without losing job state.

## Tech Stack

- **Language:** Rust
- **Web Framework:** Axum
- **Database:** PostgreSQL
- **Database Library:** SQLx
- **Message Queue:** RabbitMQ
- **RabbitMQ Client:** lapin
- **Observability:** tracing, tracing-subscriber
- **Infrastructure:** Docker, Docker Compose

## Running Locally

Start the entire system with Docker Compose:

docker compose up --build


Verify that the API is running:

curl http://localhost:3000/health


## API Documentation

No UI is provided. The API can be tested using cURL or Postman.

### 1. Upload CSV

curl -X POST http://localhost:3000/imports -F "file=@test.csv"


Example response:

{ "import_id": "UUID", "status": "PENDING" }


### 2. Check Import Status

curl http://localhost:3000/imports/{uuid}


Example response:

{ "id": "UUID", "status": "COMPLETEDWITHERRORS", "processedrows": 12, "validrows": 11, "invalid_rows": 1 }


### 3. View Invalid Rows

curl http://localhost:3000/imports/{uuid}/errors


Example response:

[ { "row": 12, "reason": "Parse Error: Invalid format" } ]


### 4. Request a Report

curl -X POST http://localhost:3000/imports/{uuid}/reports


Example response:

{ "report_id": "UUID", "status": "PENDING" }


### 5. Fetch Report Data

curl http://localhost:3000/reports/{report_uuid}


Example response:

{ "status": "COMPLETED", "data": [] }


## Job Lifecycle and Reliability

### Failure Handling

- A background sweeper detects jobs stuck in `PROCESSING` beyond a configured timeout.
- If a worker crashes, the sweeper resets the job to `PENDING`.
- Malformed CSV rows are recorded separately instead of failing the entire import.
- Valid rows continue to be processed even when individual rows are invalid.

### Retry Strategy

Transient failures, such as database connection issues, trigger retries with exponential backoff.

After `MAX_ATTEMPTS = 3`:

1. The message is acknowledged.
2. The job status is set to `FAILED`.
3. No further automatic retries are attempted.

### Idempotency

RabbitMQ provides at-least-once delivery, so workers must safely handle duplicate messages.

The system achieves idempotency through:

- **Database constraints:** `import_rows` uses `UNIQUE(import_id, order_id)`.
- **Conflict handling:** Duplicate rows are ignored using `ON CONFLICT DO NOTHING`.
- **Atomic job claims:** Workers claim jobs using an atomic `UPDATE ... RETURNING` query.
- **ACK ordering:** RabbitMQ messages are acknowledged only after the PostgreSQL transaction successfully commits.

This ensures that a worker crash during processing does not result in inconsistent or duplicated data when the job is retried.

## Database Schema

### `imports`

Stores import job metadata, status, retry attempts, and row counts.

### `import_rows`

Stores successfully parsed CSV records.

### `invalid_rows`

Stores malformed CSV rows along with their row numbers and error reasons.

### `reports`

Stores report generation state and the generated JSON report.

### `outbox_events`

Stores events that need to be published to RabbitMQ, enabling the Transactional Outbox Pattern.

## Testing

The test suite covers the critical parts of the system:

- **Unit tests:** CSV parsing and malformed row handling.
- **Integration tests:** Transactional Outbox behavior.
- **Reliability tests:** Database idempotency and concurrent worker claims.

Run the tests with:

cargo test --workspace


## Observability

The system uses structured logging through `tracing` and `tracing-subscriber`.

Logs include information useful for debugging background jobs, failures, retries, and worker activity.

## Trade-offs and Known Limitations

### Why RabbitMQ Instead of Kafka?

RabbitMQ is well suited for asynchronous job processing where acknowledgements, retries, and worker-based consumption are important.

Kafka would be a better choice if the system required durable event streams, event replay, or multiple independent consumers.

### Why PostgreSQL?

PostgreSQL provides:

- Durable persistent state
- Transactions
- Strong consistency
- Unique constraints
- Atomic updates

These features are particularly useful for job management and idempotent processing.

### Why Not Redis?

Redis was unnecessary because the core requirements are durable job processing and relational persistence. PostgreSQL already provides the required state management and concurrency guarantees.

### File Storage Limitation

Uploaded files are stored on a shared Docker volume for this assignment.

In production, an object storage service such as Amazon S3 would be preferable. PostgreSQL would store the object key rather than the file itself, allowing workers running on different machines to access the uploaded file reliably.
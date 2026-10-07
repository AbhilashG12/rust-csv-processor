# Build Stage
FROM rust:latest AS builder
WORKDIR /app
COPY . .
# Tell SQLx to use the prepared .sqlx metadata instead of a live DB
ENV SQLX_OFFLINE=true 
RUN cargo build --release

# Runtime Stage
FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y libssl-dev ca-certificates && rm -rf /var/lib/apt/lists/*
WORKDIR /app

COPY --from=builder /app/target/release/api /usr/local/bin/api
COPY --from=builder /app/target/release/import_worker /usr/local/bin/import_worker
COPY --from=builder /app/target/release/report_worker /usr/local/bin/report_worker

# Create uploads directory for the API
RUN mkdir -p /app/uploads
CREATE TABLE imports (
    id UUID PRIMARY KEY,
    file_name TEXT NOT NULL,
    status TEXT NOT NULL,
    total_rows INT NOT NULL DEFAULT 0,
    processed_rows INT NOT NULL DEFAULT 0,
    valid_rows INT NOT NULL DEFAULT 0,
    invalid_rows INT NOT NULL DEFAULT 0,
    attempts INT NOT NULL DEFAULT 0,
    error TEXT,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    started_at TIMESTAMP WITH TIME ZONE,
    completed_at TIMESTAMP WITH TIME ZONE
);

CREATE TABLE import_rows (
    id BIGSERIAL PRIMARY KEY,
    import_id UUID NOT NULL REFERENCES imports(id) ON DELETE CASCADE,
    user_id TEXT NOT NULL,
    order_id TEXT NOT NULL,
    product TEXT NOT NULL,
    quantity INT NOT NULL,
    unit_price NUMERIC(18,2) NOT NULL,
    status TEXT NOT NULL,
    -- Database-level idempotency: Prevents duplicate rows if a worker crashes mid-job
    UNIQUE(import_id, order_id)
);

CREATE TABLE invalid_rows (
    id BIGSERIAL PRIMARY KEY,
    import_id UUID NOT NULL REFERENCES imports(id) ON DELETE CASCADE,
    row_number INT NOT NULL,
    raw_data TEXT NOT NULL,
    reason TEXT NOT NULL
);

CREATE TABLE reports (
    id UUID PRIMARY KEY,
    import_id UUID NOT NULL REFERENCES imports(id) ON DELETE CASCADE,
    status TEXT NOT NULL,
    attempts INT NOT NULL DEFAULT 0,
    error TEXT,
    result JSONB,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    started_at TIMESTAMP WITH TIME ZONE,
    completed_at TIMESTAMP WITH TIME ZONE
);

CREATE TABLE outbox_events (
    id UUID PRIMARY KEY,
    aggregate_type TEXT NOT NULL,
    aggregate_id UUID NOT NULL,
    event_type TEXT NOT NULL,
    payload JSONB NOT NULL,
    status TEXT NOT NULL DEFAULT 'PENDING',
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
);

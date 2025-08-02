-- Create payment queue table
CREATE UNLOGGED TABLE IF NOT EXISTS payment_queue (
    id BIGSERIAL PRIMARY KEY,
    correlation_id UUID NOT NULL UNIQUE,
    amount DECIMAL(10,2) NOT NULL,
    processor_type VARCHAR(10) DEFAULT 'default',
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    processed BOOLEAN DEFAULT FALSE
);

-- Index for efficient FIFO processed with skip locked
CREATE INDEX IF NOT EXISTS idx_payment_queue_pending 
ON payment_queue (id) WHERE processed = FALSE;

-- Index for date range queries on processed payments
CREATE INDEX IF NOT EXISTS idx_payment_queue_processed_date 
ON payment_queue (created_at) WHERE processed = TRUE;

-- Simple health check cache (cleared every 5 seconds)
CREATE UNLOGGED TABLE IF NOT EXISTS health_check_cache (
    key TEXT PRIMARY KEY,
    value JSONB NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE INDEX idx_payment_queue_worker_optimized
ON payment_queue (processed, id) WHERE processed = FALSE;
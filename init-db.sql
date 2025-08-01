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

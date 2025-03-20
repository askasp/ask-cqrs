-- Events table for storing all domain events
CREATE TABLE IF NOT EXISTS events (
    id TEXT PRIMARY KEY,
    stream_name TEXT NOT NULL,
    stream_id TEXT NOT NULL,
    event_data JSONB NOT NULL,
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
    stream_position BIGINT NOT NULL,
    global_position BIGSERIAL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(stream_id, stream_position)
);

-- Indexes for efficient querying
CREATE INDEX IF NOT EXISTS idx_events_global_position ON events(global_position);
CREATE INDEX IF NOT EXISTS idx_events_stream ON events(stream_name);
CREATE INDEX IF NOT EXISTS idx_events_stream_id ON events(stream_id);

-- Notification function and trigger for new events
CREATE OR REPLACE FUNCTION notify_new_event() RETURNS TRIGGER AS $$
BEGIN
    PERFORM pg_notify('new_event', NEW.global_position::text);
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Drop and recreate trigger (triggers can't use IF NOT EXISTS)
DROP TRIGGER IF EXISTS events_notify_trigger ON events;
CREATE TRIGGER events_notify_trigger
    AFTER INSERT ON events
    FOR EACH ROW
    EXECUTE FUNCTION notify_new_event();

-- View snapshots for caching view state
CREATE TABLE IF NOT EXISTS view_snapshots (
    id TEXT PRIMARY KEY,
    view_name TEXT NOT NULL,
    partition_key TEXT NOT NULL,
    state JSONB NOT NULL,
    last_event_position BIGINT NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(view_name, partition_key)
);

-- Index for efficient view snapshot lookups
CREATE INDEX IF NOT EXISTS idx_view_snapshots_lookup ON view_snapshots(view_name, partition_key);

-- Event processing claims for competing consumers - Enhanced to handle all state
CREATE TABLE IF NOT EXISTS event_processing_claims (
    id TEXT PRIMARY KEY,
    stream_name TEXT NOT NULL,
    stream_id TEXT NOT NULL,
    handler_name TEXT NOT NULL,
    last_position BIGINT NOT NULL,
    claimed_by TEXT,
    claim_expires_at TIMESTAMP WITH TIME ZONE,
    error_count INT NOT NULL DEFAULT 0,
    last_error TEXT,
    next_retry_at TIMESTAMP WITH TIME ZONE,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    last_updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(stream_name, stream_id, handler_name)
);

CREATE INDEX IF NOT EXISTS idx_event_claims_handler ON event_processing_claims(handler_name);
CREATE INDEX IF NOT EXISTS idx_event_claims_stream ON event_processing_claims(stream_name, stream_id);
CREATE INDEX IF NOT EXISTS idx_event_claims_expires ON event_processing_claims(claim_expires_at);
CREATE INDEX IF NOT EXISTS idx_event_claims_retry ON event_processing_claims(next_retry_at); 
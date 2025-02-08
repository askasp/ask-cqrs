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

-- Persistent subscriptions for tracking event processing progress
CREATE TABLE IF NOT EXISTS persistent_subscriptions (
    id UUID PRIMARY KEY,
    name TEXT NOT NULL,
    consumer_group TEXT NOT NULL,
    last_processed_position BIGINT DEFAULT 0,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(name, consumer_group)
);

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
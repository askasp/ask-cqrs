-- Events table for storing all domain events


CREATE TABLE IF NOT EXISTS events (
    id TEXT PRIMARY KEY,
    stream_name TEXT NOT NULL,
    stream_id TEXT NOT NULL,
    event_data JSONB NOT NULL,
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
    stream_position BIGINT NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(stream_id, stream_position)
);

CREATE OR REPLACE FUNCTION notify_new_event() RETURNS TRIGGER AS $$
BEGIN
    PERFORM pg_notify('new_event', NEW.id::text);
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Only create trigger if events table exists
DO $$
BEGIN
    IF EXISTS (SELECT FROM information_schema.tables WHERE table_name = 'events') THEN
        -- Drop and recreate trigger (triggers can't use IF NOT EXISTS)
        DROP TRIGGER IF EXISTS events_notify_trigger ON events;
        CREATE TRIGGER events_notify_trigger
            AFTER INSERT ON events
            FOR EACH ROW
            EXECUTE FUNCTION notify_new_event();
    END IF;
END $$;

-- Indexes for efficient querying
CREATE INDEX IF NOT EXISTS idx_events_stream ON events(stream_name);
CREATE INDEX IF NOT EXISTS idx_events_stream_id ON events(stream_id);
CREATE INDEX IF NOT EXISTS idx_events_streamname_streamid_position
    ON events (stream_name, stream_id, stream_position, id);

-- Track event processing progress with integrated retry/dead-letter logic
CREATE TABLE IF NOT EXISTS handler_stream_offsets (
    handler TEXT NOT NULL,
    stream_id TEXT NOT NULL,
    stream_name TEXT NOT NULL,
    last_position BIGINT NOT NULL,
    retry_count INT NOT NULL DEFAULT 0,
    next_retry_at TIMESTAMP WITH TIME ZONE,
    dead_lettered BOOLEAN NOT NULL DEFAULT FALSE,
    dead_lettered_position BIGINT,
    dead_lettered_event_id TEXT,
    last_error TEXT,
    last_updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (handler, stream_id, stream_name)
);

-- Index for finding streams ready for retry
CREATE INDEX IF NOT EXISTS idx_handler_stream_retry_ready
    ON handler_stream_offsets (handler, next_retry_at)
    WHERE next_retry_at IS NOT NULL AND dead_lettered = FALSE;

-- Index for finding dead-lettered streams
CREATE INDEX IF NOT EXISTS idx_handler_stream_dead_lettered
    ON handler_stream_offsets (handler, last_updated_at)
    WHERE dead_lettered = TRUE;

-- Add claimed_by and claimed_until columns to handler_stream_offsets
ALTER TABLE handler_stream_offsets 
    ADD COLUMN IF NOT EXISTS claimed_by TEXT,
    ADD COLUMN IF NOT EXISTS claimed_until TIMESTAMP WITH TIME ZONE;

-- Add index for finding unclaimed streams
CREATE INDEX IF NOT EXISTS idx_handler_stream_unclaimed
    ON handler_stream_offsets (handler, claimed_until)
    WHERE claimed_by IS NULL OR claimed_until IS NOT NULL;

-- View snapshots for caching view state
CREATE TABLE IF NOT EXISTS view_snapshots (
    id TEXT PRIMARY KEY,
    view_name TEXT NOT NULL,
    partition_key TEXT NOT NULL,
    state JSONB NOT NULL,

    -- Map of "stream_name:stream_id" to stream position
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(view_name, partition_key)
);

-- Index for efficient view snapshot lookups
CREATE INDEX IF NOT EXISTS idx_view_snapshots_lookup ON view_snapshots(view_name, partition_key);
CREATE INDEX IF NOT EXISTS idx_handler_stream_offsets_lookup
    ON handler_stream_offsets (handler, stream_name, stream_id, last_position)
    WHERE dead_lettered = FALSE;
    
ALTER TABLE events DROP CONSTRAINT IF EXISTS events_stream_id_stream_position_key;

-- Add the new unique constraint on stream_name and stream_position

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.table_constraints 
        WHERE constraint_name = 'events_stream_name_stream_id_stream_position_key'
          AND table_name = 'events'
    ) THEN
        ALTER TABLE events ADD CONSTRAINT events_stream_name_stream_id_stream_position_key 
            UNIQUE (stream_name, stream_id, stream_position);
    END IF;
END $$;

-- Add processed_stream_positions column to view_snapshots to track which events have been applied
ALTER TABLE view_snapshots ADD COLUMN IF NOT EXISTS processed_stream_positions JSONB NOT NULL DEFAULT '{}'::jsonb;

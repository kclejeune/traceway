CREATE TABLE rate_limit_buckets (
    bucket_key TEXT PRIMARY KEY,
    expires_at INTEGER NOT NULL,
    request_count INTEGER NOT NULL
);

CREATE INDEX idx_rate_limit_buckets_expires_at ON rate_limit_buckets(expires_at);

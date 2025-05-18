CREATE TABLE IF NOT EXISTS tweets (
    id SERIAL PRIMARY KEY,
    timestamp TIMESTAMP NOT NULL,
    username VARCHAR(255) NOT NULL,
    text TEXT,
    created_at TIMESTAMP,
    retweets INTEGER,
    likes INTEGER
);

-- Create index for query performance
CREATE INDEX IF NOT EXISTS idx_tweets_username ON tweets (username);
CREATE INDEX IF NOT EXISTS idx_tweets_created_at ON tweets (created_at);

-- Ensure app_user exists (matches postgres-init.sql)
DO $$ BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_catalog.pg_roles WHERE rolname = 'app_user'
    ) THEN
        CREATE USER app_user WITH PASSWORD 'app_password123';
    END IF;
END $$;

-- Grant privileges on the table
GRANT ALL ON TABLE tweets TO postgres_user;
CREATE TABLE IF NOT EXISTS LOGS.data_load_logs (
    id SERIAL PRIMARY KEY,
    task_name VARCHAR(255) NOT NULL,
    start_time TIMESTAMP NOT NULL,
    end_time TIMESTAMP,
    status VARCHAR(20) DEFAULT 'RUNNING',
    rows_processed INTEGER,
    error_message TEXT,
    duration_seconds INTEGER
);

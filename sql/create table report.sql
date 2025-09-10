-- create table reports
DROP TABLE IF EXISTS reports;
CREATE TABLE reports (
    row_id INT AUTO_INCREMENT PRIMARY KEY,
    id VARCHAR(50) NOT NULL,
    name VARCHAR(255),
    fee DECIMAL(10,2),
    provider VARCHAR(50) NOT NULL,
    reportPeriod CHAR(7) NOT NULL,
    ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    status VARCHAR(20),
    job_id INT,
    FOREIGN KEY (job_id) REFERENCES processing_jobs(job_id),
    UNIQUE (id, provider, reportPeriod)
);

/*CREATE TABLE processing_jobs (
    job_id INT AUTO_INCREMENT PRIMARY KEY,
    file_name VARCHAR(255) NOT NULL,
    provider VARCHAR(255),
    report_period VARCHAR(7), -- format YYYY-MM
    rows_processed INT DEFAULT 0,
    rows_inserted INT DEFAULT 0,
    rows_failed INT DEFAULT 0,
    status ENUM('STARTED', 'SUCCESS', 'FAILED', 'PARTIAL') NOT NULL DEFAULT 'STARTED',
    error_summary TEXT,
    started_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    finished_at TIMESTAMP
);*/



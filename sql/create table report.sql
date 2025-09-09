-- create table reports
/*
CREATE TABLE reports (
    id            VARCHAR(50) NOT NULL,
    name          VARCHAR(255),
    fee           DECIMAL(10,2),
    provider      VARCHAR(50) NOT NULL,
    reportPeriod  CHAR(7) NOT NULL, -- 'YYYY-MM'
    ingested_at   TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    status        VARCHAR(20),

    CONSTRAINT pk_reports PRIMARY KEY (id, provider, reportPeriod)
);
*/

CREATE TABLE processing_jobs (
    job_id INT AUTO_INCREMENT PRIMARY KEY,
    file_name VARCHAR(255) NOT NULL,
    provider VARCHAR(100),
    report_period VARCHAR(20),
    rows_processed INT DEFAULT 0,
    rows_inserted INT DEFAULT 0,
    rows_failed INT DEFAULT 0,
    status ENUM('SUCCESS', 'FAILED', 'PARTIAL') NOT NULL,
    error_summary TEXT,
    started_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    finished_at TIMESTAMP NULL
);


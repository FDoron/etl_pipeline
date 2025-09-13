-- create table reports
/*DROP TABLE IF EXISTS reports;
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
);*/

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

/*
CREATE TABLE clients (
    client_id VARCHAR(9) PRIMARY KEY,
    first_name VARCHAR(255) NOT NULL,
    last_name VARCHAR(255) NOT NULL,
    provider VARCHAR(255) NOT NULL
);

INSERT INTO clients (client_id, first_name, last_name, provider) VALUES
('123456789', 'יוסף', 'כהן', 'ספק_א'),
('987654321', 'שרה', 'לוי', 'ספק_ב'),
('456789123', 'דוד', 'ישראלי', 'ספק_ג'),
('321654987', 'רחל', 'מזרחי', 'ספק_א'),
('789123456', 'משה', 'פרץ', 'ספק_ב');
*/

INSERT INTO clients (client_id, first_name, last_name, provider) VALUES
('012345678', 'אברהם', 'כהן', 'ספק_א'),
('123456789', 'שרה', 'לוי', 'ספק_ב'),
('234567890', 'דוד', 'ישראלי', 'ספק_ג'),
('345678901', 'רחל', 'מזרחי', 'ספק_א'),
('456789012', 'משה', 'פרץ', 'ספק_ב'),
('567890123', 'יעל', 'אדלר', 'ספק_ג'),
('678901234', 'יוסף', 'גולדברג', 'ספק_א'),
('789012345', 'מיכל', 'שפירא', 'ספק_ב'),
('890123456', 'אהרון', 'בלום', 'ספק_ג'),
('901234567', 'תמר', 'רוזן', 'сפק_א'),
('002345671', 'שמואל', 'וינברג', 'ספק_ב'), -- 7-digit valid ID
('013456782', 'נעמי', 'כץ', 'ספק_ג'),    -- 7-digit valid ID
('124567893', 'איתן', 'מלמד', 'ספק_א'),
('235678904', 'חנה', 'ברגר', 'ספק_ב'),
('346789015', 'בנימין', 'שלום', 'ספק_ג'),
('457890126', 'ליאורה', 'גרוס', 'ספק_א'),
('568901237', 'אליהו', 'פלד', 'ספק_ב'),
('679012348', 'מירי', 'שטיין', 'ספק_ג'),
('790123459', 'צבי', 'רוט', 'ספק_א'),
('901234560', 'אסתר', 'הרשקו', 'ספק_ב'),
('000123454', 'יונתן', 'מייזל', 'ספק_ג'), -- 7-digit valid ID
('011234565', 'שושנה', 'ליברמן', 'ספק_א'), -- 7-digit valid ID
('122345676', 'גבריאל', 'שניידר', 'ספק_ב'),
('233456787', 'דינה', 'פישר', 'ספק_ג'),
('344567898', 'שלמה', 'זילבר', 'ספק_א'),
('455678909', 'חיה', 'גלעד', 'ספק_ב'),
('566789010', 'אריה', 'קליין', 'ספק_ג'),
('677890121', 'רחל', 'סגל', 'ספק_א'),
('001234543', 'שלום', 'ברק', 'ספק_б'),   -- 7-digit valid ID
('012345654', 'ליה', 'אורן', 'ספק_ג');    -- 7-digit valid ID
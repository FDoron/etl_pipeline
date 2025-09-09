etl-pipeline/
│
├── config/
│   ├── providers.json      → provider-specific mapping
│   ├── db.yaml             → non-sensitive DB settings
│   └── logging.yaml        → logging config (levels, formats)
│
├── data/
│   ├── inbox/              → incoming files
│   ├── staging/            → normalized, pre-validated
│   ├── processed/          → cleaned & ready for DB
│   ├── failed/             → rejected files
│   └── archive/            → backups / history
│
├── src/
│   ├── utils/              → db.py, logging.py, file_ops.py
│   ├── ingestion/          → file ingestion, column mapping
│   ├── transformation/     → data cleaning & validation
│   ├── metrics/            → KPIs, summaries
│   ├── monitoring/         → watchdog triggers, dashboards
│   └── main.py             → orchestrator (CLI or Streamlit entrypoint)
│
├── tests/                  → pytest-based unit/integration tests
├── generate_sample_data.py → create fake provider files
├── requirements.txt
├── .env                    → secrets (gitignored)
└── README.md

CREATE TABLE IF NOT EXISTS runs (
    run_id INTEGER PRIMARY KEY AUTOINCREMENT,
    started_at TEXT NOT NULL,
    finished_at TEXT,
    keyword TEXT NOT NULL,
    status TEXT NOT NULL,
    total_found INTEGER DEFAULT 0,
    raw_json_path TEXT,
    error_summary TEXT
);

CREATE TABLE IF NOT EXISTS notices (
    notice_id INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id INTEGER NOT NULL,
    bid_ntce_no TEXT,
    bid_ntce_ord TEXT,
    title TEXT,
    demand_org TEXT,
    pub_org TEXT,
    close_dt TEXT,
    notice_url TEXT,
    raw_json_path TEXT,
    collect_status TEXT NOT NULL,
    raw_item_json TEXT,
    FOREIGN KEY (run_id) REFERENCES runs(run_id)
);
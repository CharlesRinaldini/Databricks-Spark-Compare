CREATE TABLE IF NOT EXISTS {env}_gold.comparedemo.sparkcompare_test_results (
    event_id STRING NOT NULL,
    comparison_id STRING NOT NULL,
    test_case_id STRING,
    test_case_name STRING,
    parent_log_id STRING,
    event_type STRING NOT NULL,
    status STRING,
    error_code STRING,
    error_description STRING,
    notes STRING,
    workload_url STRING,
    source_record_count BIGINT,
    target_record_count BIGINT,
    error_count BIGINT,
    results_json STRING,
    created_at STRING NOT NULL
)
USING DELTA;

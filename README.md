# Batch ETL Pipeline — Bronze → Silver → Gold 

A complete batch pipeline:
- Raw ingest (JSONL) → Bronze Parquet
- Clean/standardize → Silver Parquet
- Aggregations → Gold metrics
- Data quality checks at each layer
- Airflow orchestration



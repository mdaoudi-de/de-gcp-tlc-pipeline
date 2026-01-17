# de-gcp-tlc-pipeline

Pipeline Data Engineer end-to-end sur GCP (GCS + BigQuery + Airflow/Cloud Composer) à partir des données publiques NYC TLC.

## Scope (V1)
- Ingestion fichiers (Parquet mensuels) -> GCS (raw)
- Chargement BigQuery (raw)
- Transformations -> BigQuery (curated : fact/dim)
- Data Quality (nulls, types, volumétrie, doublons)
- Orchestration Airflow (local + Composer)
- Restitution Streamlit

## Quickstart (local)
```bash
make venv
make install
cp .env.example .env
make lint
make test

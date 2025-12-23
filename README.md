# StockAnomaly

Comprehensive toolkit for ingesting stock data, training anomaly/fraud detection models, and processing realtime detections. This repository includes ingestion pipelines (historical & realtime), feature engineering, an XGBoost training pipeline, a fraud detector consumer, storage helpers, and Docker Compose wiring for Postgres + Grafana.

**Key components**
- `ingestion/` — data ingestion scripts (NSE, Alpaca, Polygon, realtime ingestion).
- `storage/` — helpers for collecting and persisting raw history.
- `features/feature_builder.py` — feature engineering logic.
- `model/train_model.py` — model training (XGBoost) and evaluation.
- `processing/fraud_detector.py` — consumer that applies models to realtime streams and raises anomalies.
- `utils/` — small helpers (e.g., `kafka.py`).
- `docker-compose.yml` — Compose file to run Postgres, Grafana, and other services.

---

**Table of contents**
- Project overview
- Requirements
- Quickstart (local)
- NSE Postgres ingestion (detailed)
- XGBoost model training (detailed)
- Fraud detector consumer (detailed)
- Docker Compose: Postgres + Grafana integration
- Data layout & features
- Troubleshooting & tips
- Contributing

---

**Project overview**

StockAnomaly is intended to detect anomalous / fraudulent behaviour in stock tick data using both batch and realtime pipelines. The repo contains ingestion adapters (polling & websocket), a feature builder, model training (XGBoost), a realtime fraud detector consumer that can subscribe to Kafka/streams, and infrastructure scaffolding (Postgres for storage, Grafana for dashboards).

**Requirements**

- Python 3.9+ (virtualenv/venv recommended)
- See `requirements.txt` for Python deps; typical packages include: `pandas`, `xgboost`, `scikit-learn`, `psycopg2-binary`, `sqlalchemy`, `kafka-python`, etc.
- Docker & docker-compose (for Postgres + Grafana)

Quick notes:
- Use environment variables for credentials (Postgres, Kafka, Alpaca API keys).
- Config settings live in the `config/` folder (e.g., `config/settings.py`).

---

**Quickstart (local)**

1. Start Postgres + Grafana via Docker Compose:

```bash
docker-compose up -d
```

2. Create required DB/schemas (see `ingestion/nse_postgres_producer.py` and your `config/settings.py` for table names).

3. Run ingestion (historical or realtime) to populate data:

```bash
python ingestion/historical_poll.py   # historical poll
python ingestion/ingest_real_time.py  # realtime ingestion
```

4. Build features and train model:

```bash
python features/feature_builder.py
python model/train_model.py
```

5. Start fraud detector to consume realtime events and score them:

```bash
python processing/fraud_detector.py
```

---

**NSE Postgres ingestion (ingestion/nse_postgres_producer.py)**

Purpose:
- Ingest NSE (National Stock Exchange) tick/quote/trade data and write to Postgres for downstream training and dashboards.

Where to look:
- `ingestion/nse_postgres_producer.py` — main producer script that reads from the data source and persists to Postgres.
- `storage/collect_history.py` — helper utilities for writing CSV or batching inserts.

How it works (summary):
- Reads raw tick/history rows (CSV or API)
- Normalizes fields to a canonical schema (timestamp, symbol, price, volume, bid/ask, source)
- Uses `psycopg2`/`sqlalchemy` to INSERT or COPY into Postgres table(s)
- Optionally publishes messages to Kafka for realtime consumers

Configuration & env vars:
- DB connection string in `config/settings.py` or via `DATABASE_URL` environment variable
- Table names and batch sizes are configurable inside the script

Running it:

```bash
python ingestion/nse_postgres_producer.py --source data/raw_history.csv
```

Notes / best practices:
- Use Postgres COPY for large batch loads.
- Ensure correct timezone handling for timestamps (store as UTC).
- Add indices on `(symbol, timestamp)` and any columns used for Grafana queries.

---

**XGBoost model training (model/train_model.py)**

Purpose:
- Train an XGBoost model to detect anomalies/fraud based on engineered features.

Where to look:
- `model/train_model.py` — training script: loads feature dataset, splits train/val/test, trains XGBoost, evaluates, and persists model artifact.
- `features/feature_builder.py` — builds and persists features used by the model.

Typical flow:
1. Load cleaned feature dataset (CSV or DB table).
2. Split into training/validation/test sets.
3. Define an XGBoost DMatrix and training hyperparameters.
4. Train with early stopping and evaluate on validation set.
5. Save the trained model (joblib/pickle or XGBoost native format) to `model/artifacts/`.

Command example:

```bash
python model/train_model.py --features data/features.csv --output model/artifacts/xgb_model.json
```

Hyperparameters & tips:
- Use `scale_pos_weight` if labels are imbalanced (fraud/anomaly are rare).
- Log metrics (ROC-AUC, precision@k) and save the best model via early stopping.

Model serving:
- The model file can be loaded inside `processing/fraud_detector.py` to score incoming events.

---

**Fraud detector consumer (processing/fraud_detector.py)**

Purpose:
- Subscribe to realtime market/candidate streams (Kafka or other), compute features, score with the trained XGBoost model, and emit anomaly alerts (to Kafka, DB, or webhook).

Where to look:
- `processing/fraud_detector.py` — the consumer logic.
- `utils/kafka.py` — helper for Kafka consumers/producers.

How it works (summary):
- Connects to Kafka topic(s) or reads from a stream
- Reconstructs or computes required features for the model using `features/feature_builder.py` helpers
- Loads persisted model artifact and scores incoming events
- On anomalous score > threshold, the consumer writes an alert to Postgres and optionally publishes to a dedicated Kafka topic

Running it:

```bash
python processing/fraud_detector.py --model model/artifacts/xgb_model.json
```

Operational notes:
- Use a consumer group so you can run multiple instances for scale.
- Keep model loading and feature transforms consistent with training.
- Persist metadata (scoring time, model version, score) to Postgres for auditing and Grafana visualization.

---

**Docker Compose: Postgres + Grafana integration**

This repo includes `docker-compose.yml` to simplify running Postgres and Grafana. The basic idea:
- Postgres stores raw & processed data and alerts.
- Grafana connects to Postgres as a data source and visualizes metrics and alerts.

Steps to use:

1. Start services:

```bash
docker-compose up -d
```

2. Open Grafana UI (default http://localhost:3000) and add Postgres as a data source (connection details usually in `docker-compose.yml` environment variables).

3. Create dashboards exploring tables used by ingestion and the detector (e.g., anomalies table, daily volumes, per-symbol metrics).

Compose tips:
- Ensure Postgres volumes are persisted (check `docker-compose.yml`).
- Expose Postgres port only on trusted networks when deploying.

---

**Data layout & features**

- Raw history CSV: `data/raw_history.csv`
- Realtime annotated CSV for development: `data/us_stock_realtime_with_anomalies.csv`
- Feature builder writes aggregated features used for model training.

Common features:
- price returns over multiple windows
- volume deltas & z-scores
- bid-ask spread features
- time-of-day indicators

Indexing & storage suggestions:
- Partition Postgres tables by date for high-volume ingest.
- Index `(symbol, timestamp)` to speed queries for dashboards and training data pulls.

---

**Troubleshooting & tips**

- If ingestion is slow for large CSVs, use `COPY` to load quickly into Postgres.
- Confirm timezone conversions when joining data across sources.
- When running Grafana, ensure the Postgres user has read access to the tables used by dashboards.

---

**Contributing**

1. Fork & branch
2. Run linters/tests
3. Open a PR with a clear description

---

**Files to inspect for details**
- [ingestion/nse_postgres_producer.py](ingestion/nse_postgres_producer.py)
- [model/train_model.py](model/train_model.py)
- [processing/fraud_detector.py](processing/fraud_detector.py)
- [features/feature_builder.py](features/feature_builder.py)
- [docker-compose.yml](docker-compose.yml)

If you'd like, I can:
- add example Grafana dashboard JSON templates
- add a small example dataset and an end-to-end demo script
- add environment variable examples and a .env.sample

---

Acknowledgements: created to support ingestion, training, and realtime detection workflows.

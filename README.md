# Binance DLT Pipeline (DuckDB-first)

This repo ingests Binance **symbols** and **klines** using [dlt](https://dlthub.com) and lands them in **DuckDB** locally (no Docker required). Later you can switch to ClickHouse by changing `.env`.

## 0) Create and activate a venv
```bash
python3 -m venv .venv
source .venv/bin/activate
# Binance DLT Pipeline (PostgreSQL-first)

This repo ingests Binance **symbols** and **klines** using [dlt](https://dlthub.com) and lands them in **PostgreSQL**.  
Data is later exported for analysis (CSV → Colab/Python) and visualized using value, volume, and volatility metrics.

---

## 0) Create and activate a venv
```bash
python3 -m venv .venv
source .venv/bin/activate

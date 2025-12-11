# Binance DLT Pipeline (PostgreSQL-first)

This repository ingests Binance **symbols** and **klines** using [dlt](https://dlthub.com) and loads them into **PostgreSQL**.  
The exported data is then analyzed to compute price trends, volume patterns, and volatility metrics.

---

## 0) Create and activate a virtual environment
```bash
python3 -m venv .venv
source .venv/bin/activate
```

---

## 1) Install dependencies
```bash
pip install -r requirements.txt
```

Or install manually:

```bash
pip install dlt psycopg2 python-dotenv pandas
```

---

## 2) Configure PostgreSQL

Create a `.env` file in the project root:

```
DLT_POSTGRES_USER=postgres
DLT_POSTGRES_PASSWORD=yourpassword
DLT_POSTGRES_DATABASE=binance
DLT_POSTGRES_HOST=localhost
DLT_POSTGRES_PORT=5432
```

---

## 3) Run the DLT pipeline

This loads 1-hour candlestick data for BTCUSDT and ETHUSDT.

```bash
python dlt_pipeline/binance_pipeline.py
```

Data will be stored in:

```
binance_raw.binance_klines
```

---

## 4) Export data for analysis (optional)

```sql
COPY (
    SELECT * FROM binance_raw.binance_klines
) TO '/tmp/export.csv' CSV HEADER;
```

---

## 5) Analysis Notebook

Open:

```
analysis/binance_analysis.ipynb
```

The notebook includes:
- Price vs time plots
- Hourly and daily volume charts
- Volatility comparison (log returns)

---

## Summary of Results

| Symbol   | Avg Value | Total Volume | Volatility |
|----------|-----------|--------------|------------|
| BTCUSDT  | 64,320.45 | 28,195.3     | 0.0452     |
| ETHUSDT  | 3,285.71  | 51,231.2     | 0.0539     |

BTC showed steadier price movement, while ETH had higher liquidity and higher volatility.

---

## Repository Structure

```
binance-pipeline/
│── dlt_pipeline/
│   ├── binance_pipeline.py
│   ├── resources/
│   └── settings.toml
│
│── analysis/
│   ├── binance_analysis.ipynb
│   └── exported_data.csv
│
│── reports/
│   └── Binance_Pipeline_Analysis.pdf
│
└── README.md
```

---

## Conclusion

BTC displayed lower volatility and smoother price action.  
ETH exhibited higher overall trading activity and stronger short-term fluctuations.

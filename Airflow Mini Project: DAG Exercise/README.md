# Market Volatility DAG (marketvol)

Apache Airflow pipeline for downloading and analyzing AAPL and TSLA stock data with 1-minute intervals.

**Schedule:** Daily at 6:00 PM, weekdays only (Mon-Fri)

---

## Quick Start

### 1. Install Dependencies

```bash
pip install apache-airflow yfinance pandas celery redis
brew install redis  # macOS, or: sudo apt-get install redis-server
brew services start redis
```

### 2. Initialize Airflow

```bash
export AIRFLOW_HOME=~/airflow
airflow db init
airflow users create --username admin --password admin --role Admin --email admin@example.com --firstname Admin --lastname User
```

### 3. Configure Celery Executor

Edit `$AIRFLOW_HOME/airflow.cfg`:

```ini
[core]
executor = CeleryExecutor

[celery]
broker_url = redis://localhost:6379/0
result_backend = db+postgresql://airflow:airflow@localhost/airflow
```

### 4. Deploy DAG

```bash
cp marketvol.py $AIRFLOW_HOME/dags/
airflow dags list | grep marketvol  # Verify
```

### 5. Start Airflow (3 Separate Terminals)

```bash
# Terminal 1
airflow webserver --port 8080

# Terminal 2
airflow scheduler

# Terminal 3
airflow celery worker
```

### 6. Run the DAG

1. Open http://localhost:8080 (login: admin/admin)
2. Toggle `marketvol` DAG to **ON**
3. Click the **Play** button to trigger manually

---

## Verify Results

### Check Files
```bash
# View downloaded data
ls -la /tmp/data/*/query/

# Expected files: AAPL.csv, TSLA.csv, analysis_report.txt
cat /tmp/data/*/query/analysis_report.txt
```

### Check Task Status
- **UI:** All tasks should be green (success)
- **CLI:** `airflow dags list-runs -d marketvol`

### View Analysis Output
```bash
# View task logs
airflow tasks logs marketvol run_query <execution_date>

# Example output in logs:
# MARKET VOLATILITY ANALYSIS REPORT
# 1. BASIC STATISTICS BY SYMBOL
# 2. VOLATILITY ANALYSIS
# etc.
```

---

## Troubleshooting

| Issue | Solution |
|-------|----------|
| DAG not appearing | `python $AIRFLOW_HOME/dags/marketvol.py` to check syntax |
| Tasks stuck in "queued" | Verify Celery worker is running, check `redis-cli ping` |
| No data retrieved | Market data only available during trading hours (9:30 AM - 4 PM ET) |
| "mv: cannot stat" error | Add `mkdir -p` before `mv` in tasks t3/t4 |

---

## Task Flow

```
        t0 (Create Directory)
       /  \
     t1    t2  (Download AAPL & TSLA)
      |     |
     t3    t4  (Move to query location)
       \  /
        t5 (Run Analysis)
```

---

## Running for Multiple Days

**Required:** Run for at least 2 days

**Option 1 (Recommended):** Keep all terminals running, DAG runs automatically at 6 PM weekdays

**Option 2:** Manual trigger: `airflow dags trigger marketvol` (repeat next day)

**Save scheduler logs for next project:**
```bash
cp -r $AIRFLOW_HOME/logs/scheduler/ ~/scheduler_logs_backup/
```

---

## Project Structure

```
/tmp/data/
├── 2024-01-15/query/
│   ├── AAPL.csv
│   ├── TSLA.csv
│   └── analysis_report.txt
└── 2024-01-16/query/
    ├── AAPL.csv
    ├── TSLA.csv
    └── analysis_report.txt
```
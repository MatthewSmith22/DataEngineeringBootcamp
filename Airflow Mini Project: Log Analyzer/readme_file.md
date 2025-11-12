# Airflow Log Analyzer

A Python script to analyze Airflow log files and extract error messages from the `marketvol` DAG.

## Overview

This is a standalone command-line script that monitors your marketvol DAG by:
- Scanning all log files in the marketvol directory
- Counting total ERROR messages
- Displaying each error with details

## Prerequisites

- Python 3.6+
- Airflow with the marketvol DAG already running
- Access to `~/airflow/logs/marketvol/`

## Installation

```bash
git clone <your-repo-url>
cd airflow-log-analyzer
chmod +x log_analyzer.py
```

## Usage

```bash
python3 log_analyzer.py ~/airflow/logs/marketvol
```

**Capture output for submission:**
```bash
python3 log_analyzer.py ~/airflow/logs/marketvol > execution_log.txt
```

## Expected Output

```
Total number of errors: 6
Here are all the errors:

[2020-09-27 20:12:59,742] {taskinstance.py:1150} ERROR - No columns to parse from file
[2020-09-27 20:00:41,364] {taskinstance.py:1150} ERROR - No columns to parse from file
[2020-09-26 20:20:46,692] {taskinstance.py:1150} ERROR - No columns to parse from file
[2020-09-26 20:15:33,479] {taskinstance.py:1150} ERROR - No columns to parse from file
[2020-10-03 20:06:04,487] {taskinstance.py:1150} ERROR - No columns to parse from file
[2020-10-03 20:00:51,254] {taskinstance.py:1150} ERROR - No columns to parse from file
```

## How It Works

1. Uses `pathlib` to recursively find all `.log` files:
   ```python
   file_list = Path(log_dir).rglob('*.log')
   ```

2. Calls `analyze_file()` function for each log file:
   ```python
   count, cur_list = analyze_file(file)
   ```

3. Aggregates and prints total error count and messages

## Troubleshooting

**No log files found:**
- Ensure marketvol DAG has run at least once
- Check path: `ls -l ~/airflow/logs/marketvol/`

**Directory does not exist:**
- Verify Airflow is installed: `airflow version`
- Check config: `cat ~/airflow/airflow.cfg | grep base_log_folder`

**Permission denied:**
```bash
chmod -R +r ~/airflow/logs/marketvol/
```

## Verifying Your Setup

Before running the analyzer:

1. **Check your marketvol DAG has run:**
   ```bash
   airflow dags list | grep marketvol
   ```

2. **Verify log directory exists:**
   ```bash
   ls -l ~/airflow/logs/marketvol/
   ```
   
   You should see directories like:
   - `python_download_AAPL/`
   - `python_download_TSLA/`
   - `copy_AAPL/`
   - `copy_TSLA/`

3. **Run the analyzer:**
   ```bash
   python3 log_analyzer.py ~/airflow/logs/marketvol
   ```

## Project Structure

```
.
├── log_analyzer.py       # Main script
├── README.md             # This file
├── run_analyzer.sh       # Optional helper script
└── execution_log.txt     # Output (generated after running)
```

## Submission

1. Push code to GitHub
2. Include this README
3. Attach `execution_log.txt` with the command output

## Author

Created as part of the Airflow Mini Project - Log Analyzer
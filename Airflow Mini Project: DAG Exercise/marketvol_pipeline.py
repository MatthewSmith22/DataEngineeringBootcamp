from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.bash import BashOperator
from datetime import datetime, timedelta, date
import yfinance as yf
import pandas as pd

default_args = {
    'owner': 'airflow',
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2025, 10, 26, 18, 0, 0),
}

dag = DAG(
    'marketvol',
    default_args=default_args,
    description='Stock market data pipeline',
    schedule='0 18 * * 1-5',
    catchup=False,
)

def download_stock(symbol, **context):
    exec_date = context['ds']
    start_date = date.today()
    end_date = start_date + timedelta(days=1)
    df = yf.download(symbol, start=start_date, end=end_date, interval='1m')
    df.to_csv(f'/tmp/data/{exec_date}/{symbol}.csv', header=False)
    print(f'Downloaded {symbol}')

def run_query(**context):
    exec_date = context['ds']
    aapl = pd.read_csv(f'/tmp/hdfs/{exec_date}/AAPL.csv')
    tsla = pd.read_csv(f'/tmp/hdfs/{exec_date}/TSLA.csv')
    print(f'AAPL avg close: {aapl.iloc[:, 4].mean()}')
    print(f'TSLA avg close: {tsla.iloc[:, 4].mean()}')

t0 = BashOperator(
    task_id='init_dir',
    bash_command='mkdir -p /tmp/data/{{ ds }}',
    dag=dag,
)

t1 = PythonOperator(
    task_id='download_aapl',
    python_callable=download_stock,
    op_kwargs={'symbol': 'AAPL'},
    dag=dag,
)

t2 = PythonOperator(
    task_id='download_tsla',
    python_callable=download_stock,
    op_kwargs={'symbol': 'TSLA'},
    dag=dag,
)

t3 = BashOperator(
    task_id='move_aapl',
    bash_command='mkdir -p /tmp/hdfs/{{ ds }} && mv /tmp/data/{{ ds }}/AAPL.csv /tmp/hdfs/{{ ds }}/',
    dag=dag,
)

t4 = BashOperator(
    task_id='move_tsla',
    bash_command='mkdir -p /tmp/hdfs/{{ ds }} && mv /tmp/data/{{ ds }}/TSLA.csv /tmp/hdfs/{{ ds }}/',
    dag=dag,
)

t5 = PythonOperator(
    task_id='run_query',
    python_callable=run_query,
    dag=dag,
)

# Set dependencies
t0 >> [t1, t2]
t1 >> t3
t2 >> t4
[t3, t4] >> t5
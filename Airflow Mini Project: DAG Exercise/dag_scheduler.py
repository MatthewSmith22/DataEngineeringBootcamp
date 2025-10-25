from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta, date
import yfinance as yf
import pandas as pd

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime.combine(date.today(), datetime.strptime('18:00', '%H:%M').time()),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

# Create the DAG
dag = DAG(
    'marketvol',
    default_args=default_args,
    description='Market volatility data pipeline',
    schedule=timedelta(days=1),
    catchup=False,
)

# Task 0: Create temporary directory for data download
t0 = BashOperator(
    task_id='create_temp_directory',
    bash_command='mkdir -p /tmp/data/{{ ds }}',
    dag=dag,
)

# Function to download stock data
def download_stock_data(symbol, **context):
    """
    Download stock data for the given symbol with 1-minute interval.
    """
    execution_date = context['ds']  # Get execution date in YYYY-MM-DD format
    
    # Set date range for download
    start_date = date.today()
    end_date = start_date + timedelta(days=1)
    
    # Download data with 1-minute interval
    stock_df = yf.download(symbol, start=start_date, end=end_date, interval='1m')
    
    # Save to CSV without header
    output_path = f'/tmp/data/{execution_date}/{symbol}.csv'
    stock_df.to_csv(output_path, header=False)
    
    print(f"Downloaded {len(stock_df)} records for {symbol}")
    print(f"Saved to: {output_path}")
    
    return output_path

# Task 1: Download AAPL data
t1 = PythonOperator(
    task_id='download_AAPL',
    python_callable=download_stock_data,
    op_kwargs={'symbol': 'AAPL'},
    dag=dag,
)

# Task 2: Download TSLA data
t2 = PythonOperator(
    task_id='download_TSLA',
    python_callable=download_stock_data,
    op_kwargs={'symbol': 'TSLA'},
    dag=dag,
)

# Task 3: Move AAPL data to query location
t3 = BashOperator(
    task_id='move_AAPL_to_query_location',
    bash_command='mv /tmp/data/{{ ds }}/AAPL.csv /tmp/data/{{ ds }}/query/',
    dag=dag,
)

# Task 4: Move TSLA data to query location
t4 = BashOperator(
    task_id='move_TSLA_to_query_location',
    bash_command='mv /tmp/data/{{ ds }}/TSLA.csv /tmp/data/{{ ds }}/query/',
    dag=dag,
)

# Function to run query on both data files
def run_query(**context):
    """
    Run analytics query on both AAPL and TSLA data files.
    """
    execution_date = context['ds']
    data_dir = f'/tmp/data/{execution_date}/query'
    
    # Read both CSV files
    aapl_df = pd.read_csv(f'{data_dir}/AAPL.csv', 
                          names=['Datetime', 'Open', 'High', 'Low', 'Close', 'Volume'])
    tsla_df = pd.read_csv(f'{data_dir}/TSLA.csv',
                          names=['Datetime', 'Open', 'High', 'Low', 'Close', 'Volume'])
    
    # Add symbol column
    aapl_df['Symbol'] = 'AAPL'
    tsla_df['Symbol'] = 'TSLA'
    
    # Combine datasets
    combined_df = pd.concat([aapl_df, tsla_df], ignore_index=True)
    
    print("\n" + "="*60)
    print("MARKET VOLATILITY ANALYSIS REPORT")
    print(f"Execution Date: {execution_date}")
    print("="*60)
    
    # Query 1: Basic Statistics
    print("\n1. BASIC STATISTICS BY SYMBOL:")
    print("-" * 60)
    for symbol in ['AAPL', 'TSLA']:
        symbol_data = combined_df[combined_df['Symbol'] == symbol]
        print(f"\n{symbol}:")
        print(f"  Total Records: {len(symbol_data)}")
        print(f"  Opening Price: ${symbol_data['Open'].iloc[0]:.2f}")
        print(f"  Closing Price: ${symbol_data['Close'].iloc[-1]:.2f}")
        print(f"  Highest Price: ${symbol_data['High'].max():.2f}")
        print(f"  Lowest Price: ${symbol_data['Low'].min():.2f}")
        print(f"  Average Volume: {symbol_data['Volume'].mean():.0f}")
    
    # Query 2: Volatility Calculation
    print("\n2. VOLATILITY ANALYSIS:")
    print("-" * 60)
    for symbol in ['AAPL', 'TSLA']:
        symbol_data = combined_df[combined_df['Symbol'] == symbol].copy()
        
        # Calculate returns
        symbol_data['Returns'] = symbol_data['Close'].pct_change()
        
        # Calculate volatility (standard deviation of returns)
        volatility = symbol_data['Returns'].std()
        volatility_pct = volatility * 100
        
        # Calculate price range
        price_range = symbol_data['High'].max() - symbol_data['Low'].min()
        price_range_pct = (price_range / symbol_data['Open'].iloc[0]) * 100
        
        print(f"{symbol}:")
        print(f"  Volatility (Std Dev): {volatility_pct:.4f}%")
        print(f"  Price Range: ${price_range:.2f} ({price_range_pct:.2f}%)")
    
    # Query 3: Price Change Analysis
    print("\n3. PRICE CHANGE ANALYSIS:")
    print("-" * 60)
    for symbol in ['AAPL', 'TSLA']:
        symbol_data = combined_df[combined_df['Symbol'] == symbol]
        open_price = symbol_data['Open'].iloc[0]
        close_price = symbol_data['Close'].iloc[-1]
        price_change = close_price - open_price
        price_change_pct = (price_change / open_price) * 100
        
        print(f"{symbol}:")
        print(f"  Price Change: ${price_change:.2f}")
        print(f"  Percentage Change: {price_change_pct:.2f}%")
        print(f"  Direction: {'📈 UP' if price_change > 0 else '📉 DOWN' if price_change < 0 else '➡️ FLAT'}")
    
    # Query 4: Volume Analysis
    print("\n4. VOLUME ANALYSIS:")
    print("-" * 60)
    volume_stats = combined_df.groupby('Symbol')['Volume'].agg(['sum', 'mean', 'max', 'min'])
    print(volume_stats)
    
    # Query 5: Comparison Summary
    print("\n5. COMPARATIVE SUMMARY:")
    print("-" * 60)
    aapl_volatility = aapl_df['Close'].pct_change().std() * 100
    tsla_volatility = tsla_df['Close'].pct_change().std() * 100
    
    more_volatile = 'TSLA' if tsla_volatility > aapl_volatility else 'AAPL'
    print(f"More Volatile Stock: {more_volatile}")
    
    aapl_vol_total = aapl_df['Volume'].sum()
    tsla_vol_total = tsla_df['Volume'].sum()
    higher_volume = 'TSLA' if tsla_vol_total > aapl_vol_total else 'AAPL'
    print(f"Higher Trading Volume: {higher_volume}")
    
    print("\n" + "="*60)
    print("ANALYSIS COMPLETED SUCCESSFULLY")
    print("="*60 + "\n")
    
    # Save report to file
    report_path = f'/tmp/data/{execution_date}/query/analysis_report.txt'
    with open(report_path, 'w') as f:
        f.write(f"Market Volatility Analysis Report\n")
        f.write(f"Execution Date: {execution_date}\n")
        f.write(f"Generated: {datetime.now()}\n")
        f.write(f"\nReport saved successfully.\n")
    
    print(f"Report saved to: {report_path}")
    
    return "Query executed successfully"

# Task 5: Run query on both data files
t5 = PythonOperator(
    task_id='run_query',
    python_callable=run_query,
    dag=dag,
)

# Set job dependencies
# t1 and t2 must run only after t0
t0 >> t1
t0 >> t2

# t3 must run after t1
t1 >> t3

# t4 must run after t2
t2 >> t4

# t5 must run after both t3 and t4 are complete
[t3, t4] >> t5
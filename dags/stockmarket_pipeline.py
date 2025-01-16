from airflow import DAG
from datetime import  datetime, timedelta
from airflow.operators.python import PythonOperator

from airflow.hooks.base import BaseHook


import json

default_args = {
    "owner": "nonso",
    "retries": 5,
    "retry_delay": timedelta(minutes=2)
}

import os
import json
import requests
from typing import Dict, Optional
from dotenv import load_dotenv
from pathlib import Path

def fetch_stock_data() -> Optional[Dict]:
    """
    Fetch stock data from Alpha Vantage API and save to JSON file.
    Uses default values: AAPL stock with 5-minute intervals.
    Saves to dags/stock_data.json by default.
    
    Returns:
        Dict containing the API response data or None if the request fails
    """
    # Load environment variables from .env file
    load_dotenv()
    
    # Get API key from environment variable
    api_key = os.getenv('ALPHA_VANTAGE_API_KEY')
    if not api_key:
        raise ValueError(
            "API key not found. Please set ALPHA_VANTAGE_API_KEY in your .env file"
        )
    
    # Construct API URL with default parameters
    base_url = 'https://www.alphavantage.co/query'
    params = {
        'function': 'TIME_SERIES_INTRADAY',
        'symbol': 'AAPL',
        'interval': '5min',
        'apikey': api_key
    }
    
    try:
        # Make API request
        response = requests.get(base_url, params=params)
        response.raise_for_status()
        
        data = response.json()
        
        # Save to default location
        output_path = Path("dags/stock_data.json")
        output_path.parent.mkdir(parents=True, exist_ok=True)
        
        with open(output_path, 'w') as f:
            json.dump(data, f, indent=2)
                
        return data
        
    except requests.RequestException as e:
        print(f"Error fetching data: {str(e)}")
        return None
fetch_stock_data()



def transform_stock_data(input_file: str, output_file: str):
    
    import pandas as pd
    """
    Cleans raw stock data and calculates financial metrics.
    """
    input_file = 'dags/stock_data.json'
    output_file = 'dags/stock_data.csv'
    try:
        # Load raw data from the JSON file
        with open(input_file, 'r') as f:
            raw_data = json.load(f)
        
        # Extract time series data
        time_series = raw_data.get('Time Series (5min)', {})
        if not time_series:
            raise ValueError("Time series data is missing or invalid.")
        
        # Convert time series to a DataFrame
        df = pd.DataFrame.from_dict(time_series, orient='index')
        df.index = pd.to_datetime(df.index)
        df.sort_index(inplace=True)

        # Rename columns and process data as before
        df.rename(columns={
            '1. open': 'open',
            '2. high': 'high',
            '3. low': 'low',
            '4. close': 'close',
            '5. volume': 'volume'
        }, inplace=True)
        df = df.apply(pd.to_numeric, errors='coerce')
        df['moving_avg_5'] = df['close'].rolling(window=5).mean()
        df['moving_avg_10'] = df['close'].rolling(window=10).mean()
        df['volatility'] = df['close'].pct_change().rolling(window=5).std()
        df.dropna(inplace=True)

        # Save the processed data to a CSV file
        df.to_csv(output_file, index=True)
        print(f"Transformed data saved to {output_file}")
    except FileNotFoundError:
        print(f"Input file not found: {input_file}")
    except ValueError as e:
        print(f"Data transformation error: {e}")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
transform_stock_data('dags/stock_data.json', 'dags/stock_data.csv')


def load_to_postgresql(csv_file: str, table_name: str, max_retries: int = 3):
    """
    Loads transformed stock data into a PostgreSQL table with secure credentials and error handling.

    Args:
        csv_file (str): Path to the transformed CSV file.
        table_name (str): Target table name.
        max_retries (int): Number of retries for database insertions.
    """
    # Retrieve database credentials from Airflow connection
    import os
    import pandas as pd
    import psycopg2
    from psycopg2 import OperationalError, errors
    from airflow.hooks.base import BaseHook
    
    conn_id = "postgres_default"  # Use the connection ID defined in Airflow
    conn_details = BaseHook.get_connection(conn_id)

    db_name = conn_details.schema
    user = conn_details.login
    password = conn_details.password
    host = conn_details.host
    port = conn_details.port

    # Load transformed data into a DataFrame
    df = pd.read_csv(csv_file)

    # Establish database connection
    try:
        conn = psycopg2.connect(
            dbname=db_name,
            user=user,
            password=password,
            host=host,
            port=port
        )
        cursor = conn.cursor()

        # Create table if it doesn't exist
        cursor.execute(f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                timestamp TIMESTAMP PRIMARY KEY,
                open NUMERIC,
                high NUMERIC,
                low NUMERIC,
                close NUMERIC,
                volume NUMERIC,
                moving_avg_5 NUMERIC,
                moving_avg_10 NUMERIC,
                volatility NUMERIC
            );
        """)
        conn.commit()

        # Insert data with retries
        for _, row in df.iterrows():
            retry_count = 0
            while retry_count < max_retries:
                try:
                    cursor.execute(f"""
                        INSERT INTO {table_name} (timestamp, open, high, low, close, volume, moving_avg_5, moving_avg_10, volatility)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (timestamp) DO NOTHING;
                    """, tuple(row))
                    conn.commit()
                    break  # Exit retry loop on success
                except (OperationalError, errors.DatabaseError) as e:
                    retry_count += 1
                    if retry_count == max_retries:
                        print(f"Failed to insert row after {max_retries} retries. Error: {e}")
                        continue  # Log and continue with other rows
                    print(f"Retrying insert... Attempt {retry_count}/{max_retries}")

    except Exception as e:
        print(f"Error connecting to PostgreSQL: {e}")
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()
        print(f"Data loading process completed for {table_name}.")
        
def run_load_to_postgresql(**kwargs):
    load_to_postgresql(
        csv_file='dags/stock_data.csv',
        table_name='stock_metrics'
    )



        


with DAG(
    default_args=default_args,
    dag_id="dag_stock_market_etl_pipeline_v1",
    start_date=datetime(2025, 1, 16),
    description="This is a stockmarket ETL pipeline",
    schedule_interval="0 0 * * *"
    
) as dag:
    fetch_data = PythonOperator(
        task_id='fetch_data',
        python_callable=fetch_stock_data
    )
    
    transform_data = PythonOperator(
        task_id='transform_data',
        python_callable=transform_stock_data,
        op_kwargs={'input_file': 'dags/stock_data.json', 'output_file': 'dags/stock_data.csv'},
        dag=dag
    )
    

    

    
    load_data = PythonOperator(
        task_id='load_data',
        python_callable=load_to_postgresql,
        op_kwargs={'csv_file': 'dags/stock_data.csv', 'table_name': 'stock_metrics'},
        dag=dag
    )
    


    fetch_data >> transform_data >> load_data
    

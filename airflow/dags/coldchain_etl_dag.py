import pandas as pd
from sqlalchemy import create_engine
from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python_operator import PythonOperator
# from coldchain_etl_pipline.scripts.my_order_generator import generate_order_logs
# from coldchain_etl_pipline.scripts.transformation import transform_order_logs


def load_data_to_db(**kwargs):
    # Getting credentials from Airflow
    print("Getting connection credentials...")
    try:
        connection_uri = f"postgresql+psycopg2://airflow:airflow@postgres:5432/order_db"
        
        engine = create_engine(connection_uri)
        
    
    except Exception as e:
        print("An error occured: {}".format(e))
    
    else:
        print("Connection successfull!!!")

    file_path = f"/opt/airflow/dags/coldchain_etl_pipline/data/raw/orderlogs_{kwargs.get('ds')}.csv"

    # Initialize DataFrame
    print("Initializing DataFrame...")
    
    try:
        df = pd.read_csv(file_path, parse_dates=["order_date"])
    
    except Exception as e:
        print("An error occured: {}".format(e))
    
    else:
        print("Pushing to DB...")
    
        try:
        # Push to Database
            df.to_sql("orders", con=engine, if_exists="append", index=False)
    
        except Exception as e:
            print("An error occured: {}".format(e))
    
        else:
            print("{} records pushed to database successfull".format(len(df)))



with DAG (dag_id="coldchain-etl", start_date=datetime(2026, 8, 31), schedule=timedelta(minutes=5), \
          catchup=False, description="Simulating an ETL pipeline...") as dag:
    
    # extract = PythonOperator(
    #     task_id = "Generating_data",
    #     python_callable=generate_order_logs
    # )

    # transform_logs = PythonOperator(
    #     task_id="Processing_data",
    #     python_callable=transform_order_logs
    #)

    loading_data_to_db = PythonOperator(
        task_id = "loading_to_db",
        python_callable = load_data_to_db 
    )


    # extract >> transform_logs >> 
    loading_data_to_db
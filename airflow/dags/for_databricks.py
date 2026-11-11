import random
from datetime import datetime, timedelta
import pandas as pd
from faker import Faker
from rich import print

from airflow import DAG
from airflow.decorators import task
from airflow.operators.python_operator import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

PRODUCTS: dict = {
        1: {"name":"Meat Pie", "price": 4.50},
        2: {"name":"Chicken Pie", "price": 6.50},
        3: {"name":"Doughnut", "price": 3.50},
        4: {"name":"Sausage Roll", "price":4.50},
        5: {"name":"Shawarma", "price": 5.50},
        6: {"name":"Pudding", "price": 2.50},
        7: {"name":"Ice Cream", "price": 3.50},
        8: {"name":"Tiramisu", "price": 4.50},
        9: {"name":"Strawberry Cake", "price": 7.00},
        10: {"name":"Chocolate Milk", "price": 2.50},
        11: {"name":"Sparkling Water", "price": 3.50},
        12: {"name":"Orange Juice", "price": 4.50},
        13: {"name":"Milk Tea", "price": 5.50},
        14: {"name":"Coffee", "price": 6.50}
}


def generate_order_logs(istore, ds=None, no_of_records: int = 1000, error_rate: float = 0.12):
    base_date = datetime.now()
    
    fake = Faker()

    data = []

    seconds = 45    
    for i in range(no_of_records):


        base_date = base_date.replace(hour=8, minute=0, second=0)
        
        to_add = random.randint(43, 45)

        order_date = base_date + timedelta(seconds=seconds)
        base_date = base_date + timedelta(seconds=seconds)
        
        seconds += to_add

        order_id = fake.bothify(text = 'ORD-####-???')
        store_id = istore
        product_id = random.randint(1, 14)
        quantity = random.randint(2, 20) 
        payment_method = random.choice(["Cash", "Bank Transfer", "POS Payment"])
        

        if random.random() < error_rate:
            corruption_type = random.choice(["null_id", "bad_total", "null_payment"])
            
            if corruption_type == "null_id":
                order_id = None
            
            elif corruption_type == "bad_total":
                quantity = None
            
            elif corruption_type == "null_payment":
                payment_method = None

        logs: dict = {
            "order_date":order_date,
            "order_id": order_id,
            "store_id": store_id,
            "product_id": product_id,
            "quantity": quantity,
            "payment_method": payment_method
        }

        data.append(logs)

    df = pd.DataFrame(data)

    stores = {
        1 : "lagos",
        2: "ibadan",
        3: "kano",
        4: "portharcout",
        5: "abuja"
    }

    location = stores.get(istore, "unknown")

    filepath = f"raw/{location}/sales_{base_date.strftime("%Y_%m_%d")}.csv"

    try:
        file = df.to_csv(index=False)

        s3hook = S3Hook(aws_conn_id='for_databricks')
        
        s3hook.load_string(
            string_data=file,
            key=filepath,
            bucket_name='riays-databricks',
            replace=True
        )

        print("Generation complete for {}! No. of Records: {}.".format(istore, len(data)))
    
    except Exception as e:
        print("❌ An error occured: {}".format(e))
    
    else:    
        print("✅ Data dump for Store {} completed successfully!!!".format(istore))





with DAG (dag_id="Riays-Store-DataDump", start_date=datetime(2026, 8, 31), schedule='0 6 * * *', \
          catchup=False, description="Loading Daily Sales Data into S3", tags=['source', 'aws']) as dag:

          lagos_data_dump = PythonOperator(
                task_id = "lagos_data_dump",
                python_callable = generate_order_logs,
                op_kwargs = {"istore": 1}
          )

          ibadan_data_dump = PythonOperator(
                task_id = "ibadan_data_dump",
                python_callable = generate_order_logs,
                op_kwargs = {"istore": 2}
          )

          kano_data_dump = PythonOperator(
                task_id = "kano_data_dump",
                python_callable = generate_order_logs,
                op_kwargs = {"istore": 3}
          )

          portharcout_data_dump = PythonOperator(
                task_id = "portharcout_data_dump",
                python_callable = generate_order_logs,
                op_kwargs = {"istore": 4}
          )

          abuja_data_dump = PythonOperator(
                task_id = "abuja_data_dump",
                python_callable = generate_order_logs,
                op_kwargs = {"istore": 5}
          )

          lagos_data_dump >> ibadan_data_dump >> kano_data_dump >> portharcout_data_dump >> abuja_data_dump

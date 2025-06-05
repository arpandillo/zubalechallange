from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import psycopg2
import csv
import os

default_args = {
    'owner': 'airflow',
    'retries': 2,
    'retry_delay': timedelta(minutes=1),
}

def load_data(**context):
    # Get partition date from Airflow DAG logical execution date
    partition_date = context['logical_date'].date()

    conn = psycopg2.connect(
        dbname="airflow",
        user="airflow",
        password="airflow",
        host="postgres"
    )
    cur = conn.cursor()

    # Create tables (products has no partition_date, orders does)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS products (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            category TEXT NOT NULL,
            price NUMERIC NOT NULL
        );
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS orders (
            id INTEGER PRIMARY KEY,
            product_id INTEGER REFERENCES products(id),
            quantity INTEGER NOT NULL,
            created_date DATE NOT NULL,
            partition_date DATE NOT NULL
        );
    """)

    # Load products
    products_path = 'data/products.csv'
    if os.path.exists(products_path):
        with open(products_path, newline='') as f:
            reader = csv.DictReader(f)
            for row in reader:
                cur.execute("""
                    INSERT INTO products (id, name, category, price)
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT (id) DO NOTHING;
                """, (
                    int(row['id']),
                    row['name'],
                    row['category'],
                    float(row['price'])
                ))

    # Load orders and inject partition_date dynamically
    orders_path = 'data/orders.csv'
    if os.path.exists(orders_path):
        with open(orders_path, newline='') as f:
            reader = csv.DictReader(f)
            for row in reader:
                cur.execute("""
                    INSERT INTO orders (id, product_id, quantity, created_date, partition_date)
                    VALUES (%s, %s, %s, %s, %s)
                    ON CONFLICT (id) DO NOTHING;
                """, (
                    int(row['id']),
                    int(row['product_id']),
                    int(row['quantity']),
                    row['created_date'],
                    partition_date
                ))

    conn.commit()
    cur.close()
    conn.close()

# Define the DAG
with DAG(
    dag_id='load_csv_partitioned_dag',
    default_args=default_args,
    start_date=datetime(2025, 1, 1),
    schedule_interval='*/5 * * * *',
    catchup=False
) as dag:

    load_task = PythonOperator(
        task_id='load_csv_with_partition_date',
        python_callable=load_data,
        provide_context=True
    )

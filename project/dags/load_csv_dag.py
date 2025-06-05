from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import psycopg2
import csv

default_args = {
    'owner': 'airflow',
    'retries': 2,
    'retry_delay': timedelta(minutes=1),
}

def load_data():
    conn = psycopg2.connect(
        dbname="airflow",
        user="airflow",
        password="airflow",
        host="postgres"
    )
    cur = conn.cursor()

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
            created_date DATE NOT NULL
        );
    """)

    with open('/opt/airflow/data/products.csv', newline='') as f:
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
    with open('/opt/airflow/data/orders.csv', newline='') as f:
        reader = csv.DictReader(f)
        for row in reader:
            cur.execute("""
                INSERT INTO orders (id, product_id, quantity, created_date)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (id) DO NOTHING;
            """, (
                int(row['id']),
                int(row['product_id']),
                int(row['quantity']),
                row['created_date']
            ))


    conn.commit()
    cur.close()
    conn.close()

with DAG(
    dag_id='load_csv_to_postgres',
    default_args=default_args,
    start_date=datetime(2025, 1, 1),
    schedule_interval='*/5 * * * *',  # Every 5 minutes
    catchup=False
) as dag:
    load_task = PythonOperator(
        task_id='load_csv_data',
        python_callable=load_data
    )

    load_task

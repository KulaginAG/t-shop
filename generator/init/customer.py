import numpy as np
import pandas as pd
from faker import Faker
from tqdm import tqdm
import psycopg2
import psycopg2.extras
from psycopg2.extensions import register_adapter, AsIs
from concurrent.futures import ThreadPoolExecutor
import keyring
from dotenv import load_dotenv
import os

register_adapter(np.int64, AsIs)
register_adapter(np.float64, AsIs)
load_dotenv()


def db_connection():
    db_host = os.getenv("DB_HOST_PG")
    db_port = os.getenv("DB_PORT_PG")
    db_user = os.getenv("DB_USER_PG")

    return psycopg2.connect(
        database='postgres',
        user=db_user,
        password=f"{keyring.get_password('PostgreSQL', db_user)}",
        host=db_host,
        port=db_port,
    )


# Function for inserting a data package into PostgreSQL
def insert_batch(batch):
    try:
        conn = db_connection()
        cursor = conn.cursor()
        insert_query = '''INSERT INTO customer (first_name, last_name, birth_dt) VALUES %s;'''
        psycopg2.extras.execute_values(cursor, insert_query, batch)
        conn.commit()
    except (Exception, psycopg2.Error) as error:
        print("Error inserting batch into PostgreSQL", error)
    finally:
        # Closing the database connection
        if conn:
            cursor.close()
            conn.close()


def generate_data(num_records):
    print('Data generation has started\n')
    data = {
        'first_name': [fake.first_name() for _ in range(num_records)],
        'last_name': [fake.last_name() for _ in range(num_records)],
        'birth_dt': [fake.date_of_birth(minimum_age=18, maximum_age=60) for _ in range(num_records)]
    }
    return pd.DataFrame(data)


try:
    conn = db_connection()
    cursor = conn.cursor()

    create_table_query = '''
    DROP TABLE IF EXISTS public.customer;
    CREATE TABLE IF NOT EXISTS public.customer
    (
        id serial PRIMARY KEY,
        first_name varchar(100) DEFAULT NULL,
        last_name varchar(100) DEFAULT NULL,
        birth_dt date DEFAULT NULL,
        record_dttm timestamp(3) without time zone NOT NULL DEFAULT NOW()
    )

    TABLESPACE pg_default;

    ALTER TABLE IF EXISTS public.product OWNER to postgres;
    '''

    cursor.execute(create_table_query)
    conn.commit()
except (Exception, psycopg2.Error) as error:
    print("Error inserting data into PostgreSQL", error)
finally:
    if conn:
        cursor.close()
        conn.close()

# Initializing Faker
fake = Faker()

# Number of customers
customer_count = 5_000_000

# Data generation
customer = generate_data(customer_count)
# Converting a DataFrame to a list of tuples
records = customer.values.tolist()

batch_size = 100_000
# Partitioning data into batches
batches = [records[i:i + batch_size] for i in range(0, len(records), batch_size)]
# Number of threads per insertion
num_threads = 32

# Inserting data using multithreading
print('Insert data started')
with ThreadPoolExecutor(max_workers=num_threads) as executor:
    list(tqdm(executor.map(insert_batch, batches), total=len(batches)))

print(f"All rows inserted successfully into PostgreSQL")

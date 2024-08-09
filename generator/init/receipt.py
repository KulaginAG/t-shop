import numpy as np
import pandas as pd
from datetime import datetime, timedelta
import random
import psycopg2
import psycopg2.extras
from psycopg2.extensions import register_adapter, AsIs
from concurrent.futures import ThreadPoolExecutor
from uuid import uuid4
import keyring
from time import sleep
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


def get_count(table):
    query = f"""
             SELECT MAX(id)
             FROM {table}
             """

    try:
        conn = db_connection()
        cursor = conn.cursor()
        cursor.execute(query)
        result = cursor.fetchone()

    except (Exception, psycopg2.Error) as error:
        print("Error:", error)

    finally:
        # Закрытие соединения с базой данных
        if conn:
            cursor.close()
            conn.close()

    return result[0]


def get_ids(table):
    query = f"""
             SELECT id
             FROM {table}
             """
    try:
        conn = db_connection()
        cursor = conn.cursor()
        cursor.execute(query)
        result = [row[0] for row in cursor.fetchall()]
    except (Exception, psycopg2.Error) as error:
        print("Error:", error)
    finally:
        # Закрытие соединения с базой данных
        if conn:
            cursor.close()
            conn.close()

    return result


# Function to generate date and time with normal distribution
def generate_datetime(mean_time, std_dev, date):
    mean_datetime = datetime.combine(date, mean_time)
    seconds_from_mean = np.random.normal(loc=0, scale=std_dev * 60 * 60)
    generated_datetime = mean_datetime + timedelta(seconds=seconds_from_mean)
    return generated_datetime


# Function for generating data based on customer purchases
def generate_receipt(total_customers, p_purchase, start_date, end_date, mean_weekday_time, mean_weekend_time, std_dev,
                     customer_ids, product_ids, size_ids):
    current_date = start_date
    receipts = []
    receipts_item = []

    while current_date < end_date:
        print('processing date:', current_date.strftime('%Y-%m-%d %H:%M:%S'))
        if current_date.weekday() + 1 < 5:  # Weekday
            current_p_purchase = p_purchase * (random.randint(50, 150) / 100)
            mean_time = mean_weekday_time
        else:  # Weekend
            current_p_purchase = p_purchase * (random.randint(150, 300) / 100)
            mean_time = mean_weekend_time

        num_purchases = np.random.binomial(total_customers, current_p_purchase)

        for _ in range(num_purchases):
            customer_id = random.choice(customer_ids)
            receipt_id = str(uuid4())
            receipt_dttm = generate_datetime(mean_time, std_dev, current_date)

            # Generating data for sale
            receipts.append([receipt_id, customer_id, receipt_dttm])

            # Generate receipt item data for the current sale
            shape = 2.0
            scale = 0.55
            num_items = min(int(np.ceil(np.random.gamma(shape, scale))), 20)

            for _ in range(num_items):
                product_id = random.choice(product_ids)
                size_id = random.choice(size_ids)
                quantity = min(int(np.ceil(np.random.gamma(shape, scale))), 50)

                receipts_item.append([receipt_id, product_id, size_id, quantity])

        current_date += timedelta(days=1)

    df_receipts = pd.DataFrame(receipts, columns=['id', 'customer_id', 'receipt_dttm'])
    df_receipts_item = pd.DataFrame(receipts_item, columns=['receipt_id', 'product_id', 'size_id', 'quantity'])

    return df_receipts, df_receipts_item


def generate_and_insert_data(date_range, total_customers, p_purchase, mean_weekday_time, mean_weekend_time, std_dev,
                             customer_ids, product_ids, size_ids):
    start_date, end_date = date_range
    conn = db_connection()
    try:
        cursor = conn.cursor()
        receipts_insert_query = '''INSERT INTO receipt (id, customer_id, receipt_dttm) VALUES %s;'''
        receipts_item_insert_query = '''INSERT INTO receipt_item (receipt_id, product_id, size_id, quantity) VALUES %s;'''
        insert_queries = [receipts_insert_query, receipts_item_insert_query]

        receipts, receipts_item = generate_receipt(total_customers,
                                                   p_purchase,
                                                   start_date,
                                                   end_date,
                                                   mean_weekday_time,
                                                   mean_weekend_time,
                                                   std_dev,
                                                   customer_ids,
                                                   product_ids,
                                                   size_ids)

        tables = {'receipt': receipts,
                  'receipt_item': receipts_item
                  }

        for table, query in zip(tables.items(), insert_queries):
            records = table[1].values.tolist()
            psycopg2.extras.execute_values(cursor, query, records)
            conn.commit()
            print(f'{table[0]}: {len(records)} rows inserted successfully into PostgreSQL\n')
            sleep(5)

    except (Exception, psycopg2.Error) as error:
        print("Error:", error)

    finally:
        if conn:
            cursor.close()
            conn.close()


# Simulation parameters
total_customers = get_count('customer')  # Total number of clients
p_purchase = 0.01  # Probability of purchase by one customer per day (approximate)

try:
    conn = db_connection()
    cursor = conn.cursor()

    customer_ids = get_ids('customer')
    product_ids = get_ids('product')
    size_ids = get_ids('size')

    create_table_query = '''
    DROP TABLE IF EXISTS public.receipt;
    CREATE TABLE IF NOT EXISTS public.receipt
    (
        id char(36) PRIMARY KEY,
        customer_id serial NOT NULL,
        receipt_dttm timestamp(3) NOT NULL,
        record_dttm timestamp(3) without time zone NOT NULL DEFAULT NOW(),
        FOREIGN KEY (customer_id) REFERENCES customer(id)
    )

    TABLESPACE pg_default;

    ALTER TABLE IF EXISTS public.receipt OWNER to postgres;

    DROP TABLE IF EXISTS public.receipt_item;
    CREATE TABLE IF NOT EXISTS public.receipt_item
    (
        id bigserial PRIMARY KEY,
        receipt_id char(36) NOT NULL,
        product_id serial NOT NULL,
        size_id smallserial NOT NULL,
        quantity smallserial NOT NULL,
        record_dttm timestamp(3) without time zone NOT NULL DEFAULT NOW(),
        FOREIGN KEY (receipt_id) REFERENCES receipt(id),
        FOREIGN KEY (product_id) REFERENCES product(id),
        FOREIGN KEY (size_id) REFERENCES size(id)
    )

    TABLESPACE pg_default;

    ALTER TABLE IF EXISTS public.receipt_item OWNER to postgres;
    '''

    cursor.execute(create_table_query)
    conn.commit()

    start_date = datetime(2024, 3, 25, 0, 0, 0)
    mean_weekday_time = datetime.strptime("20:00", "%H:%M").time()
    mean_weekend_time = datetime.strptime("15:00", "%H:%M").time()
    std_dev = 2  # Standard deviation in hours
    days = 30 * 2

    date_ranges = [(start_date + timedelta(days=i), start_date + timedelta(days=i + 1)) for i in range(days)]
    num_threads = 32  # It is recommended to use more threads for I/O-bound tasks

    with ThreadPoolExecutor(max_workers=num_threads) as executor:
        futures = [
            executor.submit(generate_and_insert_data, date_range, total_customers, p_purchase, mean_weekday_time,
                            mean_weekend_time, std_dev, customer_ids, product_ids, size_ids)
            for date_range in date_ranges
        ]
        for future in futures:
            future.result()

except (Exception, psycopg2.Error) as error:
    print("Error inserting data into PostgreSQL:", error)

finally:
    if conn:
        cursor.close()
        conn.close()
        print("PostgreSQL connection is closed")

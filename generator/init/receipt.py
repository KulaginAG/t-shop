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
import sys
from utils import Logs

register_adapter(np.int64, AsIs)
register_adapter(np.float64, AsIs)
load_dotenv()

cwd = os.path.dirname(os.path.realpath(__file__))
logfile = Logs(cwd)
logfile.set_config()


def db_connection():
    db_name = os.getenv("DB_NAME_PG")
    db_host = os.getenv("DB_HOST_PG")
    db_port = os.getenv("DB_PORT_PG")
    db_user = os.getenv("DB_USER_PG")

    return psycopg2.connect(
        database=db_name,
        user=db_user,
        password=f"{keyring.get_password(db_name, db_user)}",
        host=db_host,
        port=db_port,
    )


def get_count(table):
    query = f"""
             SELECT MAX(id)
             FROM app.{table}
             """

    try:
        conn = db_connection()
        cursor = conn.cursor()
        cursor.execute(query)
        result = cursor.fetchone()

    except (Exception, psycopg2.Error) as error:
        logfile.show_action(f'Error: {error}')
        logfile.add_separator()
        sys.exit(1)

    finally:
        # Закрытие соединения с базой данных
        if conn:
            cursor.close()
            conn.close()

    return result[0]


def get_ids(table):
    query = f"""
             SELECT id
             FROM app.{table}
             """
    try:
        conn = db_connection()
        cursor = conn.cursor()
        cursor.execute(query)
        result = [row[0] for row in cursor.fetchall()]
    except (Exception, psycopg2.Error) as error:
        logfile.show_action(f'Error: {error}')
        logfile.add_separator()
        sys.exit(1)
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
        logfile.show_action(f'Processing date: {current_date.strftime('%Y-%m-%d %H:%M:%S')}')
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
        receipts_insert_query = '''INSERT INTO app.receipt (id, customer_id, receipt_dttm) VALUES %s;'''
        receipts_item_insert_query = '''INSERT INTO app.receipt_item (receipt_id, product_id, size_id, quantity) VALUES %s;'''
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

        logfile.show_action(f'Inserting date: {start_date.strftime('%Y-%m-%d %H:%M:%S')}')
        for table, query in zip(tables.items(), insert_queries):
            records = table[1].values.tolist()
            psycopg2.extras.execute_values(cursor, query, records)
            conn.commit()
            logfile.show_action(f'{table[0]}: {len(records)} rows inserted successfully for date: {start_date.strftime('%Y-%m-%d %H:%M:%S')}')
            sleep(5)

    except (Exception, psycopg2.Error) as error:
        # Logging error information
        logfile.show_action(f'An error occurred: {error}')
        logfile.add_separator()

    finally:
        if conn:
            cursor.close()
            conn.close()


# Simulation parameters
p_purchase = 0.01  # Probability of purchase by one customer per day (approximate)

try:
    logfile.add_separator()
    logfile.name('receipt init generator')
    logfile.show_action('Connecting to database')

    total_customers = get_count('customer')  # Total number of clients

    conn = db_connection()
    cursor = conn.cursor()

    logfile.show_action('Getting ids')
    customer_ids = get_ids('customer')
    product_ids = get_ids('product')
    size_ids = get_ids('size')

    create_table_query = '''
    DROP TABLE IF EXISTS app.receipt;
    CREATE TABLE IF NOT EXISTS app.receipt
    (
        id char(36) PRIMARY KEY,
        customer_id serial NOT NULL,
        receipt_dttm timestamp(3) NOT NULL,
        record_dttm timestamp(3) without time zone NOT NULL DEFAULT NOW(),
        FOREIGN KEY (customer_id) REFERENCES app.customer(id)
    );

    DROP TABLE IF EXISTS app.receipt_item;
    CREATE TABLE IF NOT EXISTS app.receipt_item
    (
        id bigserial PRIMARY KEY,
        receipt_id char(36) NOT NULL,
        product_id serial NOT NULL,
        size_id smallserial NOT NULL,
        quantity smallserial NOT NULL,
        record_dttm timestamp(3) without time zone NOT NULL DEFAULT NOW(),
        FOREIGN KEY (receipt_id) REFERENCES app.receipt(id),
        FOREIGN KEY (product_id) REFERENCES app.product(id),
        FOREIGN KEY (size_id) REFERENCES app.size(id)
    );
    '''

    logfile.show_action('Creating table')
    cursor.execute(create_table_query)
    conn.commit()

    logfile.show_action('Setting parameters')

    start_date = datetime(2024, 3, 25, 0, 0, 0)
    end_date = datetime.now()
    days = (end_date - start_date).days

    mean_weekday_time = datetime.strptime("20:00", "%H:%M").time()
    mean_weekend_time = datetime.strptime("15:00", "%H:%M").time()
    std_dev = 2  # Standard deviation in hours

    date_ranges = [(start_date + timedelta(days=i), start_date + timedelta(days=i + 1)) for i in range(days)]
    num_threads = 32  # It is recommended to use more threads for I/O-bound tasks

    logfile.show_action('Data generation has started')
    with ThreadPoolExecutor(max_workers=num_threads) as executor:
        futures = [
            executor.submit(generate_and_insert_data, date_range, total_customers, p_purchase, mean_weekday_time,
                            mean_weekend_time, std_dev, customer_ids, product_ids, size_ids)
            for date_range in date_ranges
        ]
        for future in futures:
            future.result()

    logfile.show_action('All rows inserted successfully into PostgreSQL')
    logfile.show_action('Completed successfully')
    logfile.add_separator()

except (Exception, psycopg2.Error) as error:
    # Logging error information
    logfile.show_action(f'An error occurred: {error}')
    logfile.add_separator()

finally:
    if conn:
        cursor.close()
        conn.close()

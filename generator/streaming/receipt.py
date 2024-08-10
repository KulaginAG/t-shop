import numpy as np
import pandas as pd
from datetime import datetime, timedelta
import random
import psycopg2
import psycopg2.extras
from psycopg2.extensions import register_adapter, AsIs
from uuid import uuid4
import keyring
from time import sleep
import logging
from dotenv import load_dotenv
import os

# Logging setup
logging.basicConfig(filename='receipt.log', level=logging.INFO, format='[%(asctime)s] (%(levelname)s): %(message)s',
                    force=True)

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


def get_count(conn, table):
    query = f"SELECT MAX(id) FROM {table}"
    with conn.cursor() as cursor:
        cursor.execute(query)
        result = cursor.fetchone()
    return result[0]


def get_ids(conn, table):
    query = f"SELECT id FROM {table}"
    with conn.cursor() as cursor:
        cursor.execute(query)
        result = [row[0] for row in cursor.fetchall()]
    return result


def calculate_day_coefficient(day_of_week):
    """Determine the coefficient for the given day of the week."""
    if day_of_week in range(1, 6):  # Weekdays (Monday to Friday)
        return np.random.uniform(0.5, 1.5)
    else:  # Weekends (Saturday and Sunday)
        return np.random.uniform(1.5, 3.0)


def calculate_time_coefficient(hour, base_hour, min_coefficient, max_coefficient):
    """Calculate the time-dependent coefficient."""
    # Calculate the hour difference
    hour_diff = min(abs(hour - base_hour), 24 - abs(hour - base_hour))
    # Normalize the hour difference to a range between 0 and 1
    normalized_diff = 1 - (hour_diff / 12) ** 3  # 12 instead of 24, since the max distance in a 24-hour circle is 12
    # Calculate the coefficient based on the normalized difference
    return min_coefficient + (max_coefficient - min_coefficient) * normalized_diff


def calculate_delay(input_datetime, total_customers, day_coefficient, base_hour, min_coefficient, max_coefficient,
                    base_purchase_probability=0.01):
    # Calculate delay based on the provided datetime, number of clients, and coefficients
    time_coefficient = calculate_time_coefficient(input_datetime.hour, base_hour, min_coefficient, max_coefficient)
    effective_coefficient = day_coefficient * time_coefficient
    purchase_probability = base_purchase_probability * effective_coefficient
    avg_interval = 86400 / (total_customers * purchase_probability)
    std_dev_multiplier = 0.5 if input_datetime.isoweekday() in range(1, 6) else 2.0
    mean_interval = avg_interval
    std_dev = avg_interval * std_dev_multiplier
    final_interval = np.random.normal(mean_interval, std_dev)
    return abs(final_interval)


def generate_receipt(customer_ids, product_ids, size_ids):
    receipts = []
    receipts_item = []

    customer_id = random.choice(customer_ids)
    receipt_id = str(uuid4())
    receipt_dttm = datetime.now()

    receipts.append([receipt_id, customer_id, receipt_dttm])

    shape = 2.0
    scale = 0.55
    num_items = int(np.ceil(np.random.gamma(shape, scale)))

    for _ in range(num_items):
        product_id = random.choice(product_ids)
        size_id = random.choice(size_ids)
        quantity = int(np.ceil(np.random.gamma(shape, scale)))
        receipts_item.append([receipt_id, product_id, size_id, quantity])

    df_receipts = pd.DataFrame(receipts, columns=['id', 'customer_id', 'receipt_dttm'])
    df_receipts_item = pd.DataFrame(receipts_item, columns=['receipt_id', 'product_id', 'size_id', 'quantity'])

    return df_receipts, df_receipts_item


def generate_and_insert_data(conn, customer_ids, product_ids, size_ids):
    try:
        with conn.cursor() as cursor:
            receipts_insert_query = '''INSERT INTO receipt (id, customer_id, receipt_dttm) VALUES %s;'''
            receipts_item_insert_query = '''INSERT INTO receipt_item (receipt_id, product_id, size_id, quantity) VALUES %s;'''
            receipts, receipts_item = generate_receipt(customer_ids, product_ids, size_ids)
            psycopg2.extras.execute_values(cursor, receipts_insert_query, receipts.values.tolist())
            psycopg2.extras.execute_values(cursor, receipts_item_insert_query, receipts_item.values.tolist())
            conn.commit()
            logging.info(f'receipt: {len(receipts)} rows inserted successfully')
            logging.info(f'receipt_item: {len(receipts_item)} rows inserted successfully')
    except (Exception, psycopg2.Error) as error:
        logging.error(f"Error: {error}")


def main():
    with db_connection() as conn:
        while True:
            current_datetime = datetime.now()
            next_day = datetime.now().date() + timedelta(days=1)
            day_coefficient = calculate_day_coefficient(current_datetime.isoweekday())

            if current_datetime.isoweekday() in range(1, 6):  # Weekdays (Monday to Friday)
                base_hour = random.randint(18, 20)  # for weekdays
                min_coefficient = 0.5
                max_coefficient = 1.5
            else:  # Weekends (Saturday and Sunday)
                base_hour = random.randint(12, 18)  # for weekends
                min_coefficient = 1.5
                max_coefficient = 3.0

            while datetime.now().date() < next_day:
                total_customers = get_count(conn, 'customer')
                customer_ids = get_ids(conn, 'customer')
                product_ids = get_ids(conn, 'product')
                size_ids = get_ids(conn, 'size')

                next_min = datetime.now() + timedelta(minutes=1)
                # Check every minute for up-to-date information on customers, products, sizes
                while datetime.now() < next_min:
                    try:
                        generate_and_insert_data(conn, customer_ids, product_ids, size_ids)
                        interval = calculate_delay(datetime.now(), total_customers, day_coefficient, base_hour,
                                                   min_coefficient, max_coefficient)

                        logging.info(f'Current datetime: {datetime.now()}, Interval: {interval}')

                        sleep(interval)

                    except (Exception, psycopg2.Error) as error:
                        logging.error(f"Error: {error}")


if __name__ == "__main__":
    main()

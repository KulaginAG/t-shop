import numpy as np
import pandas as pd
from faker import Faker
import psycopg2.extras
from psycopg2.extensions import register_adapter, AsIs
import keyring
from random import randint
from time import sleep
from dotenv import load_dotenv
import os

register_adapter(np.int64, AsIs)
register_adapter(np.float64, AsIs)
load_dotenv()
fake = Faker()

db_host = os.getenv("DB_HOST_PG")
db_port = os.getenv("DB_PORT_PG")
db_user = os.getenv("DB_USER_PG")

try:
    conn = psycopg2.connect(
        database='postgres',
        user=db_user,
        password=f"{keyring.get_password('PostgreSQL', db_user)}",
        host=db_host,
        port=db_port,
    )

    cursor = conn.cursor()

    insert_customer_query = '''INSERT INTO customer (first_name, last_name, birth_dt) VALUES %s;'''

    while True:
        customer = {
            'first_name': [fake.first_name() for _ in range(1)],
            'last_name': [fake.last_name() for _ in range(1)],
            'birth_dt': [fake.date_of_birth(minimum_age=18, maximum_age=60) for _ in range(1)]
        }
        customer = pd.DataFrame(customer)

        records = customer.values.tolist()

        # Inserting data into PostgreSQL
        psycopg2.extras.execute_values(cursor, insert_customer_query, records)
        conn.commit()
        print(fr"Added customer: {records}")
        sleep(randint(1, 50))
except (Exception, psycopg2.Error) as error:
    print("Error inserting data into PostgreSQL:", error)

finally:
    if conn:
        cursor.close()
        conn.close()
        print("PostgreSQL connection is closed")
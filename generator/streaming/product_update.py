import numpy as np
import random
import psycopg2.extras
from psycopg2.extensions import register_adapter, AsIs
import keyring
from time import sleep
from dotenv import load_dotenv
import os

register_adapter(np.int64, AsIs)
register_adapter(np.float64, AsIs)
load_dotenv()

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

    get_product_query = '''
            SELECT id, price
            FROM product
            ORDER BY random()
            LIMIT 1
            '''

    while True:
        cursor.execute(get_product_query)
        result = cursor.fetchone()
        product_id, current_price = result[0], result[1]
        new_price = round(current_price * random.randint(80, 150) / 100, 2)
        update_product_query = f"""
                                UPDATE product
                                SET 
                                    price = {new_price},
                                    record_dttm = NOW()
                                WHERE id = {product_id}
                                """
        cursor.execute(update_product_query)
        conn.commit()
        print(f'product id {result[0]} changed price from {result[1]} to {new_price}')
        sleep(random.randint(500, 1000))
except (Exception, psycopg2.Error) as error:
    print("Error inserting data into PostgreSQL:", error)

finally:
    if conn:
        cursor.close()
        conn.close()
        print("PostgreSQL connection is closed")

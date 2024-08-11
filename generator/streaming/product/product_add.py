import random
import numpy as np
import pandas as pd
import psycopg2.extras
from psycopg2.extensions import register_adapter, AsIs
import keyring
from time import sleep
from dotenv import load_dotenv
import os

register_adapter(np.int64, AsIs)
register_adapter(np.float64, AsIs)
load_dotenv()

product = pd.read_csv('../../src/styles.csv', sep=',', on_bad_lines='skip')
product = product[product['masterCategory'] == 'Apparel']
product = product[['productDisplayName', 'articleType']]
product.rename(columns={'productDisplayName': 'name',
                        'articleType': 'category',
                        },
               inplace=True
               )
product.drop_duplicates(subset=['name', 'category'], inplace=True)

# Gamma distribution parameters for median 80
shape = 3.0
scale = 30.0

# Generate prices for each unique product
prices = np.random.gamma(shape, scale, len(product))

# Create a price mapping
price_mapping = {product: round(price, 2) for product, price in zip(product['name'], prices)}

# Assign prices to the DataFrame
product['price'] = product['name'].map(price_mapping)

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
            SELECT name, category, price
            FROM app.product
            '''

    product_add = pd.read_sql(get_product_query, conn)
    print('Data read successfully')
except (Exception, psycopg2.Error) as error:
    print("Error inserting data into PostgreSQL:", error)

finally:
    if conn:
        cursor.close()
        conn.close()
        print("PostgreSQL connection is closed")

max_id = 0

try:
    conn = psycopg2.connect(
        database='postgres',
        user='postgres',
        password=f"{keyring.get_password('PostgreSQL', 'postgres')}",
        host='109.120.134.204',
        port=5425,
    )

    cursor = conn.cursor()
    insert_product_query = '''INSERT INTO app.product (name, category, price) VALUES %s;'''
    while max_id < max(product.index):
        max_id = max(product_add.index)
        product_new = product.iloc[max_id:max_id + 1]

        records = product_new.values.tolist()

        # Inserting data into PostgreSQL
        psycopg2.extras.execute_values(cursor, insert_product_query, records)
        conn.commit()

        product_add = pd.concat([product_add, product_new], ignore_index=True)
        print(fr"Added product id: {max_id + 2}")
        sleep(random.randint(5_000, 10_000))

except (Exception, psycopg2.Error) as error:
    print("Error inserting data into PostgreSQL:", error)

finally:
    if conn:
        cursor.close()
        conn.close()
        print("PostgreSQL connection is closed")

import numpy as np
import pandas as pd
import psycopg2.extras
from psycopg2.extensions import register_adapter, AsIs
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


product = pd.read_csv('styles.csv', sep=',', on_bad_lines='skip')
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
product.reset_index(drop=True, inplace=True)
product_init = product.iloc[:len(product) // 2]

try:
    conn = db_connection()
    cursor = conn.cursor()

    create_table_query = '''
    DROP TABLE IF EXISTS public.product;
    CREATE TABLE IF NOT EXISTS public.product
    (
        id serial PRIMARY KEY,
        name varchar(100) NOT NULL,
        category varchar(20) DEFAULT NULL,
        price numeric(10, 2) NOT NULL DEFAULT 0,
        record_dttm timestamp(3) without time zone NOT NULL DEFAULT NOW()
    )

    TABLESPACE pg_default;

    ALTER TABLE IF EXISTS public.product OWNER to postgres;
    '''
    cursor.execute(create_table_query)
    conn.commit()

    insert_query = '''INSERT INTO product (name, category, price) VALUES %s;'''
    # Convert DataFrame to List of Tuples (for each row)
    records = product_init.to_records(index=False)
    records_list = list(records)

    # Inserting data into PostgreSQL
    psycopg2.extras.execute_values(cursor, insert_query, records_list)
    conn.commit()
    print(f"{cursor.rowcount} rows inserted successfully into PostgreSQL")

except (Exception, psycopg2.Error) as error:
    print("Error inserting data into PostgreSQL", error)

finally:
    if conn:
        cursor.close()
        conn.close()
        print("PostgreSQL connection is closed")

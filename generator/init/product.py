import numpy as np
import pandas as pd
import psycopg2.extras
from psycopg2.extensions import register_adapter, AsIs
import keyring
from dotenv import load_dotenv
import os
import sys
from pathlib import Path
from utils import Logs


register_adapter(np.int64, AsIs)
register_adapter(np.float64, AsIs)
load_dotenv()

cwd = os.path.dirname(os.path.realpath(__file__))
logfile = Logs(cwd)
logfile.set_config()


def get_project_path():
    current_dir = Path(__file__).resolve()
    project_root = current_dir
    while not (project_root / '.env').exists() and project_root != project_root.parent:
        project_root = project_root.parent

    return project_root


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


try:
    logfile.add_separator()
    logfile.name('product init generator')

    logfile.show_action('Importing data')
    file_path = get_project_path() / 'generator/src'
    product = pd.read_csv(f'{file_path}/styles.csv', sep=',', on_bad_lines='skip')

    logfile.show_action('Processing data')
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
        logfile.show_action('Connecting to database')
        conn = db_connection()
        cursor = conn.cursor()

        create_table_query = '''
        DROP TABLE IF EXISTS app.product CASCADE;
        CREATE TABLE IF NOT EXISTS app.product
        (
            id serial PRIMARY KEY,
            name varchar(100) NOT NULL,
            category varchar(20) DEFAULT NULL,
            price numeric(10, 2) NOT NULL DEFAULT 0,
            record_dttm timestamp(3) without time zone NOT NULL DEFAULT NOW()
        );
        '''

        cursor.execute(create_table_query)
        conn.commit()
        logfile.show_action('Table product created')

        insert_query = '''INSERT INTO app.product (name, category, price) VALUES %s;'''
        # Convert DataFrame to List of Tuples (for each row)
        records = product_init.to_records(index=False)
        records_list = list(records)

        # Inserting data into PostgreSQL
        logfile.show_action('Inserting data')
        psycopg2.extras.execute_values(cursor, insert_query, records_list)
        conn.commit()

        logfile.show_action('All rows inserted successfully into PostgreSQL')
        logfile.show_action('Completed successfully')
        logfile.add_separator()

    except (Exception, psycopg2.Error) as error:
        logfile.show_action(f'Error inserting data into PostgreSQL: {error}')
        logfile.add_separator()
        sys.exit(1)

    finally:
        if conn:
            cursor.close()
            conn.close()

except Exception as error:
    # Logging error information
    logfile.show_action(f'An error occurred: {error}')
    logfile.add_separator()

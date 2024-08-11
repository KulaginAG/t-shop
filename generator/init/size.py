import numpy as np
import pandas as pd
import psycopg2.extras
from psycopg2.extensions import register_adapter, AsIs
import keyring
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


size = {
    'size': ['XS', 'S', 'M', 'L', 'XL', 'XXL', '3XL']
}
size = pd.DataFrame(size)

try:
    logfile.add_separator()
    logfile.name('size init generator')
    logfile.show_action('Connecting to database')

    conn = db_connection()
    cursor = conn.cursor()

    create_table_query = '''
    DROP TABLE IF EXISTS app.size;
    CREATE TABLE IF NOT EXISTS app.size
    (
        id smallserial PRIMARY KEY,
        size varchar(5) DEFAULT NULL,
        record_dttm timestamp(3) without time zone NOT NULL DEFAULT NOW()
    );
    '''
    cursor.execute(create_table_query)
    conn.commit()

    insert_query = '''INSERT INTO app.size (size) VALUES %s;'''
    # Convert DataFrame to List of Tuples (for each row)
    records = size.values.tolist()

    # Inserting data into PostgreSQL
    logfile.show_action('Inserting data')
    psycopg2.extras.execute_values(cursor, insert_query, records)
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

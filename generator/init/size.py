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


size = {
    'size': ['XS', 'S', 'M', 'L', 'XL', 'XXL', '3XL']
}
size = pd.DataFrame(size)

try:
    conn = db_connection()
    cursor = conn.cursor()

    create_table_query = '''
    DROP TABLE IF EXISTS public.size;
    CREATE TABLE IF NOT EXISTS public.size
    (
        id smallserial PRIMARY KEY,
        size varchar(5) DEFAULT NULL,
        record_dttm timestamp(3) without time zone NOT NULL DEFAULT NOW()
    )

    TABLESPACE pg_default;

    ALTER TABLE IF EXISTS public.size OWNER to postgres;
    '''
    cursor.execute(create_table_query)
    conn.commit()

    insert_query = '''INSERT INTO size (size) VALUES %s;'''
    # Convert DataFrame to List of Tuples (for each row)
    records = size.values.tolist()

    # Inserting data into PostgreSQL
    psycopg2.extras.execute_values(cursor, insert_query, records)
    conn.commit()
    print(f"{cursor.rowcount} rows inserted successfully into PostgreSQL")

except (Exception, psycopg2.Error) as error:
    print("Error inserting data into PostgreSQL", error)

finally:
    if conn:
        cursor.close()
        conn.close()
        print("PostgreSQL connection is closed")

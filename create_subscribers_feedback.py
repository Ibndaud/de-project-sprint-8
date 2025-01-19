import psycopg2

# PostgreSQL connection parameters
host = "localhost"
port = "5432"
database = "de"
user = "jovyan"
password = "jovyan"

# Connect to the PostgreSQL database
conn = psycopg2.connect(
    host=host,
    port=port,
    database=database,
    user=user,
    password=password
    )

# Create a cursor object
cursor = conn.cursor()

# Create a table
table_name = "subscribers_feedback"
create_table_query = f"""
    DROP TABLE IF EXISTS {table_name};
    CREATE TABLE IF NOT EXISTS {table_name} (
        id serial4 NOT NULL,
        restaurant_id text NOT NULL,
        adv_campaign_id text NOT NULL,
        adv_campaign_content text NOT NULL,
        adv_campaign_owner text NOT NULL,
        adv_campaign_owner_contact text NOT NULL,
        adv_campaign_datetime_start int8 NOT NULL,
        adv_campaign_datetime_end int8 NOT NULL,
        datetime_created int8 NOT NULL,
        client_id text NOT NULL,
        trigger_datetime_created int4 NOT NULL,
        feedback varchar NULL,
        CONSTRAINT id_pk PRIMARY KEY (id)
    );
"""
cursor.execute(create_table_query)

# Commit the changes and close the connection
conn.commit()
conn.close()
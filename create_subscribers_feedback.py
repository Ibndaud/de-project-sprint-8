import psycopg2

from subscribers_feedback_config import DB_CONFIG


table_name = 'subscribers_feedback'
create_table_query = f'''
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
'''


with psycopg2.connect(**DB_CONFIG) as conn:
    with conn.cursor() as cursor:
        cursor.execute(create_table_query)
        print(f'Table {table_name} created successfully.')

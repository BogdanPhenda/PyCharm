from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import psycopg2

# Настройки подключения к базе данных
DB_CONFIG = {
    'dbname': 'your_db_name',
    'user': 'your_user',
    'password': 'your_password',
    'host': 'your_host',
    'port': 'your_port'
}

# Логика обработки данных
def process_and_insert_data():
    try:
        # Устанавливаем соединение с базой данных
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()

        # SQL-запрос для вставки данных
        insert_query = """
        INSERT INTO norm_data.norm_flats (
            "timestamp",
            online,
            room,
            status,
            section_name,
            price,
            price_base,
            area_total,
            area_given,
            area_kitchen,
            number,
            floor_of_flat,
            tags,
            plan_url,
            floor_plan_url,
            finishing,
            flats_uuid,
            flats_type_uuid,
            floors_in_section,
            building_uuid,
            block_uuid,
            uuid_real_estate_type
        )
        SELECT 
            (now() + interval '3m')::timestamp::text,
            true,
            replace((SELECT CASE 
                WHEN block_uuid = 'f49f5e6b-67f1-4596-a4f8-5f27f1f5f457' AND rooms = '9' THEN '0'
                ELSE rooms 
            END), 'None', '0') AS room,
            'Свободна',
            CASE 
                WHEN section = '' THEN 'Нет секции'
                ELSE section 
            END AS section,
            CASE 
                WHEN price_sale = '' THEN 0
                ELSE price_sale::bigint
            END AS price,
            price_base::bigint,
            CAST(area_total AS double precision), 
            CAST(area_living AS double precision), 
            CAST(area_kitchen AS double precision),  
            CASE 
                WHEN number IS NULL THEN '' 
                ELSE number
            END AS number,
            CASE 
                WHEN floor = '' THEN 0
                ELSE floor::bigint
            END AS floor,
            CASE 
                WHEN type IN ('apartment', 'Апартамент', 'апартаменты', 'Апартаменты', 'Аппартаменты', 'Гостиничный номер') THEN 'Апартаменты'
                WHEN type IN ('flat', 'квартира', 'Квартира', 'жилая', 'Пентхаус', 'студия', 'Студия', 'residential') THEN 'Квартира'
                WHEN type IN ('гараж', 'Паркинг', 'parking', 'Парковка', 'Машино-место') THEN 'Машино-место'
                WHEN type IN ('Вилла', 'Загородная недвижимость', 'house') THEN 'Загородная недвижимость'
                WHEN type IN ('Таунхаус') THEN 'Таунхаус'
                WHEN type IN ('non-residential', 'Кладовая', 'Кладовка') THEN 'Кладовка'
                WHEN type IN ('Коммерция', 'Коммерческая недвижимость', 'Коммерческое помещение') THEN 'Коммерческая недвижимость'
                ELSE NULL
            END AS tags,
            plans_flat,
            plans_floor,
            finishing,
            uuid,
            CASE
                WHEN block_uuid = 'f49f5e6b-67f1-4596-a4f8-5f27f1f5f457' AND rooms = '9' THEN '019f2104-628f-468a-a368-2df80e0b3247'::uuid
                WHEN block_uuid = '04c6223f-24fc-412b-bf49-2adcd8ddccc8' AND rooms = '1' THEN '019f2104-628f-468a-a368-2df80e0b3247'::uuid
                ...
                WHEN rooms = '9' THEN 'c2e97799-25d6-459e-a88c-ff16eee1f3a2'::uuid
                ELSE null
            END AS flats_type_uuid,
            floors_in_section::bigint, 
            building_uuid,
            block_uuid,
            CASE
                WHEN type IN ('apartment', 'Апартамент', 'апартаменты', 'Апартаменты', 'Аппартаменты', 'Гостиничный номер') THEN '1fc784b5-639c-4777-91ce-da6324ba59f7'::uuid
                WHEN type IN ('flat', 'Квартира', 'квартира', 'жилая', 'Пентхаус', 'студия', 'Студия', 'residential') THEN '9f866b12-2848-4b60-b754-811212ce8657'::uuid
                ...
                ELSE NULL
            END AS uuid_real_estate_type
        FROM fids_raw_data.flats
        WHERE price_base IS NOT NULL AND price_sale IS NOT NULL
        AND (plans_flat IS NOT NULL OR plans_flat <> '')
        ON CONFLICT DO NOTHING;
        """

        # Выполняем запрос
        cursor.execute(insert_query)
        conn.commit()

    except Exception as e:
        print(f"Error: {e}")
    finally:
        cursor.close()
        conn.close()

# Конфигурация DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='norm_data_processing',
    default_args=default_args,
    description='Processing and inserting data into norm_flats',
    schedule_interval='@hourly',  # Запускается каждый час
    start_date=datetime(2023, 11, 1),
    catchup=False,
) as dag:

    process_data = PythonOperator(
        task_id='process_and_insert_data',
        python_callable=process_and_insert_data,
    )

    process_data

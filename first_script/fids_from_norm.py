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
            WHEN new.block_uuid = 'f49f5e6b-67f1-4596-a4f8-5f27f1f5f457' AND new.rooms = '9' THEN '019f2104-628f-468a-a368-2df80e0b3247'::uuid
            WHEN new.block_uuid = '04c6223f-24fc-412b-bf49-2adcd8ddccc8' AND new.rooms = '1' THEN '019f2104-628f-468a-a368-2df80e0b3247'::uuid
            WHEN new.block_uuid = '04c6223f-24fc-412b-bf49-2adcd8ddccc8' AND new.rooms = '2' THEN 'b35e9518-4d9d-4e58-8b4a-51d7aa42d7ec'::uuid
            WHEN new.block_uuid = '04c6223f-24fc-412b-bf49-2adcd8ddccc8' AND new.rooms = '3' THEN '8c89d8c7-7ea0-4412-a0cb-44cb12914309'::uuid
            WHEN new.block_uuid = '04c6223f-24fc-412b-bf49-2adcd8ddccc8' AND new.rooms = '4' THEN '1102e5b0-cc60-45dc-bc86-520c1f48f085'::uuid
            WHEN new.block_uuid = '04c6223f-24fc-412b-bf49-2adcd8ddccc8' AND new.rooms = '5' THEN '80033d77-b52a-44b9-a630-8f5243b93553'::uuid
            WHEN new.rooms = '0' and type 'Квартира' THEN '019f2104-628f-468a-a368-2df80e0b3247'::uuid
            WHEN new.rooms = '1' and type 'Квартира' THEN 'b35e9518-4d9d-4e58-8b4a-51d7aa42d7ec'::uuid
            WHEN new.rooms = '2' and type 'Квартира' THEN '8c89d8c7-7ea0-4412-a0cb-44cb12914309'::uuid
            WHEN new.rooms = '3' and type 'Квартира' THEN '1102e5b0-cc60-45dc-bc86-520c1f48f085'::uuid
            WHEN new.rooms = '4' and type 'Квартира' THEN '80033d77-b52a-44b9-a630-8f5243b93553'::uuid
            WHEN new.rooms = '5' and type 'Квартира' THEN 'e03ab11a-1d40-459e-8918-69d2a72f19bb'::uuid
            WHEN new.rooms = '6' and type 'Квартира' THEN 'b1e0f483-af4a-4e22-9944-2e49dd148303'::uuid
            WHEN new.rooms = '7' and type 'Квартира' THEN 'f61066c2-9dc4-49d8-93fd-485f979450dc'::uuid
            WHEN new.rooms = '8' and type 'Квартира' THEN '41acd6ed-b2a9-48da-b9e7-a45b7d645402'::uuid
            WHEN new.rooms = '9' and type 'Квартира' THEN 'c2e97799-25d6-459e-a88c-ff16eee1f3a2'::uuid
            WHEN new.rooms = '0' and type 'Апартаменты' THEN '362b73de-c175-4479-a76c-c2ee422b89f5'::uuid
            WHEN new.rooms = '1' and type 'Апартаменты' THEN '84203563-c482-45ee-8388-4968fa947834'::uuid
            WHEN new.rooms = '2' and type 'Апартаменты' THEN '6f6a9f85-bfb6-4785-acb8-d0877124ee49'::uuid
            WHEN new.rooms = '3' and type 'Апартаменты' THEN 'aacf7549-1286-421c-8280-a5ea73729e56'::uuid
            WHEN new.rooms = '4' and type 'Апартаменты' THEN '9cf2f8f2-b42a-459a-bd37-8d0bd053a6db'::uuid
            WHEN new.rooms = '5' and type 'Апартаменты' THEN 'f7e3fbcc-98dc-4cfa-a799-e63c14c00690'::uuid
            WHEN new.rooms = '6' and type 'Апартаменты' THEN 'a3be9f2b-5b1a-4a25-bbb7-956a73a0b469'::uuid
            WHEN new.rooms = '7' and type 'Апартаменты' THEN 'dfde23eb-a9ba-49b3-8ac2-6481864b6f51'::uuid
            WHEN new.rooms = '8' and type 'Апартаменты' THEN '0a060ad3-06ae-4135-935a-0fc5088ff92c'::uuid
            WHEN new.rooms = '9' and type 'Апартаменты' THEN '9711fe08-37c2-441b-b5fc-c8b5c79749e5'::uuid
            ELSE null
            END AS flats_type_uuid,
            floors_in_section::bigint, 
            building_uuid,
            block_uuid,
            CASE
            WHEN new.type IN ('apartment', 'Апартамент', 'апартаменты', 'Аппартаменты', 'Апартаменты', 'Гостиничный номер') THEN '1fc784b5-639c-4777-91ce-da6324ba59f7'::uuid
            WHEN new.type IN ('flat', 'Квартира', 'квартира', 'жилая', 'Пентхаус', 'студия', 'Студия', 'residential') THEN '9f866b12-2848-4b60-b754-811212ce8657'::uuid
            WHEN new.type IN ('гараж', 'Паркинг', 'parking', 'Парковка', 'Машино-место') THEN '53108344-b451-4f8f-87c7-27b9d8337eb6'::uuid
            WHEN new.type IN ('Вилла', 'Загородная недвижимость', 'house') THEN '82884858-ce63-4e1a-8356-1b9ce5afbbd9'::uuid
            WHEN new.type IN ('Таунхаус') THEN 'bfeaa7a8-f415-4a80-8c11-f88e55c3a9c9'::uuid
            WHEN new.type IN ('non-residential', 'Кладовая', 'Кладовка') THEN '25a2ed59-3178-4f0a-92fe-36f4e7d9c221'::uuid
            WHEN new.type IN ('Коммерция', 'Коммерческая недвижимость', 'Коммерческое помещение') THEN '97852a12-6ba2-4b98-879e-6277a9d780f9'::uuid
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

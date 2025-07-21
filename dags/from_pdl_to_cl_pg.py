# Импорт необходимых библиотек
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.common.sql.sensors.sql import SqlSensor
from airflow.exceptions import AirflowSkipException

import pandas as pd
import reverse_geocoder as rg
from datetime import datetime
import logging

# Настройка логгера
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
handler = logging.StreamHandler()
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)

"""
    Функция проверки данных в таблице,
    аргументы - дата начала (за какой период проверяются данные), 
    контекстные переменные AirFlow
"""
def check_data_exists(date_from: str, **context):
    
    # Логирование начала процесса
    logger.info(f"Проверка наличие данных за дату {date_from}")
    # Подключение к PostgreSQL (в AirFlow настроены коннекторы)
    postgres_hook = PostgresHook(postgres_conn_id='conn_pg')
    # SQL-запрос в PostgreSQL
    records = postgres_hook.get_records(
        f"SELECT 1 FROM cl.dim_time WHERE TO_CHAR(time, 'YYYY-MM-DD') = '{date_from}' LIMIT 1")
    # Получение True - если запись есть, False - в противном случае
    exists = bool(records)
    # Логирование булевого значения есть ли данные в PostgreSQL или нет
    logger.info(f"Наличие данных в PostgreSQL: {exists}")
    # Для обработки функции load_from_pdl_to_cl
    return exists

"""
    Функция загрузки данных из слоя Primary Date Layer -> в Core Layer,
    аргументы - дата начала (за какой период проверяются данные), 
    контекстные переменные AirFlow
"""
def load_from_pdl_to_cl(date_from: str, **context):
    
    # При успешном выполнении кода
    try:
        # Логирование начала загрузки
        logger.info(f"Начало загрузки данных за {date_from}")
        # Проверка наличия данных в PostgreSQL (предыдущая функция)
        if check_data_exists(date_from):
            # Логирование о пропуске task load_from_pdl_to_cl ввиду наличия данных за данную дату
            logger.warning(f"Данные за {date_from} загружены в PostgreSQL, task skipped")
            # Пробрасывание исключения для skipped task load_from_pdl_to_cl
            raise AirflowSkipException

        # Подключение к PostgreSQL (в AirFlow настроены коннекторы)
        postgres_hook = PostgresHook(postgres_conn_id='conn_pg')
        # Создание объекта для подключения к PostgreSQL и выполнении запросов
        engine = postgres_hook.get_sqlalchemy_engine()
        # Переменная с запросом
        query = f"""SELECT * FROM pdl.raw_earthquakes WHERE TO_CHAR(time, 'YYYY-MM-DD') = '{date_from}'"""
        # Получение данных в датафрэйм Pandas из PostgreSQL
        df = pd.read_sql(query, engine)
        # Логирование получение данных
        logger.info(f"Получено {len(df)} записей")
 
        # Логирование о приведении датафрэйма к нужному виду
        logger.info("Создание dim_time")
        # Приведение датафрэйма к нужному виду
        dim_time = pd.DataFrame({
            'time': pd.to_datetime(df['time']),
            'year': pd.to_datetime(df['time']).dt.year,
            'month': pd.to_datetime(df['time']).dt.month,
            'hour': pd.to_datetime(df['time']).dt.hour,
            'minute': pd.to_datetime(df['time']).dt.minute,
            'second': pd.to_datetime(df['time']).dt.second,
            'microsecond': pd.to_datetime(df['time']).dt.microsecond,
            'day_of_week': pd.to_datetime(df['time']).dt.day_name(),
            'id_fact': df['id']
        })

        # Логирование о приведении датафрэйма к нужному виду
        logger.info("Создание dim_location")
        # Приведение датафрэйма к нужному виду 
        # Тут столбцы region, country с помощью библиотеки reverse_geocoder 
        # Получаем в виде строк-названий по координатам (перебор с помощью цикла последовательно)
        coordinates = list(zip(df['latitude'], df['longitude']))
        results = []
        for coord in coordinates:
            result = rg.search(coord, mode=1)[0]
            results.append(result)
        dim_location = pd.DataFrame({
            'latitude': df['latitude'],
            'longitude': df['longitude'],
            'place': df['place'],
            'region': [result.get('admin1', 'unknown') for result in results],
            'country': [result.get('cc', 'unknown') for result in results],
            'id_fact': df['id']
        })

        # Логирование о приведении датафрэйма к нужному виду
        logger.info("Создание dim_magnitude")
        # Приведение датафрэйма к нужному виду
        dim_magnitude = pd.DataFrame({
            'mag': df['mag'],
            'mag_type': df['mag_type'],
            'mag_error': df['mag_error'],
            'mag_source': df['mag_source'],
            'id_fact': df['id']
        })
        
        # Логирование о начале загрузки данных
        logger.info("Начало загрузки данных в PostgreSQL в слой Core Layer")
        # Подключение к PostgreSQL, загрузка в БД
        with engine.connect() as conn:
            # Начало транзакции
            transaction = conn.begin()
            # При успешном выполнении кода
            try:
                # Структура для загрузки в БД (схема, таблица, датафрэйм)
                tables_data = [
                    ('cl', 'dim_time', dim_time),
                    ('cl', 'dim_location', dim_location),
                    ('cl', 'dim_magnitude', dim_magnitude)
                ]
                # Перебор в цикле (создание колонки id_fact, загрузка данных в каждую таблицу)
                for schema, table, table_df in tables_data:
                    # Логирование какая таблица в работе
                    logger.info(f"Начало загрузки в {schema}.{table}")
                    
                    # Логирование о добавлении столбца id_fact
                    logger.info(f"Добавление столбца id_fact {schema}.{table} временно")
                    # Выполнение запроса на добавление столбца
                    conn.execute(f"""
                        ALTER TABLE {schema}.{table} 
                        ADD COLUMN IF NOT EXISTS id_fact VARCHAR(50)
                    """)
                    
                    # Логирование о количестве вставленных записей в таблицу
                    logger.info(f"Добавлено {len(table_df)} записей в {schema}.{table}")
                    # Функция управляет сама commit или rollback транзакции
                    table_df.to_sql(
                        name=table,
                        con=conn,
                        schema=schema,
                        if_exists='append',
                        index=False
                    )
                
                # Добавление таблицы fact_earthquakes
                logger.info("Добавление записей в fact_earthquakes")
                fact_query = f"""
                    INSERT INTO cl.fact_earthquakes (id, depth, nst, gap, dmin, rms, status, time_id, location_id, magnitude_id)
                    SELECT
                        r.id as id,
                        r.depth AS depth,
                        r.nst AS nst,
                        r.gap AS gap,
                        r.dmin AS dmin,
                        r.rms AS rms,
                        r.status AS status,
                        t.id AS time_id,
                        l.id AS location_id,
                        m.id AS magnitude_id
                    FROM
                        pdl.raw_earthquakes r
                    JOIN 
                        cl.dim_time t ON t.id_fact = r.id
                    JOIN 
                        cl.dim_location l ON l.id_fact = r.id
                    JOIN
                        cl.dim_magnitude m ON m.id_fact = r.id
                    WHERE
                        TO_CHAR(r.time, 'YYYY-MM-DD') = '{date_from}'
                """
                # Выполнение запроса
                result = conn.execute(fact_query)
                # Логирование о добавлении записей в таблицу
                logger.info(f"Добавлено {result.rowcount} записей")
                
                # Удаление временного столбца id_fact
                for schema, table, _ in tables_data:
                    # Логирование об удалении столбца
                    logger.info(f"Удаление временного столбца id_fact в {schema}.{table}")
                    # Выполнение запроса
                    conn.execute(f"""ALTER TABLE {schema}.{table} DROP COLUMN IF EXISTS id_fact""")
                # commit транзакции
                transaction.commit()
                # Логирование об успешном процессе
                logger.info("Данные загружены")
            # Обработка исключений    
            except Exception as err:
                # rollback транзакции
                transaction.rollback()
                # Логирование об ошибке
                logger.error(f"Ошибка транзакции: {str(err)}")
                # Пробрасывание исключения для того, чтобы task завершилась со статусом failed
                raise

    # Обработка исключений
    except Exception as err:
        # Логирование об ошибке
        logger.error(f"Ошибка при загрузке данных: {str(err)}")
        # Пробрасывание исключения для того, чтобы task завершилась со статусом failed
        raise

# Настройка аргументов по умолчанию
DEFAULT_ARGS = {
    'owner': 'admin',
    'retries': 2,
    'retry_delay': 600,
    'start_date': datetime(2025, 7, 19)
}

# Настройка DAG, использование Jinja-шаблонизатора для объявления даты начала,
# Настроен сенсор для проверки наличия сырых данных
with DAG(
    dag_id='from_pdl_to_cl_pg',
    description='Загрузка данных из Primary Data Layer в Core Layer по схеме "Звезда"',
    tags=['admin', 'from_pdl_to_cl'],
    schedule='@daily',
    default_args=DEFAULT_ARGS,
    catchup=True,
    max_active_runs=1,
    max_active_tasks=1
) as dag:
    
    start_dag = EmptyOperator(task_id='start_dag')
    end_dag = EmptyOperator(task_id='end_dag', trigger_rule='all_done')

    check_data = SqlSensor(
        task_id='check_data',
        conn_id='conn_pg',
        sql="""SELECT 1 FROM pdl.raw_earthquakes WHERE TO_CHAR(time, 'YYYY-MM-DD') = '{{ ds }}' LIMIT 1""",
        mode='reschedule',
        poke_interval=300,
        timeout=3600,
        success=lambda cnt: cnt == 1
    )

    load_from_pdl_to_cl = PythonOperator(
        task_id='load_from_pdl_to_cl',
        python_callable=load_from_pdl_to_cl,
        op_kwargs={'date_from': '{{ ds }}'}
    )

    start_dag >> check_data >> load_from_pdl_to_cl >> end_dag
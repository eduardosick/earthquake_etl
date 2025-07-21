# Импорт необходимых библиотек
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.common.sql.sensors.sql import SqlSensor
from airflow.exceptions import AirflowSkipException

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
    logger.info(f"Проверка наличия данных для даты {date_from} в Data Mart")
    # Подключение к PostgreSQL (в AirFlow настроены коннекторы)
    postgres_hook = PostgresHook(postgres_conn_id='conn_pg')
    # SQL-запрос в PostgreSQL
    records = postgres_hook.get_records(
        f"SELECT 1 FROM dm.all_count WHERE date = '{date_from}' LIMIT 1")
    # Получение True - если запись есть, False - в противном случае
    exists = bool(records)
    # Логирование булевого значения есть ли данные в PostgreSQL или нет
    logger.info(f"Наличие данных в PostgreSQL: {exists}")
    # Для обработки функции load_data
    return exists

"""
    Функция загрузки данных (агрегации) из слоя Core Layer -> в Data Mart,
    аргументы - дата начала (за какой период проверяются данные), 
    контекстные переменные AirFlow
"""
def load_data(date_from: str, **context):
    
    # При успешном выполнении кода
    try:
        # Логирование начала процесса
        logger.info(f"Начало загрузки данных в Data Mart для даты {date_from}")
        # Проверка наличия данных в PostgreSQL (предыдущая функция)
        if check_data_exists(date_from):
            # Логирование о пропуске task load_data ввиду наличия данных за данную дату
            logger.warning(f"Данные за {date_from} загружены в PostgreSQL, task skipped")
            # Пробрасывание исключения для skipped task load_data
            raise AirflowSkipException

        # Подключение к PostgreSQL (в AirFlow настроены коннекторы)
        postgres_hook = PostgresHook(postgres_conn_id='conn_pg')
        # Создание объекта для подключения к PostgreSQL и выполнении запросов
        engine = postgres_hook.get_sqlalchemy_engine()
        # Подключение к PostgreSQL, загрузка в БД
        with engine.connect() as conn:
            # Начало транзакции
            transaction = conn.begin()
            # При успешном выполнении кода
            try:
                # Загрузка общего количества землетрясений
                logger.info("Загрузка данных в dm.all_count")
                all_count_query = f"""
                    INSERT INTO dm.all_count (date, earthquakes_count)
                    SELECT
                        TO_CHAR(t.time, 'YYYY-MM-DD') AS date,
                        COUNT(e.id) AS earthquakes_count
                    FROM
                        cl.fact_earthquakes e
                    JOIN
                        cl.dim_time t ON e.time_id = t.id
                    WHERE
                        TO_CHAR(t.time, 'YYYY-MM-DD') = '{date_from}'
                    GROUP BY
                        date;
                """
                # Выполнение запроса на добавление данных в таблицу
                result = conn.execute(all_count_query)
                # Логирование о добавлении данных
                logger.info(f"Добавлено {result.rowcount} записей в dm.all_count")
                
                # Загрузка количества по странам
                logger.info("Загрузка данных в dm.country_count")
                country_count_query = f"""
                    INSERT INTO dm.country_count (date, country, earthquakes_count)
                    SELECT
                        TO_CHAR(t.time, 'YYYY-MM-DD') AS date,
                        l.country AS country,
                        COUNT(e.id) AS earthquakes_count
                    FROM 
                        cl.dim_location l
                    JOIN
                        cl.fact_earthquakes e ON l.id = e.location_id
                    JOIN
                        cl.dim_time t ON e.time_id = t.id
                    WHERE
                        TO_CHAR(t.time, 'YYYY-MM-DD') = '{date_from}'
                    GROUP BY
                        date, country;
                """
                # Выполнение запроса на добавление данных в таблицу
                result = conn.execute(country_count_query)
                # Логирование о добавлении данных
                logger.info(f"Добавлено {result.rowcount} записей в dm.country_count")
                
                # Загрузка топ регионов
                logger.info("Загрузка данных в dm.region_count")
                region_count_query = f"""
                    INSERT INTO dm.region_count (date, region, earthquakes_count)
                    SELECT
                        TO_CHAR(t.time, 'YYYY-MM-DD') AS date,
                        l.region AS region,
                        COUNT(e.id) OVER(PARTITION BY TO_CHAR(t.time, 'YYYY-MM-DD'), l.region) AS earthquakes_count
                    FROM 
                        cl.dim_location l
                    JOIN
                        cl.fact_earthquakes e ON l.id = e.location_id
                    JOIN
                        cl.dim_time t ON e.time_id = t.id
                    WHERE
                        TO_CHAR(t.time, 'YYYY-MM-DD') = '{date_from}'
                    ORDER BY
                        earthquakes_count DESC
                    LIMIT 1;
                """
                # Выполнение запроса на добавление данных в таблицу
                result = conn.execute(region_count_query)
                # Логирование о добавлении данных
                logger.info(f"Добавлено {result.rowcount} записей в dm.region_count")
                # commit транзаеции
                transaction.commit()
                # Логирование об успешном добавлении данных в Data Mart
                logger.info(f"Завершена загрузка данных для {date_from} в Data Mart")

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
        logger.error(f"Критическая ошибка в load_data: {str(err)}")
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
    dag_id='from_cl_to_dm',
    description='Загрузка данных из Core Layer в Data Mart',
    tags=['admin', 'from_cl_to_dm'],
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
        sql="""SELECT 1 FROM cl.dim_time WHERE TO_CHAR(time, 'YYYY-MM-DD') = '{{ ds }}' LIMIT 1""",
        mode='reschedule',
        poke_interval=300,
        timeout=3600,
        success=lambda cnt: cnt == 1
    )

    load_data = PythonOperator(
        task_id='load_data',
        python_callable=load_data,
        op_kwargs={'date_from': '{{ ds }}'}
    )

    start_dag >> check_data >> load_data >> end_dag
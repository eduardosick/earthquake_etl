# Импорт необходимых библиотек
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.exceptions import AirflowSkipException

import requests
import pandas as pd
from io import BytesIO
from io import StringIO
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
    Функция загрузки из API в MinIO,
    аргументы - даты начала и окончания(невключительно) (за какой период извлекаются данные), 
    контекстные переменные AirFlow
"""
def load_to_s3(date_from: str, date_to: str, **context) -> str:
    
    # Логирование начала процесса
    logger.info(f"Начало загрузки данных по API в MinIO с {date_from} по {date_to}")
    
    # Определение необходимых параметров для получения CSV-файла по API (адрес, параметры в виде файла, даты начала и даты окончания)
    API_URL = 'https://earthquake.usgs.gov/fdsnws/event/1/query'
    PARAMS = {
        'format': 'csv',
        'starttime': date_from,
        'endtime': date_to
    }
    # При успешном выполнении кода
    try:
        # Логирование отправка GET-запроса
        logger.info("Отправка GET-запроса к API")
        # GET-запрос
        response = requests.get(API_URL, params=PARAMS)
        # Проверка статуса ответа сервера (вызовет исключение, если не 200-299)
        response.raise_for_status()
        # Логирование об успешном обращении к API
        logger.info("Запрос к API выполнен успешно")

        # Обработка ответа от сервера, передача бинарных данных (байты) в Pandas так, будто это "файл" 
        data = BytesIO(response.content)
        # Чтение файла в Pandas
        df = pd.read_csv(data)
        # Конвертация в CSV-строку (для загрузки в MinIO без обработки в сыром виде)
        csv = df.to_csv()
        # Логирование размера полученных данных
        logger.info(f"Размер файла: {len(csv) / 1024} KB")

        # Подключение к MinIO (в AirFlow настроены коннекторы)
        s3_hook = S3Hook(aws_conn_id='conn_s3')
        # "Путь", по которому будут находиться сырые данные
        s3_key = f'earthquakes/{date_from}.csv'
        # Логирование "пути", где будут находиться сырые данные
        logger.info(f"Загрузка данных в MinIO по ключу: {s3_key}")
        # Сохранение данных в MinIO (bucket определён в MinIO)
        s3_hook.load_string(
            string_data=csv,
            key=s3_key,
            bucket_name='raw-data',
            replace=True,
            encoding='utf-8'
        )
        # Логирование об успешном сохранении сырых данных
        logger.info("Данные сохранены в MinIO")
        # Возвращение ключа для загрузки из MinIO в PostgreSQL с помощью XCom
        return s3_key

    # Обработка исключений    
    except Exception as err:
        # Логирование ошибки
        logger.error(f"Ошибка в task load_to_s3: {str(err)}", exc_info=True)
        # Пробрасывание исключения для того, чтобы task завершилась со статусом failed
        raise

"""
    Функция проверки наличия данных в PostgreSQL,
    аргументы - дата начала (за какую дату проверяются данные),
    контекстные переменные AirFlow
"""
def check_data_to_pg(date_from: str, **context) -> bool:
    
    # Логирование проверки наличия данных в PostgreSQL
    logger.info(f"Проверка наличия данных в PostgreSQL за {date_from}")
    
    # При успешном выполнении кода
    try:
        # Подключение к PostgreSQL (в AirFlow настроены коннекторы)
        postgres_hook = PostgresHook(postgres_conn_id='conn_pg')
        # SQL-запрос в PostgreSQL
        records = postgres_hook.get_records(
            f"SELECT 1 FROM pdl.raw_earthquakes WHERE TO_CHAR(time, 'YYYY-MM-DD') = '{date_from}' LIMIT 1"
        )
        # Получение True - если запись есть, False - в противном случае
        exists = bool(records)
        # Логирование булевого значения есть ли данные в PostgreSQL или нет
        logger.info(f"Наличие данных в PostgreSQL: {exists}")
        # Для обработки функции load_to_pg
        return exists
        
    # Обработка исключений
    except Exception as err:
        # Логирование ошибки
        logger.error(f"Ошибка в функции check_data_to_pg: {str(err)}", exc_info=True)
        # Пробрасывание исключения для того, чтобы task завершилась со статусом failed
        raise

"""
    Функция загрузки из MinIO в PostgreSQL,
    аргументы - дата начала (за какую дату загружаются данные), 
    контекстные переменные AirFlow
"""
def load_to_pg(date_from: str, **context) -> None:
    
    # Логирование начала процесса
    logger.info(f"Начало загрузки данных из MinIO в PostgreSQL за дату {date_from}")

    # Проверка наличия данных в PostgreSQL (предыдущая функция)
    if check_data_to_pg(date_from):
        # Логирование о пропуске task load_to_pg ввиду наличия данных за данную дату
        logger.warning(f"Данные за {date_from} загружены в PostgreSQL, task skipped")
        # Пробрасывание исключения для skipped task load_to_pg
        raise AirflowSkipException
    
    # При успешном выполнении кода
    try:
        # Получение key в MinIO для выгрузки файла
        key_csv = context['ti'].xcom_pull(task_ids='load_to_s3')
        # Логирование о получении ключа в MinIO
        logger.info(f"Получен ключ в MinIO для выгрузки: {key_csv}")
        # Подключение к MinIO (в AirFlow настроены коннекторы)
        s3_hook = S3Hook(aws_conn_id='conn_s3')
        # Логирование о чтении сырых данных
        logger.info(f"Чтение данных из MinIO: {key_csv}")
        csv = s3_hook.read_key(
            key=key_csv,
            bucket_name='raw-data'
        )
        # Логирование о обработке сырых данных
        logger.info("Обработка сырых данных в CSV с помощью Pandas")
        # Читаем csv-строку в "файл"
        df = pd.read_csv(StringIO(csv))
        # Расположение столбцов в удобном виде
        df = df[
            ['id', 'time', 'latitude', 'longitude', 
             'depth', 'mag', 'magType', 'nst', 
             'gap', 'dmin', 'rms', 'net', 
             'updated', 'place', 'type', 'horizontalError', 
             'depthError', 'magError', 'magNst', 'status', 
             'locationSource', 'magSource'
             ]
        ]
        # Переименование столбцов
        df.columns = ['id', 'time', 'latitude', 'longitude', 
                       'depth', 'mag', 'mag_type', 'nst', 
                       'gap', 'dmin', 'rms', 'net', 
                       'updated', 'place', 'type', 'horizontal_error', 
                       'depth_error', 'mag_error', 'mag_nst', 'status', 
                       'location_source', 'mag_source'
        ]
        # Преобразование столбца в тип datetime
        df['time'] = pd.to_datetime(df['time'])
        # Преобразование столбца в тип datetime
        df['updated'] = pd.to_datetime(df['updated'])
        # Логирование о преобразовании датафрэйма
        logger.info(f"Количество строк обработатно {len(df)}")
        
        # Подключение к PostgreSQL (в AirFlow настроены коннекторы)
        postgres_hook = PostgresHook(postgres_conn_id='conn_pg')
        # Создание объекта для подключения к PostgreSQL и выполнении запросов
        engine = postgres_hook.get_sqlalchemy_engine()
        # Логирование о начале загрузки данных
        logger.info("Начало загрузки сырых данных в Primary Data Layer, raw_earthquakes")
        # Подключение к PostgreSQL, загрузка CSV в БД
        with engine.connect() as conn:
            # Функция управляет сама commit или rollback транзакции
            df.to_sql(
                name='raw_earthquakes',
                con=engine,
                schema='pdl',
                if_exists='append',
                index=False
            )
        # Логирование успешной загрузки
        logger.info("Данные загружены в PostgreSQL")
    # Обработка исключений    
    except Exception as err:
        # Логирование ошибки
        logger.error(f"Ошибка в task load_to_pg: {str(err)}", exc_info=True)
        # Пробрасывание исключения для того, чтобы task завершилась со статусом failed
        raise

# Настройка аргументов по умолчанию
DEFAULT_ARGS = {
    'owner': 'admin',
    'retries': 2,
    'retry_delay': 600,
    'start_date': datetime(2025, 7, 19)
}
# Настройка DAG, использование Jinja-шаблонизатора для объявления дат начала и окончания
with DAG(
    dag_id='from_api_to_s3_and_pg',
    description='Загрузка CSV-файла из API в MinIO, из MinIO в PostgreSQL',
    tags=['admin', 'from_api_to_s3', 'from_s3_to_pg'],
    schedule='@daily',
    default_args=DEFAULT_ARGS,
    catchup=True,
    max_active_runs=1,
    max_active_tasks=1
) as dag:
    
    start_dag = EmptyOperator(task_id='start_dag')
    end_dag = EmptyOperator(task_id='end_dag', trigger_rule='all_done')

    load_to_s3 = PythonOperator(
        task_id='load_to_s3',
        python_callable=load_to_s3,
        op_kwargs={
            'date_from': '{{ ds }}',
            'date_to': '{{ macros.ds_add(ds, 1) }}'
        }
    )

    load_to_pg = PythonOperator(
        task_id='load_to_pg',
        python_callable=load_to_pg,
        op_kwargs={'date_from': '{{ ds }}'}
    )

    start_dag >> load_to_s3 >> load_to_pg >> end_dag
import psycopg2
import requests
from airflow.decorators import dag
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
import pendulum
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

def get_info_from_api(**context):
    # Fetch data
    data = requests.get(url="https://velib-metropole-opendata.smovengo.cloud/opendata/Velib_Metropole/station_information.json")
    station_infos = data.json()["data"]["stations"]

    context['ti'].xcom_push(key='stations_info_data', value=station_infos)

def insert_data(**context):
    try:

        conn = psycopg2.connect(
            dbname="velib",
            user="airflow",
            password="airflow",
            host = "postgres",
            port=5432
        )

        station_infos = context['ti'].xcom_pull(task_ids="get_station_info_from_api",key='stations_info_data'),

        cur = conn.cursor()
        for station in station_infos[0]:
            cur.execute("""
                INSERT INTO velib_stations (station_id, name, capacity, lat, lon)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (station_id) DO NOTHING
            """, (
                station["station_id"],
                station["name"],
                station["capacity"],
                station["lat"],
                station["lon"]
            ))

        conn.commit()
        cur.close()
        conn.close()

    except Exception as e :
        print("=========================")
        print(e)
        print("===========================")


@dag(
    start_date=pendulum.datetime(2025, 8, 30),
    catchup=False,
    tags=["velib", "station_info","insert"],
)
def worflow_insert_stations_info_db():

    begin = EmptyOperator(task_id="begin")

    create_velib_station_table_stg = SQLExecuteQueryOperator(
        task_id="create_velib_station_table",
        conn_id="postgres",
        sql="""
            CREATE TABLE IF NOT EXISTS velib_stations (
                station_id BIGINT PRIMARY KEY,
                name TEXT,
                capacity INTEGER,
                lat DOUBLE PRECISION,
                lon DOUBLE PRECISION
            )
            """,
        )

    get_station_info_from_api_stg = PythonOperator(
        task_id="get_station_info_from_api",
        python_callable=get_info_from_api,
        )

    insert_station_data_stg = PythonOperator(
        task_id="insert_station_data",
        python_callable=insert_data,
    )

    end = EmptyOperator(task_id="end")

    begin >> create_velib_station_table_stg >> get_station_info_from_api_stg >> insert_station_data_stg >> end

worflow_insert_stations_info_db()
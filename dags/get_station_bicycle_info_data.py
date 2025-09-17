import requests
import psycopg2
from datetime import datetime
from airflow.decorators import dag
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
import pendulum
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
import pytz

#in function insert data put try except when access station data and output the station with and error
#in fetch data, to avoid max request i will run dag each 5 minutes.

def fetch_station_status(**context):
    # Fetch data from the API
    response = requests.get("https://velib-metropole-opendata.smovengo.cloud/opendata/Velib_Metropole/station_status.json")
    
    station_status  = response.json()["data"]["stations"]

    context['ti'].xcom_push(key='stations_status_data', value=station_status)

def convert_timestamp_to_date(timestamp):
    timezone = pytz.timezone('CET')
    dt = datetime.fromtimestamp(timestamp, tz=timezone).replace(tzinfo=None)
    return dt


def insert_data(**context):

    try:
        conn = psycopg2.connect(
            dbname="velib",
            user="airflow",
            password="airflow",
            host = "postgres",
            port=5432
    )

    except psycopg2.Error as e:
        print(e)

    cur = conn.cursor()

    station_status = context['ti'].xcom_pull(task_ids="get_station_status_from_api",key='stations_status_data'),

    for station in station_status[0]:
        station["last_reported"] = convert_timestamp_to_date(station["last_reported"])
        cur.execute("""
            INSERT INTO station_status (station_id, num_bikes_available, num_mechanical, num_ebike, num_docks_available, last_reported, stationCode) VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (station_id, last_reported) DO UPDATE SET
            num_bikes_available = EXCLUDED.num_bikes_available,
            num_mechanical = EXCLUDED.num_mechanical,
            num_ebike = EXCLUDED.num_ebike,
            num_docks_available = EXCLUDED.num_docks_available;
        """, (
            station["station_id"],
            station["num_bikes_available"],
            station["num_bikes_available_types"][0]["mechanical"],
            station["num_bikes_available_types"][1]["ebike"],
            station["num_docks_available"],
            station["last_reported"],
            station["stationCode"]
        ))
        
        
    
    conn.commit()
    cur.close()
    conn.close()

@dag(
    start_date=pendulum.datetime(2025, 9, 1),
    schedule="* 6-17 * * *",
    catchup=False,
    tags=["velib", "station_info","insert"],
)
def workflow_station_bicycle_data():

    begin = EmptyOperator(task_id="begin")

    create_velib_station_status_stg = SQLExecuteQueryOperator(
        task_id="create_velib_station_status_table",
        conn_id="postgres",
        sql="""
                CREATE TABLE IF NOT EXISTS station_status (
                station_id BIGINT NOT NULL,
                num_bikes_available INTEGER NOT NULL,
                num_mechanical INTEGER NOT NULL,
                num_ebike INTEGER NOT NULL,
                num_docks_available INTEGER NOT NULL,
                last_reported TIMESTAMP NOT NULL,
                stationCode VARCHAR(50) NOT NULL,
                PRIMARY KEY (station_id, last_reported)
            ) """,
        )
    
    get_station_status_from_api_stg = PythonOperator(
        task_id="get_station_status_from_api",
        python_callable=fetch_station_status,
        )

    insert_station_status_data_stg = PythonOperator(
        task_id="insert_station_status_data",
        python_callable=insert_data,
    )

    end = EmptyOperator(task_id="end")

    begin >> create_velib_station_status_stg >> get_station_status_from_api_stg >> insert_station_status_data_stg >> end

workflow_station_bicycle_data()

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from zoneinfo import ZoneInfo
from datetime import datetime, timedelta
import requests
import json

# Default arguments
default_args = {
    'start_date': datetime.now().replace(minute=0, second=0, microsecond=0),
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

# API key dan lokasi
API_KEY = "6697ee55d2c2753b51e28a3e7e5a75fd"
LAT = "-7.2458"  # Latitude Surabaya
LON = "112.7383"  # Longitude Surabaya
CITY_NAME = 'Surabaya'
jakarta_tz = ZoneInfo("Asia/Jakarta")

# Define DAG
dag = DAG(
    'weather_air_pollution_pipeline',
    default_args=default_args,
    schedule_interval='@hourly',  # Ambil data setiap jam
)

# Task 1: Fetch weather data
def fetch_weather_data():
    url = f"https://api.openweathermap.org/data/2.5/weather?lat={LAT}&lon={LON}&units=metric&appid={API_KEY}"
    response = requests.get(url)
    
    if response.status_code == 200:
        with open('/tmp/weather_data.json', 'w') as f:
            f.write(response.text)
        print("Weather data fetched successfully.")
    else:
        raise Exception(f"Failed to fetch weather data. Status code: {response.status_code}")

fetch_weather_task = PythonOperator(
    task_id='fetch_weather_data',
    python_callable=fetch_weather_data,
    dag=dag
)

# Task 2: Fetch air pollution data
def fetch_air_pollution_data():
    url = f"http://api.openweathermap.org/data/2.5/air_pollution?lat={LAT}&lon={LON}&appid={API_KEY}"
    response = requests.get(url)
    
    if response.status_code == 200:
        with open('/tmp/air_pollution_data.json', 'w') as f:
            f.write(response.text)
        print("Air pollution data fetched successfully.")
    else:
        raise Exception(f"Failed to fetch air pollution data. Status code: {response.status_code}")

fetch_pollution_task = PythonOperator(
    task_id='fetch_air_pollution_data',
    python_callable=fetch_air_pollution_data,
    dag=dag
)

# Task 3: Fetch weather forecast data
def fetch_forecast_data():
    url = f"http://api.openweathermap.org/data/2.5/forecast?lat={LAT}&lon={LON}&appid={API_KEY}&units=metric"
    response = requests.get(url)

    if response.status_code == 200:
        with open('/tmp/forecast_data.json', 'w') as f:
            f.write(response.text)
        print("Forecast data fetched successfully.")
    else:
        raise Exception(f"Failed to fetch forecast data. Status code: {response.status_code}")

fetch_forecast_task = PythonOperator(
    task_id='fetch_forecast_data',
    python_callable=fetch_forecast_data,
    dag=dag
)

# Task 4: Process and analyze weather data
def process_weather_data():
    pg_hook = PostgresHook(postgres_conn_id='open_weather_map')

    with open('/tmp/weather_data.json', 'r') as f:
        weather_data = json.load(f)
    
    insert_cmd = """
        INSERT INTO weathers (
            city_name, temperature, feels_like, temp_min, temp_max, pressure,
            humidity, visibility, wind_speed, wind_deg, cloudiness,
            weather_main, weather_description, sunrise, sunset, dt
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
        ) RETURNING id;
    """

    temperature = weather_data['main']['temp']
    feels_like = weather_data['main']['feels_like']
    temp_min = weather_data['main']['temp_min']
    temp_max = weather_data['main']['temp_max']
    pressure = weather_data['main']['pressure']
    humidity = weather_data['main']['humidity']
    visibility = weather_data.get('visibility', 0) / 1000  # In kilometers
    wind_speed = weather_data['wind']['speed']
    wind_deg = weather_data['wind']['deg']
    cloudiness = weather_data['clouds']['all']
    weather_main = weather_data['weather'][0]['main']
    weather_description = weather_data['weather'][0]['description']
    sunrise = datetime.fromtimestamp(weather_data['sys']['sunrise'], jakarta_tz).strftime('%Y-%m-%d %H:%M:%S')
    sunset = datetime.fromtimestamp(weather_data['sys']['sunset'], jakarta_tz).strftime('%Y-%m-%d %H:%M:%S')
    dt = datetime.fromtimestamp(weather_data['dt'], jakarta_tz).strftime('%Y-%m-%d %H:%M:%S')

    data = (
        CITY_NAME, temperature, feels_like, temp_min, temp_max, pressure,
        humidity, visibility, wind_speed, wind_deg, cloudiness,
        weather_main, weather_description, sunrise, sunset, dt
    )

    result = pg_hook.get_first(insert_cmd, parameters=data)
    inserted_id = result[0] if result else None

    return inserted_id

process_weather_task = PythonOperator(
    task_id='process_weather_data',
    python_callable=process_weather_data,
    dag=dag
)

# Task 5: Process and analyze air pollution data
def process_air_pollution_data():
    pg_hook = PostgresHook(postgres_conn_id='open_weather_map')

    with open('/tmp/air_pollution_data.json', 'r') as f:
        air_pollution_data = json.load(f)
    
    insert_cmd = """
        INSERT INTO air_quality (
            city_name, aqi, co, no, no2, o3, so2, pm2_5, pm10, nh3, dt
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
        );
    """

    aqi = air_pollution_data['list'][0]['main']['aqi']
    co = air_pollution_data['list'][0]['components']['co']
    no = air_pollution_data['list'][0]['components']['no']
    no2 = air_pollution_data['list'][0]['components']['no2']
    o3 = air_pollution_data['list'][0]['components']['o3']
    so2 = air_pollution_data['list'][0]['components']['so2']
    pm2_5 = air_pollution_data['list'][0]['components']['pm2_5']
    pm10 = air_pollution_data['list'][0]['components']['pm10']
    nh3 = air_pollution_data['list'][0]['components']['nh3']
    dt = datetime.fromtimestamp(air_pollution_data['list'][0]['dt'], jakarta_tz).strftime('%Y-%m-%d %H:%M:%S')

    data = (
        CITY_NAME, aqi, co, no, no2, o3, so2, pm2_5, pm10, nh3, dt
    )

    pg_hook.run(insert_cmd, parameters=data)

process_pollution_task = PythonOperator(
    task_id='process_air_pollution_data',
    python_callable=process_air_pollution_data,
    dag=dag
)

# Task 6: Process and analyze forecast data
def process_forecast_data(**context):
    inserted_id = context['ti'].xcom_pull(task_ids='process_weather_data')

    pg_hook = PostgresHook(postgres_conn_id='open_weather_map')

    with open('/tmp/forecast_data.json', 'r') as f:
        forecast_data = json.load(f)

    insert_cmd = """
        INSERT INTO forecasts (
            weather_id, city_name, dt, temperature, feels_like, temp_min, temp_max,
            pressure, humidity, visibility, wind_speed, wind_deg, cloudiness,
            weather_main, weather_description
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
        );
    """

    for forecast in forecast_data['list']:
        dt = datetime.fromtimestamp(forecast['dt'], jakarta_tz).strftime('%Y-%m-%d %H:%M:%S')
        temperature = forecast['main']['temp']
        feels_like = forecast['main']['feels_like']
        temp_min = forecast['main']['temp_min']
        temp_max = forecast['main']['temp_max']
        pressure = forecast['main']['pressure']
        humidity = forecast['main']['humidity']
        visibility = forecast.get('visibility', 0) / 1000  # In kilometers
        wind_speed = forecast['wind']['speed']
        wind_deg = forecast['wind']['deg']
        cloudiness = forecast['clouds']['all']
        weather_main = forecast['weather'][0]['main']
        weather_description = forecast['weather'][0]['description']

        data = (
            inserted_id, CITY_NAME, dt, temperature, feels_like, temp_min, temp_max,
            pressure, humidity, visibility, wind_speed, wind_deg, cloudiness,
            weather_main, weather_description
        )

        pg_hook.run(insert_cmd, parameters=data)

process_forecast_task = PythonOperator(
    task_id='process_forecast_data',
    python_callable=process_forecast_data,
    provide_context=True,
    dag=dag
)

# Define task dependencies
fetch_weather_task >> process_weather_task >> fetch_forecast_task >> process_forecast_task
fetch_pollution_task >> process_pollution_task

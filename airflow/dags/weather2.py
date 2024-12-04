from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import requests
import json

# Default arguments
default_args = {
    'start_date': datetime(2024, 1, 1),
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

# API key dan lokasi
API_KEY = "6697ee55d2c2753b51e28a3e7e5a75fd"
LAT = "-7.2458"  # Latitude Surabaya
LON = "112.7383"  # Longitude Surabaya

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

# Task 3: Process and analyze weather data
def process_weather_data():
    with open('/tmp/weather_data.json', 'r') as f:
        weather_data = json.load(f)
    
    # Ekstraksi data cuaca
    temperature = weather_data['main']['temp']
    feels_like = weather_data['main']['feels_like']
    humidity = weather_data['main']['humidity']
    wind_speed = weather_data['wind']['speed']
    pressure = weather_data['main']['pressure']
    rain = weather_data.get('rain', {}).get('1h', 0)
    visibility = weather_data.get('visibility', 0) / 1000  # Dalam kilometer
    dew_point = weather_data['main'].get('dew_point', "N/A")
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    # Simpan ke file (append)
    with open('/tmp/weather_analysis.txt', 'a') as f:  # 'a' untuk append
        f.write(f"\nTimestamp: {timestamp}\n")
        f.write("Weather Analysis:\n")
        f.write(f"Temperature: {temperature}°C\n")
        f.write(f"Feels Like: {feels_like}°C\n")
        f.write(f"Humidity: {humidity}%\n")
        f.write(f"Wind Speed: {wind_speed} m/s\n")
        f.write(f"Pressure: {pressure} hPa\n")
        f.write(f"Rain (last 1h): {rain} mm\n")
        f.write(f"Visibility: {visibility} km\n")
        f.write(f"Dew Point: {dew_point}\n")
    
    print("Weather analysis appended to /tmp/weather_analysis.txt")

# Task 4: Process and analyze air pollution data
def process_air_pollution_data():
    with open('/tmp/air_pollution_data.json', 'r') as f:
        air_pollution_data = json.load(f)
    
    # Ekstraksi data polusi udara
    pm25 = air_pollution_data['list'][0]['components']['pm2_5']
    pm10 = air_pollution_data['list'][0]['components']['pm10']
    co = air_pollution_data['list'][0]['components']['co']
    no2 = air_pollution_data['list'][0]['components']['no2']
    o3 = air_pollution_data['list'][0]['components']['o3']
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    # Simpan ke file (append)
    with open('/tmp/air_pollution_analysis.txt', 'a') as f:  # 'a' untuk append
        f.write(f"\nTimestamp: {timestamp}\n")
        f.write("Air Pollution Analysis:\n")
        f.write(f"PM2.5 level: {pm25} µg/m³\n")
        f.write(f"PM10 level: {pm10} µg/m³\n")
        f.write(f"CO level: {co} µg/m³\n")
        f.write(f"NO2 level: {no2} µg/m³\n")
        f.write(f"O3 level: {o3} µg/m³\n")
    
    print("Air pollution analysis appended to /tmp/air_pollution_analysis.txt")

process_pollution_task = PythonOperator(
    task_id='process_air_pollution_data',
    python_callable=process_air_pollution_data,
    dag=dag
)

# Define task dependencies
fetch_weather_task >> process_weather_task
fetch_pollution_task >> process_pollution_task

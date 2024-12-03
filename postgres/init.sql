CREATE DATABASE open_weather_map;

\c open_weather_map;

CREATE TABLE IF NOT EXISTS weathers (
    id SERIAL PRIMARY KEY,
    city_name VARCHAR(100) NOT NULL,
    temperature FLOAT NOT NULL,
    feels_like FLOAT NOT NULL,
    temp_min FLOAT NOT NULL,
    temp_max FLOAT NOT NULL,
    pressure INT NOT NULL,
    humidity INT NOT NULL,
    visibility INT NOT NULL,
    wind_speed FLOAT NOT NULL,
    wind_deg INT NOT NULL,
    cloudiness INT NOT NULL,
    weather_main VARCHAR(50),
    weather_description VARCHAR(255),
    sunrise TIMESTAMP NOT NULL,
    sunset TIMESTAMP NOT NULL,
    dt TIMESTAMP NOT NULL
);

CREATE TABLE IF NOT EXISTS forecasts (
    id SERIAL PRIMARY KEY,
    weather_id INT NOT NULL REFERENCES weathers(id) ON DELETE CASCADE ON UPDATE CASCADE,
    city_name VARCHAR(100) NOT NULL,
    temperature FLOAT NOT NULL,
    feels_like FLOAT NOT NULL,
    temp_min FLOAT NOT NULL,
    temp_max FLOAT NOT NULL,
    pressure INT NOT NULL,
    humidity INT NOT NULL,
    visibility INT NOT NULL,
    wind_speed FLOAT NOT NULL,
    wind_deg INT NOT NULL,
    cloudiness INT NOT NULL,
    weather_main VARCHAR(50),
    weather_description VARCHAR(255),
    sunrise TIMESTAMP NOT NULL,
    sunset TIMESTAMP NOT NULL,
    dt TIMESTAMP NOT NULL
);

CREATE TABLE IF NOT EXISTS air_quality (
    id SERIAL PRIMARY KEY,
    city_name VARCHAR(100) NOT NULL,
    aqi INT NOT NULL,
    co FLOAT NOT NULL,
    no FLOAT NOT NULL,
    no2 FLOAT NOT NULL,
    o3 FLOAT NOT NULL,
    so2 FLOAT NOT NULL,
    pm2_5 FLOAT NOT NULL,
    pm10 FLOAT NOT NULL,
    nh3 FLOAT NOT NULL,
    dt TIMESTAMP NOT NULL
);
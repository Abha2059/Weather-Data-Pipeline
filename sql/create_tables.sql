CREATE DATABASE IF NOT EXISTS weather_db;

USE weather_db;

CREATE TABLE IF NOT EXISTS weather_data (
    id INT NOT NULL AUTO_INCREMENT,
    city VARCHAR(100) NOT NULL,
    country VARCHAR(10),
    latitude DECIMAL(10,6),
    longitude DECIMAL(10,6),
    temperature DECIMAL(5,2),
    feels_like DECIMAL(5,2),
    humidity INT,
    pressure INT,
    wind_speed DECIMAL(5,2),
    precipitation_1h DECIMAL(6,2),
    weather_condition VARCHAR(50),
    weather_description VARCHAR(100),
    timestamp BIGINT,
    date_time DATETIME,
    temperature_category VARCHAR(20),
    weather_date DATE NOT NULL,

    PRIMARY KEY (id),

    UNIQUE KEY unique_city_date (city, weather_date)
);
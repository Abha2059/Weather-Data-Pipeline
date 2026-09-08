USE weather_db;


-- 1. View all weather data

SELECT *
FROM weather_data
ORDER BY weather_date DESC;


-- 2. Average temperature by city

SELECT
    city,
    ROUND(AVG(temperature), 2) AS avg_temperature
FROM weather_data
GROUP BY city
ORDER BY avg_temperature DESC;


-- 3. Maximum and minimum temperature by city

SELECT
    city,
    MAX(temperature) AS max_temperature,
    MIN(temperature) AS min_temperature
FROM weather_data
GROUP BY city;


-- 4. Average humidity by city

SELECT
    city,
    ROUND(AVG(humidity), 2) AS avg_humidity
FROM weather_data
GROUP BY city
ORDER BY avg_humidity DESC;


-- 5. Average wind speed by city

SELECT
    city,
    ROUND(AVG(wind_speed), 2) AS avg_wind_speed
FROM weather_data
GROUP BY city
ORDER BY avg_wind_speed DESC;


-- 6. Total precipitation by city

SELECT
    city,
    ROUND(SUM(COALESCE(precipitation_1h, 0)), 2) AS total_precipitation
FROM weather_data
GROUP BY city
ORDER BY total_precipitation DESC;


-- 7. Daily temperature trend

SELECT
    weather_date,
    city,
    ROUND(AVG(temperature), 2) AS avg_temperature
FROM weather_data
GROUP BY weather_date, city
ORDER BY weather_date, city;


-- 8. City comparison

SELECT
    city,
    ROUND(AVG(temperature), 2) AS avg_temperature,
    ROUND(AVG(humidity), 2) AS avg_humidity,
    ROUND(AVG(pressure), 2) AS avg_pressure,
    ROUND(AVG(wind_speed), 2) AS avg_wind_speed
FROM weather_data
GROUP BY city
ORDER BY avg_temperature DESC;
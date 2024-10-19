CREATE DATABASE weather_data;
USE weather_data;

CREATE TABLE weather_summary (
    id INT AUTO_INCREMENT PRIMARY KEY,
    city VARCHAR(255),
    date DATE,
    avg_temp FLOAT,
    max_temp FLOAT,
    min_temp FLOAT,
    dominant_condition VARCHAR(255)
);

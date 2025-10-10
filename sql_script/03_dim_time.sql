CREATE TABLE IF NOT EXISTS dim_time(
    id_time INT PRIMARY KEY,
    day INT,
    month INT,
    year INT,
    semester INT,
    date date UNIQUE
);
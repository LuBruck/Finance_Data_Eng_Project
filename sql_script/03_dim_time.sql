CREATE TABLE IF NOT EXISTS dim_time(
    id_time INT PRIMARY KEY,
    day INT NOT NULL,
    month INT NOT NULL,
    year INT NOT NULL,
    semester INT NOT NULL,
    date date UNIQUE NOT NULL
);
CREATE TABLE IF NOT EXISTS fact_individual_cash(
    id_individual_cash int PRIMARY KEY,
    value FLOAT,
    source VARCHAR(100),
    id_person INT,
    FOREIGN KEY(id_person) REFERENCES dim_person(id_person),
    id_time INT,
    FOREIGN KEY(id_time) REFERENCES dim_time(id_time),
    id_championship INT,
    FOREIGN KEY(id_championship) REFERENCES dim_championship(id_championship)
);
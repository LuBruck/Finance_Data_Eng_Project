CREATE TABLE IF NOT EXISTS fact_individual_cash(
    id_individual_cash int PRIMARY KEY,
    value FLOAT,
    origen VARCHAR(100),
    cpf INT,
    FOREIGN KEY(cpf) REFERENCES dim_person(cpf),
    id_time INT,
    FOREIGN KEY(id_time) REFERENCES dim_time(id_time),
    id_championship INT,
    FOREIGN KEY(id_championship) REFERENCES dim_championship(id_championship)
);
CREATE TABLE IF NOT EXISTS fact_monthly_fee (
    id_monthly_fee INT PRIMARY KEY AUTO_INCREMENT,
    value FLOAT, 
    status VARCHAR(100),
    obs VARCHAR(100),
    cpf INT, 
    FOREIGN KEY(cpf) REFERENCES dim_person(cpf),
    id_time INT, 
    FOREIGN KEY(id_time) REFERENCES dim_time(id_time)
);
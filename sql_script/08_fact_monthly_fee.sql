CREATE TABLE IF NOT EXISTS fact_monthly_fee (
    id_monthly_fee INT PRIMARY KEY AUTO_INCREMENT,
    value FLOAT, 
    status VARCHAR(100),
    obs VARCHAR(100),
    id_person INT, 
    FOREIGN KEY(id_person) REFERENCES dim_person(id_person),
    id_time INT, 
    FOREIGN KEY(id_time) REFERENCES dim_time(id_time)
);
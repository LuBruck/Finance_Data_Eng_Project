CREATE TABLE IF NOT EXISTS dim_person(
    cpf INT PRIMARY KEY AUTO_INCREMENT,
    first_name VARCHAR(100) NOT NULL,
    last_name VARCHAR(100) NOT NULL,
    id_category INT,
    FOREIGN KEY(id_category) REFERENCES dim_category(id_category)
);
CREATE TABLE IF NOT EXISTS dim_person(
    id_person INT PRIMARY KEY AUTO_INCREMENT,
    cpf VARCHAR(15) NOT NULL,
    full_name VARCHAR(200) NOT NULL,
    first_name VARCHAR(100) NOT NULL,
    last_name VARCHAR(100) NOT NULL,
    id_category INT,
    FOREIGN KEY(id_category) REFERENCES dim_category(id_category)
);
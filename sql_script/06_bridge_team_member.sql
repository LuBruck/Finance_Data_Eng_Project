CREATE TABLE IF NOT EXISTS bridge_team_member(
    cpf INT,
    FOREIGN KEY(cpf) REFERENCES dim_person(cpf),
    id_championship INT,
    FOREIGN KEY(id_championship) REFERENCES dim_championship(id_championship),
    id_team INT,
    FOREIGN KEY(id_team) REFERENCES dim_team(id_team)
);
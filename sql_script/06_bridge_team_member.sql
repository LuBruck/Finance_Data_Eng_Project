CREATE TABLE IF NOT EXISTS bridge_team_member(
    id_person INT,
    FOREIGN KEY(id_person) REFERENCES dim_person(id_person),
    id_championship INT,
    FOREIGN KEY(id_championship) REFERENCES dim_championship(id_championship),
    id_team INT,
    FOREIGN KEY(id_team) REFERENCES dim_team(id_team)
);
import pandas as pd
import numpy as np
import pymysql
import os
from dotenv import load_dotenv

load_dotenv()


def insert_time_data(connection : pymysql.Connect, start_date:str, end_date:str):
    dates = pd.date_range(start=start_date, end=end_date)
    day = dates.day
    month = dates.month
    year = dates.year
    semester = ((dates.month - 1) // 6) + 1
    
    with connection.cursor() as cursor:
        sql = (
            'INSERT IGNORE INTO dim_time(day, month, year, semester, date) '
            'VALUES '
            '(%(day)s, %(month)s, %(year)s, %(semester)s, %(data)s)'
        )

        data = [
            {
                'day': day[i],
                'month': month[i],
                'year': year[i],
                'semester': semester[i],
                'data': dates[i].strftime('%Y-%m-%d')
            }
        for i in range(len(dates))
        ]
        cursor.executemany(sql, data)

    with connection.cursor() as cursor:
        sql =(
            'SELECT * from dim_time '
            'WHERE date BETWEEN %s and %s '
        )
        dt = (start_date, end_date)
        cursor.execute(sql, dt)
        result = cursor.fetchall()
    return result

def load_dim_time(connection : pymysql.Connect, start_date:str, end_date:str):
    with connection.cursor() as cursor:
            sql = (
                'SELECT COUNT(*) '
                'FROM dim_time '
                'WHERE `date` BETWEEN %s and %s'
            )
            cursor.execute(sql, (start_date, end_date))
            cout_dates = cursor.fetchone()

    if cout_dates['COUNT(*)'] > 0:
        print("as datas indicadas já estao na tabela")
        return
    else:
        print("Datas adicionadas na tabela!!")
        result = insert_time_data(connection, start_date, end_date)
    return result

def _load_generic_dimension(connection : pymysql.Connect,table : str , column : str,data : tuple):

    num_columns = len(column)
    value_placeholders = ', '.join(['%s'] * num_columns)
    with connection.cursor() as cursor:
        sql = (
            f'INSERT IGNORE INTO {table}({column}) '
            'VALUES (%s) '
        )
        cursor.executemany(sql, data)

    with connection.cursor() as cursor:
        sql_select = (
            f'SELECT * FROM {table} '
        )
        cursor.execute(sql_select)
        result = cursor.fetchall()
    return result


def load_dim_category(connection : pymysql.Connect, categorys : tuple):
    """Load membry categorys in the table dim_category"""
    return _load_generic_dimension(
        connection=connection, 
        table='dim_category',
        column='name',
        data=categorys)

def load_dim_championship(connection : pymysql.Connect, championship : tuple):
    """ Load the championships name in the table dim_championship"""
    a = _load_generic_dimension(
       connection=connection,
       table='dim_championship',
       column='name',
       data=championship
   )
    # data = [item.('-')[-1]
    #         for item in championship]
    # print(championship)
    return a

def load_dim_team(connection : pymysql.Connect, team : tuple):
    """Load the name of the teams in the table dim_team"""
    return _load_generic_dimension(
       connection=connection,
       table='dim_team',
       column='name',
       data=team
   )

def _lookup_id(conecction: pymysql.Connect, table_name , key, id_name):

    sql = f"SELECT {key}, {id_name} FROM {table_name}"
    with conecction.cursor() as cursor:
        cursor.execute(sql)
        result = cursor.fetchall()


        df_lookup = pd.DataFrame(result, columns=[key, id_name])
        df_lookup[key] = df_lookup[key].astype(str)
        return df_lookup.set_index(key)[id_name].to_dict()

def load_dim_person(connection: pymysql.Connect, path_cvs : str):
    df = pd.read_csv(path_cvs, dtype=str)
    ids_lookup = _lookup_id(
        connection,
        'dim_category',
        'name',
        'id_category'
    )
    df['id_category'] = df['Categoria'].map(ids_lookup)

    values = df[["Nome", "CPF", "Categoria", "Primeiro Nome", "Ultimo Nome", "id_category"]].to_dict('records')

    
    with connection.cursor() as cursor:
        
        sql = (
            'INSERT INTO dim_person(cpf, full_name, first_name, last_name, id_category) '
            'VALUES (%(CPF)s, %(Nome)s, %(Primeiro Nome)s, %(Ultimo Nome)s, %(id_category)s) '
            'ON DUPLICATE KEY UPDATE '
            'id_category = VALUES(id_category) '
        )
        cursor.executemany(sql, values)

    with connection.cursor() as cursor:
        cursor.execute(
            'SELECT * FROM dim_person '
        )
        result = cursor.fetchall()
    return result

def get_unique_values(path: str, column_name : str) -> tuple:

    dt = pd.read_csv(path, usecols=[column_name], dtype=str)

    unique_values = (
        dt[column_name]
        .str.strip()
        .str.title()
        .dropna()
        .drop_duplicates()
        .tolist()
        )
    return unique_values

def load_bridge_team_member(connection: pymysql.Connect, path_team_person: str,
                            path_championship_team: str):

    person_id_map = _lookup_id(connection, 'dim_person', 'cpf', 'id_person')
    team_id_map = _lookup_id(connection, 'dim_team', 'name', 'id_team')
    championship_id_map = _lookup_id(connection, 'dim_championship', 'name', 'id_championship')
    
    dt1 = pd.read_csv(path_team_person, dtype=str)
    dt2 = pd.read_csv(path_championship_team, dtype=str)


    dt_merg = pd.merge(dt1[['Nome', 'CPF', 'Nome_Time']],
                       dt2[['Nome_Time', 'Nome_Campeonato']],
                       on="Nome_Time",
                       how='inner',
                       sort='Nome'
                       )

    dt_bridge = pd.DataFrame()
    dt_bridge['id_person'] = dt_merg['CPF'].map(person_id_map)
    dt_bridge['id_team'] = dt_merg['Nome_Time'].map(team_id_map)
    dt_bridge['id_championship'] = dt_merg['Nome_Campeonato'].map(championship_id_map)
    dt_bridge = dt_bridge.to_dict(orient='records')

        
    sql = (
        'INSERT IGNORE INTO bridge_team_member (id_person, id_team, id_championship) '
        'VALUES (%(id_person)s, %(id_team)s, %(id_championship)s)'
    )
    with connection.cursor() as cursor:
        cursor.executemany(sql, dt_bridge)


def load_fact_monthly(connection: pymysql.Connect, path_monthlyfee : str, year : str):
    
    dt_monthly_fee = pd.read_csv(path_monthlyfee, dtype=str)

    person_id_map = _lookup_id(connection, 'dim_person', 'Full_Name', 'id_person')
    dt_monthly_fee["person_id"] = dt_monthly_fee["Nome"].map(person_id_map)

    with connection.cursor() as cursor:
            sql = (
                "SELECT date , id_time FROM dim_time "
                "WHERE day = 1 and year = %s "
            )

            cursor.execute(sql, year)
            result = cursor.fetchall()

    result = [{**a, 'date': a['date'].strftime('%m/%Y')} for a in result]
    dates = {item['date'] : item['id_time'] for item in result}
    dt_monthly_fee['id_time'] = dt_monthly_fee["Mes"].map(dates)
# [["Valor", "Status", "person_id", "id_time"]]
    dt_monthly_fee = dt_monthly_fee.to_dict('records')

    sql = (
        "INSERT IGNORE INTO fact_monthly_fee (value, status, id_person, id_time) "
        "VALUES (%(Valor)s, %(Status)s, %(person_id)s, %(id_time)s) "
    )

    with connection.cursor() as cursor:
        cursor.executemany(sql, dt_monthly_fee)

def load_fact_individual_cash(connection: pymysql.Connect, path_individual_cash : str):

    df_individual_cash = pd.read_csv(path_individual_cash, dtype=str)

    id_person_map = _lookup_id(connection, "dim_person", "CPF" ,"id_person")
    id_

if __name__ == "__main__":

    connection = pymysql.Connect(
        host=os.getenv("MYSQL_HOST"),
        database=os.getenv('MYSQL_DATABASE'),
        user=os.getenv('MYSQL_USER'),
        passwd=os.getenv('MYSQL_PASSWORD'),
        port=int(os.getenv('MYSQL_HOST_PORT')),
        cursorclass= pymysql.cursors.DictCursor
    )

    with connection:
        ...
        """
        ==================================================
                FASE 1: LOAD FIXED DIMENSIONS
        ==================================================
        """

        ldt = load_dim_time(connection, '2020-01-01', '2028-12-31')

        championship = get_unique_values("out/championship_team", "Nome_Campeonato")
        ldchap = load_dim_championship(connection, championship)

        categorys = get_unique_values("out/person_master", "Categoria")
        if len(categorys) > 0:
            ldcat = load_dim_category(connection, categorys)
            print("Categorias adicionadas")
        else:
            print("Nenhuma categoria na tabela")

        team = get_unique_values("out/team_person", "Nome_Time")
        ldteam = load_dim_team(connection, team)
        """a
        ==================================================
                FASE 2: LOAD DIM_PERSON AND 
                    BRIDGE_TEAM_MEMBER
        ==================================================
        """
      
        ldp = load_dim_person(connection, "out/person_master")
        
        load_bridge_team_member(connection, "out/team_person", "out/championship_team")


        """
        ==================================================
                FASE 3: LOAD FACTS
        ==================================================
        """

        # FAZER UMA FUNÇÃO PARA PEGAR O PRIMEIRO E ULTIMO ANO CADASTRADO NO BANCO DE DADOS
        # year = _ask_year("Whith year this monthly fee control is from?")
        year = "2025"
        load_fact_monthly(connection, 'out/ControleMensalidade' , year)

        connection.commit()
        



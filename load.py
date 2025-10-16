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
    id_dates = dates.strftime('%Y%m%d').astype(int)
    
    with connection.cursor() as cursor:
        sql = (
            'INSERT IGNORE INTO dim_time(id_time, day, month, year, semester, date) '
            'VALUES '
            '(%(id_dates)s, %(day)s, %(month)s, %(year)s, %(semester)s, %(data)s)'
        )

        data = [
            {
                'id_dates': id_dates[i],
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
            cout_dates = cursor.fetchone()[0]
            
    if cout_dates > 0:
        print("as datas indicadas já estao na tabela")
    else:
        print("Datas adicionadas na tabela!!")
        result = insert_time_data(connection, start_date, end_date)
    return result

def _load_generic_dimension(connection : pymysql.Connect,table : str , column : str,data : tuple):

    with connection.cursor() as cursor:
        sql = (
            f'INSERT IGNORE INTO {table}({column}) '
            'VALUES (%s) '
        )
        cursor.executemany(sql, data)

    with connection.cursor() as cursor:
        placeholders = ', '.join(['%s'] * len(data))
        sql_select = (
            f'SELECT * FROM {table} '
            f'WHERE {column} IN ({placeholders}) '
        )
        cursor.execute(sql_select, data)
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
    return _load_generic_dimension(
       connection=connection,
       table='dim_championship',
       column='name',
       data=championship
   )

def load_dim_team(connection : pymysql.Connect, team : tuple):
    """Load the name of the teams in the table dim_team"""
    return _load_generic_dimension(
       connection=connection,
       table='dim_team',
       column='name',
       data=team
   )

if __name__ == "__main__":

    connection = pymysql.Connect(
        host=os.getenv("MYSQL_HOST"),
        database=os.getenv('MYSQL_DATABASE'),
        user=os.getenv('MYSQL_USER'),
        passwd=os.getenv('MYSQL_PASSWORD'),
        port=int(os.getenv('MYSQL_HOST_PORT'))
    )

    with connection:
        ...
        # dt = load_dim_time(connection, '2020-01-01', '2028-12-31')


        # with connection.cursor() as cursor:
        #     cursor.execute('DELETE FROM dim_category')
        #     cursor.execute('ALTER TABLE dim_category AUTO_INCREMENT = 1')
        # connection.commit()

        # categorys = ("Atleta", "Associado", "Ex-Atleta")
        # dt = load_dim_category(connection, categorys)
        # for a in dt:
        #     print(a)
       

        # with connection.cursor() as cursor:
        #     cursor.execute('DELETE FROM dim_championship')
        #     cursor.execute('ALTER TABLE dim_championship AUTO_INCREMENT = 1')
        # connection.commit()

        # championship = ('CheerFest', 'Arena', 'Engenhariadas')
        # dt = load_dim_championship(connection, championship)
        # for a in dt:
        #     print(a)
       
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
        connection.commit()
    return result

if __name__ == "__main__":

    connection = pymysql.Connect(
        host=os.getenv("MYSQL_HOST"),
        database=os.getenv('MYSQL_DATABASE'),
        user=os.getenv('MYSQL_USER'),
        passwd=os.getenv('MYSQL_PASSWORD'),
        port=int(os.getenv('MYSQL_HOST_PORT'))
    )

    with connection:
        dt = load_dim_time(connection, '2020-01-01', '2028-12-31')
       
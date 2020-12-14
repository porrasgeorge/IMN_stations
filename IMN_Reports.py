import pyodbc
import pandas as pd
import numpy as np
from datetime import date
from dateutil.relativedelta import relativedelta
# from os import makedirs, path
# import logging


def lightnings_db_connection():
    server = '192.168.4.11'
    database = 'LightningStrikes'
    password = 'lightnings'
    username = 'lightnings'
    try:
        cnxn = pyodbc.connect(driver='{SQL Server}', host=server, database=database,
                          user=username, password=password, autocommit=True)
    except:
        print("Error de conexion con Base de Datos")
        return None
    return cnxn


def read_data_by_station(station_ID, initial_date, final_date):
    cnxn = lightnings_db_connection()
    if cnxn is not None:
        sql = f'exec [IMN_GetDataByStation] \'{station_ID}\', \'{initial_date}\', \'{final_date}\''
        try:
            data_df = pd.read_sql_query(sql, cnxn)
        except pyodbc.Error as err:
            print("ERROR..... ",err)
        cnxn.close()
    return data_df

def read_data_by_variable(var_ID, initial_date, final_date):
    cnxn = lightnings_db_connection()
    if cnxn is not None:
        sql = f'exec [IMN_GetDataByVar] \'{var_ID}\', \'{initial_date}\', \'{final_date}\''
        try:
            data_df = pd.read_sql_query(sql, cnxn)
        except pyodbc.Error as err:
            print("ERROR..... ",err)
        cnxn.close()
    return data_df

def read_last_updates():
    cnxn = lightnings_db_connection()
    if cnxn is not None:
        sql = f'exec [IMN_LastDateUpdate]'
        try:
            data_df = pd.read_sql_query(sql, cnxn)
        except pyodbc.Error as err:
            print("ERROR..... ",err)
        cnxn.close()
    return data_df


end_date = date.today()
initial_date = end_date - relativedelta(months=1)

# print(initial_date)
# print(end_date)

data_df = read_data_by_variable(3, initial_date, end_date)
print(data_df)

data_2 = data_df.pivot(index="DateTime", columns="Station", values="Value")
print(data_2)
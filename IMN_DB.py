import pyodbc
import pandas as pd
import numpy as np
from os import makedirs, path
from datetime import datetime
import logging

log_filename = f'logs\\datalog_{datetime.now().strftime("%Y_%m")}.log'
makedirs(path.dirname(log_filename), exist_ok=True)
logging.basicConfig(level=logging.INFO, filename=log_filename,
                    format='%(asctime)s Ln: %(lineno)d - %(message)s')

loc_stations = [{"Name":"Rio Zapote", "Link":"https://www.imn.ac.cr/especial/tablas/canalete.html", "Lat":10.8408, "Long": -85.0398, "Description":"Rio Zapote Canalete de Upala, Alajuela"},
    {"Name":"ADIFORT", "Link":"https://www.imn.ac.cr/especial/tablas/fortuna.html", "Lat":10.4675, "Long": -84.64694, "Description":"ADIFORT en la Fortuna de San Carlos"},
    {"Name":"Upala", "Link":"https://www.imn.ac.cr/especial/tablas/upala.html", "Lat":10.88361, "Long": -85.0725, "Description":"Upala, Alajuela"},
    {"Name":"Brasilia", "Link":"https://www.imn.ac.cr/especial/tablas/brasilia.html", "Lat":10.98325, "Long": -85.34722, "Description":"Finca Brasilia del Oro, Upala, Alajuela"},
    {"Name":"Aerodromo ZN", "Link":"https://www.imn.ac.cr/especial/tablas/aeropzn.html", "Lat":10.493, "Long": -85.4212, "Description":"Aeródromo Zona Norte, San Carlos, Alajuela"},
    {"Name":"Guatuso", "Link":"https://www.imn.ac.cr/especial/tablas/guatuso.html", "Lat":10.6861, "Long": -85.9103, "Description":"ASADA San Rafael de Guatuso de Alajuela"},
    {"Name":"Balsa", "Link":"https://www.imn.ac.cr/especial/tablas/balsa.html", "Lat":10.1744, "Long": -84.4969, "Description":"Balsa, San Ramón, Alajuela"},
    {"Name":"Coopevega", "Link":"https://www.imn.ac.cr/especial/tablas/coopevega.html", "Lat":10.72, "Long": -84.40, "Description":"Coopevega en Cutris de San Carlos, Alajuela"},
    {"Name":"El General", "Link":"https://www.imn.ac.cr/especial/tablas/hidroelectrica.html", "Lat":10.2036, "Long": -83.9332, "Description":"Hidroeléctrica El General, Horquetas de Sarapiquí, Heredia"},
    {"Name":"Horquetas", "Link":"https://www.imn.ac.cr/especial/tablas/horquetas.html", "Lat":10.9332, "Long": -83.9463, "Description":"Horquetas de Sarapiquí, Heredia"},
    {"Name":"Villa Blanca", "Link":"https://www.imn.ac.cr/especial/tablas/vblanca.html", "Lat":10.2028, "Long": -84.4839, "Description":"Hotel Villa Blanca, San Ramón, Alajuela"},
    {"Name":"ITCR SC", "Link":"https://www.imn.ac.cr/especial/tablas/staclara.html", "Lat":10.2028, "Long": -84.4839, "Description":"ITCR Santa Clara, Alajuela"},
    {"Name":"Las Delicias", "Link":"https://www.imn.ac.cr/especial/tablas/lasdelicias.html", "Lat":10.4442, "Long": -84.315, "Description":"Piñales Las Delicias en San Carlos, Alajuela"},
    {"Name":"San Vicente", "Link":"https://www.imn.ac.cr/especial/tablas/sanvicente.html", "Lat":10.2797, "Long": -84.3944, "Description":"San Vicente, Ciudad Quesada, Alajuela"},
    {"Name":"Tirimbina", "Link":"https://www.imn.ac.cr/especial/tablas/tirimbina.html", "Lat":10.4172, "Long": -84.1254, "Description":"El Bosque, Río Tirimbina, La Virgen de Sarapiquí, Heredia"},
    {"Name":"El Ceibo", "Link":"https://www.imn.ac.cr/especial/tablas/elceibo.html", "Lat":10.3275, "Long": -84.0786, "Description":"El Ceibo, La Virgen de Sarapiquí, Heredia"},
    {"Name":"Betania", "Link":"https://www.imn.ac.cr/especial/tablas/betania.html", "Lat":10.6445, "Long": -84.3821, "Description":"Betania, Cutris, San Carlos, Alajuela"},
    {"Name":"Puerto Viejo", "Link":"https://www.imn.ac.cr/especial/tablas/pvsarapiqui.html", "Lat":10.4451, "Long": -84.0050, "Description":"Comando Sarapiquí, Sarapiquí, Heredia"},
    {"Name":"Pozo Azul", "Link":"https://www.imn.ac.cr/especial/tablas/pozoazul.html", "Lat":10.3858, "Long": -84.1396, "Description":"Hotel Pozo Azul, Sarapiquí, Heredia"},
    {"Name":"La Rebusca", "Link":"https://www.imn.ac.cr/especial/tablas/larebusca.html", "Lat":10.4245, "Long": -84.0091, "Description":"Finca la Rebusca, Puerto Viejo, Heredia"},
    {"Name":"Los Chiles", "Link":"https://www.imn.ac.cr/especial/tablas/loschiles.html", "Lat":11.0316, "Long": -84.7115, "Description":"Comando Los Chiles de Alajuela"},
    {"Name":"Cano Negro", "Link":"https://www.imn.ac.cr/especial/tablas/canonegro.html", "Lat":10.8918, "Long": -84.7877, "Description":"Refugio Caño Negro, Los Chiles - Alajuela"},
    {"Name":"San Jorge", "Link":"https://www.imn.ac.cr/especial/tablas/sanjorge.html", "Lat":10.7237, "Long": -84.6751, "Description":"Saint George (San Jorge), Los Chiles, Alajuela"},
    {"Name":"San Gerardo", "Link":"https://www.imn.ac.cr/especial/tablas/sangerardo.html", "Lat":10.4159, "Long": -84.1581, "Description":"San Gerardo, La Virgen de Sarapiquí, Heredia"},
    {"Name":"Tenorio", "Link":"https://www.imn.ac.cr/especial/tablas/tenorio.html", "Lat":10.7156, "Long": -84.98691, "Description":"Parque Nacional Volcán Tenorio, Guatuso, Alajuela"},
    {"Name":"Abopac", "Link":"https://www.imn.ac.cr/especial/tablas/abopac.html", "Lat":9.8947, "Long": -84.3844, "Description":"Abopac, Orotina, Alajuela"},
    {"Name":"Daniel Oduber 07", "Link":"https://www.imn.ac.cr/especial/tablas/lib07.html", "Lat":10.5889, "Long": -85.5522, "Description":"Aeropuerto Daniel Oduber en Liberia - Pista 07"}]

def lightnings_db_connection():
    server = '192.168.4.11'
    database = 'LightningStrikes'
    password = 'lightnings'
    username = 'lightnings'
    try:
        cnxn = pyodbc.connect(driver='{SQL Server}', host=server, database=database,
                          user=username, password=password, autocommit=True)
    except:
        logging.info("Error de conexion con Base de Datos")
        return None
    return cnxn

def write_stations(stations):
    cnxn = lightnings_db_connection()
    if cnxn is not None:
        cursor = cnxn.cursor()
        sql = "{CALL InsertStation (?, ?, ?, ?, ?)}"
        for station in stations:
            values = (station["Name"], station["Link"], station["Description"], station["Lat"], station["Long"])
            try:
                cursor.execute(sql, values)
            except pyodbc.Error as err:
                logging.info(err)
    cnxn.close()

def read_stations(all=False): ## Only the active Ones
    cnxn = lightnings_db_connection()
    if all:
        sql = f"""select [id], [Name], [Desc], [Lat], [Long]  from IMN_WeatherStations order by id"""
    else:
        sql = f"""select [id], [Name], [Desc], [Lat], [Long] from IMN_WeatherStations where [Enabled] = 1 order by id"""

    if cnxn is not None:
        try:
            stations_df = pd.read_sql_query(sql, cnxn)
        except pyodbc.Error as err:
            logging.info('Error leyendo las Estaciones %s' % err)
            return None
        cnxn.close()
        return stations_df
    return None

def write_variables(variables):
    cnxn = lightnings_db_connection()
    if cnxn is not None:
        cursor = cnxn.cursor()
        sql = "{CALL InsertVariable (?)}"
        for variable in variables:
            if variable not in ["Fecha"]:
                values = (variable)
                try:
                    cursor.execute(sql, values)
                except pyodbc.Error as err:
                    logging.info(err)
        cnxn.close()

def read_variables():
    cnxn = lightnings_db_connection()
    sql = f"""select * from IMN_WeatherVars order by id"""
    if cnxn is not None:
        try:
            vars_df = pd.read_sql_query(sql, cnxn)
        except pyodbc.Error as err:
            logging.info('Error Leyendo las Variables %s' % err)
            return None
        cnxn.close()
        return vars_df
    return None


def write_data_values(station, station_data):
    station_name = station["Name"]
    cols = list(station_data.columns)
    cols = cols[1:]
    
    cnxn = lightnings_db_connection()
    if cnxn is not None:
        cursor = cnxn.cursor()
        sql = "{CALL InsertDataMeasure (?, ?, ?, ?)}"
        for _, row in station_data.iterrows():
            for i in cols:
                if not np.isnan(row[i]) :
                    values = (station_name, i, row["Fecha"], row[i])
                    try:
                        cursor.execute(sql, values)
                    except pyodbc.Error as err:
                        logging.info(err)
        cnxn.close()

##########################################

def read_data_by_station(station_ID, initial_date, final_date):
    cnxn = lightnings_db_connection()
    if cnxn is not None:
        sql = f'exec [IMN_GetDataByStation] \'{station_ID}\', \'{initial_date}\', \'{final_date}\''
        try:
            data_df = pd.read_sql_query(sql, cnxn)
        except pyodbc.Error as err:
            logging.info(err)
        cnxn.close()
        return data_df
    return None

def read_data_by_variable(var_ID, initial_date, final_date):
    cnxn = lightnings_db_connection()
    if cnxn is not None:
        sql = f'exec [IMN_GetDataByVar] \'{var_ID}\', \'{initial_date}\', \'{final_date}\''
        try:
            data_df = pd.read_sql_query(sql, cnxn)
        except pyodbc.Error as err:
            logging.info(err)
        cnxn.close()
        return data_df
    return None

def read_last_updates():
    cnxn = lightnings_db_connection()
    if cnxn is not None:
        sql = f'exec [IMN_LastDateUpdate]'
        try:
            data_df = pd.read_sql_query(sql, cnxn)
        except pyodbc.Error as err:
            logging.info(err)
        cnxn.close()
        return data_df
    return None


if __name__ == "__main__":
    write_stations(loc_stations)
    print(read_stations())
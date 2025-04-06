import requests
from bs4 import BeautifulSoup
import pandas as pd
import IMN_DB as db
import logging
import time
import datetime as dt

from io import StringIO
import string


def bs_get_table(soup_data, station, table_number = 1):
    all_tables = soup_data.findAll("table")
    last_table_str = str(all_tables[table_number - 1])
    last_table_str = ''.join(char for char in last_table_str if char in string.printable) ## or char == '\n')
    last_table_str = last_table_str.replace("NAN", "0.0")
    table_pd_list:list[pd.DataFrame] = pd.read_html(StringIO(last_table_str), header=0, decimal=",", thousands=".", )
    table_df:pd.DataFrame= None
    if isinstance(table_pd_list, list):
        table_df = table_pd_list[0]
        columns = list(table_df.columns)
        for idx, col in enumerate(columns):
            if col == 'Nivel_Rio_Zapote':
                columns[idx] = "Nivel"
                break
            if col == 'PRES_mb':
                columns[idx] = "P Atm"
                break
            if col == 'Pres_mb':
                columns[idx] = "P Atm"
                break
            if col == 'Rad_PAR':
                columns[idx] = "Rad PAR"
                break   
            if col == 'P.Atm':  
                columns[idx] = "P Atm"
                break
            if col == 'P Atm_Avg':
                columns[idx] = "P Atm"
                break
            if col == 'P. Atm':
                columns[idx] = "P Atm"
                break
            if idx> 0:
                columns[idx] = col.replace(" ", "_")
        table_df.columns = columns
        table_df.iloc[:,0] = table_df.iloc[:,0].str.replace(r"([ap]).[\s\S]{0,2}([m]).", r"\1\2", regex=True)
        table_df['Fecha'] = pd.to_datetime(table_df['Fecha'], format='%d/%m/%Y %I:%M %p', errors='ignore')
        table_df['Fecha'] = pd.to_datetime(table_df['Fecha'], format='%d/%m/%Y %H:%M:%S', errors='ignore')
        return table_df



def IMN_read_station_webpage(station, table_number = 1):
    print("Station: ", station["Name"])
    response = None
    try:
        response = requests.get(station["Link"], timeout=10)
    except (requests.ConnectionError, requests.Timeout):
        logging.info(f'{station["Name"]}: Connection Error or timeout')
    
    if response:
        if response.status_code == 200:
            response.encoding = 'utf-8'
            soup_data = BeautifulSoup(response.text, 'html.parser')
            table = bs_get_table(soup_data, station, table_number)
            return table
            
        else:
            logging.info(f'{station["Name"]}: Error cargando la pagina web')
            time.sleep(10)  ## wait to check for the next page
            return None
    logging.info(f'{station["Name"]}: Error, No hay respuesta de la pagina web')
    time.sleep(10)      ## wait to check for the next page
    return None


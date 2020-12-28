import requests
from bs4 import BeautifulSoup
import pandas as pd
import IMN_DB as db
import logging
import time

def bs_get_table(soup_data, station, table_number = 1):
    All_tables = soup_data.findAll("table")
    table = All_tables[table_number - 1]
    
    if table:
        table_data=list()
        for row in table.findAll("tr"):
            row_data=list()
            col_titles = row.findAll("th")
            if col_titles:
                for col in col_titles:
                    row_data.append(col.contents[0])
                table_data.append(row_data)
                
            col_data = row.findAll("td")
            if col_data:
                for col in col_data:
                    string_data = str(col.contents[0])
                    if ',' in string_data:
                        row_data.append(string_data.replace(".", "").replace(",", "."))
                    elif ".m." in string_data:
                        row_data.append(string_data.replace(".", ""))
                    else:
                        row_data.append(string_data)
                table_data.append(row_data)
        
        if len(table_data):
            if len(table_data[0]):
                if table_data[0][0] == "Fecha":
                    table_data[0] = [d.replace("PRES_mb", "P Atm") for d in table_data[0]]
                    table_data[0] = [d.replace("Rad_PAR", "Rad PAR") for d in table_data[0]]
                    table_data[0] = [d.replace("P.Atm", "P Atm") for d in table_data[0]]
                    table_data[0] = [d.replace("P. Atm", "P Atm") for d in table_data[0]]
                    table_data[0] = [d.replace("P Atm_Avg", "P Atm") for d in table_data[0]]
                    table_data[0] = [d.replace("Nivel_Rio_Zapote", "Nivel") for d in table_data[0]]    
                    data_df = pd.DataFrame(table_data[1:], columns=table_data[0])
                    try:
                        data_df["Fecha"] = pd.to_datetime(data_df["Fecha"], format="%d/%m/%Y %I:%M %p")
                    except ValueError:
                        try:
                            data_df["Fecha"] = pd.to_datetime(data_df["Fecha"], format="%d/%m/%Y %H:%M:%S")
                        except ValueError:
                            logging.info(f'{station["Name"]}: Error parsing dates')
                            return None
                    cols = data_df.columns
                    data_df[cols[1:]] = data_df[cols[1:]].apply(pd.to_numeric, errors='coerce')
                    return data_df
    return None


def IMN_read_station_webpage(station, table_number = 1):
    #print("Station: ", station["Name"])
    response = None
    try:
        response = requests.get(station["Link"], timeout=10)
    except (requests.ConnectionError, requests.Timeout):
        logging.info(f'{station["Name"]}: Connection Error or timeout')
    
    if response:
        if response.status_code == 200:
            soup_data = BeautifulSoup(response.text, 'html.parser')
            return bs_get_table(soup_data, station, table_number)
            
        else:
            logging.info(f'{station["Name"]}: Error cargando la pagina web')
            time.sleep(10)  ## wait to check for the next page
            return None
    logging.info(f'{station["Name"]}: Error, No hay respuesta de la pagina web')
    time.sleep(10)      ## wait to check for the next page
    return None




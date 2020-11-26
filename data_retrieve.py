import requests
from bs4 import BeautifulSoup
import pandas as pd
import IMN_DB as db

def IMN_read_station_webpage(station):
    print("Station: ", station["Name"])
    response = None
    try:
        response = requests.get(station["Link"], timeout=10).text
    except (requests.ConnectionError, requests.Timeout):
        print("Error")
    
    if response:
        soup_data = BeautifulSoup(response, 'html.parser')
        table = soup_data.find("table")
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
                        row_data.append(str(col.contents[0]).replace(".", "").replace(",", "."))

                    table_data.append(row_data)
            
            if len(table_data):
                if len(table_data[0]):
                    if table_data[0][0] == "Fecha":
                        data_df = pd.DataFrame(table_data[1:], columns=table_data[0])
                        data_df["Fecha"] = pd.to_datetime(data_df["Fecha"], format="%d/%m/%Y %I:%M %p")
                        cols = data_df.columns
                        data_df[cols[1:]] = data_df[cols[1:]].apply(pd.to_numeric, errors='coerce')
                        return data_df

    return None


stations = db.read_stations()

#print(stations["Name"])

for _, station in stations.iterrows():
    print(IMN_read_station_webpage(station))
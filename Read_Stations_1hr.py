import web_scraping as ws
import IMN_DB as db
import logging
import datetime as dt
from os import makedirs, path

log_filename = f'logs\\weather_datalog_readstations{dt.datetime.now().strftime("%Y_%m")}.log'
makedirs(path.dirname(log_filename), exist_ok=True)
logging.basicConfig(level=logging.INFO, filename=log_filename,
                    format='%(asctime)s Ln: %(lineno)d - %(message)s')


stations = db.read_stations() ## All stations to scrap data

# stations = db.loc_stations
# #stations = stations[0:2]
if stations is not None:
    for idx, station in stations.iterrows():

        try:
            if station["Name"] == "UCR Santa Cruz":
                station_data = ws.IMN_read_station_webpage(station, 2)    ## station data
            else:
                station_data = ws.IMN_read_station_webpage(station)    ## station data
        except Exception as e:
            print(f"Error reading station {station['Name']}: {e}")
            logging.info(f"Error reading station {station['Name']}: {e}")
            continue
        
        if station_data is not None:
            station_vars = list(station_data.columns)       ## list of all variables in this station
            try:
                db.write_variables(station_vars)                ## check all variables are in database (stored procedure)
                db.write_data_values(station, station_data)
            except Exception as e:
                print(f"Error writing data for station {station['Name']}: {e}")
                logging.info(f"Error writing data for station {station['Name']}: {e}")
                continue
        

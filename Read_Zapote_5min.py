import web_scraping as ws
import IMN_DB as db

stations = db.read_stations() ## All stations to scrap data
if stations is not None:
    for i, station in stations.iterrows():
        print(station["Name"])
        if station["Name"] == "Rio Zapote":
            station_data = ws.IMN_read_station_webpage(station)    ## station data
            if station_data is not None:
                station_vars = list(station_data.columns)       ## list of all variables in this station
                db.write_variables(station_vars)                ## check all variables are in database (stored procedure)
                db.write_data_values(station, station_data)
            break
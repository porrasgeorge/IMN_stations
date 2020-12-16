import IMN_DB as db
import pandas as pd
from datetime import date
from dateutil.relativedelta import relativedelta
from pathlib import Path

base_path = f'\\\\192.168.30.30\\Planificacion\\Estaciones_IMN'
Path(base_path).mkdir(parents=True, exist_ok=True)
end_date = date.today()
# initial_date = end_date - relativedelta(months=1)
initial_date = end_date - relativedelta(days=1)
date_range_5min = pd.DataFrame({'DateTime': pd.date_range(start=initial_date+relativedelta(minutes=5), end=end_date, freq="5min")})
date_range_1hr = pd.DataFrame({'DateTime': pd.date_range(start=initial_date+relativedelta(minutes=60), end=end_date, freq="60min")})

variables = db.read_variables()
stations = db.read_stations()
ordered_stations = list(stations["Name"])
print(ordered_stations)

# Create a Pandas Excel writer using XlsxWriter as the engine.
writer = pd.ExcelWriter(f'{base_path}\\IMN_{initial_date.strftime("%Y_%m_%d")}.xlsx', engine='xlsxwriter', datetime_format='dd/mm/yyyy hh:mm') # pylint: disable=abstract-class-instantiated
workbook  = writer.book

for _, variable in variables.iterrows():
    data_df = db.read_data_by_variable(variable["id"], initial_date, end_date)
    data_spreaded = data_df.pivot(index="DateTime", columns="Station", values="Value")
    station_in_df = sorted(set(ordered_stations) & set(data_df["Station"]), key = ordered_stations.index)
    data_spreaded = data_spreaded[station_in_df]
    if variable["Name"] == "Rio Zapote":
        data_spreaded = pd.merge(date_range_5min,data_spreaded,on='DateTime',how='left')
    else:
        data_spreaded = pd.merge(date_range_1hr,data_spreaded,on='DateTime',how='left')
    data_spreaded.to_excel(writer, sheet_name=variable["Name"], index=False) #, startrow=4)
    worksheet = writer.sheets[variable["Name"]]
    worksheet.set_column('A:A', 18)

writer.save()

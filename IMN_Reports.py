import IMN_DB as db
import pandas as pd
from datetime import date, datetime, timedelta
from dateutil.relativedelta import relativedelta
from pathlib import Path

def generate_day_report(report_date):
    base_path = f'\\\\192.168.30.30\\Planificacion\\Estaciones_IMN'
    Path(base_path).mkdir(parents=True, exist_ok=True)
    initial_datetime = datetime(report_date.year, report_date.month, report_date.day)
    end_datetime = initial_datetime + timedelta(days=1)
    date_range_5min = pd.DataFrame({'DateTime': pd.date_range(start=initial_datetime+relativedelta(minutes=5), end=end_datetime, freq="5min")})
    date_range_1hr = pd.DataFrame({'DateTime': pd.date_range(start=initial_datetime+relativedelta(minutes=60), end=end_datetime, freq="60min")})
    print(date_range_5min)
    variables = db.read_variables()
    stations = db.read_stations()
    ordered_stations = list(stations["Name"])

    # Create a Pandas Excel writer using XlsxWriter as the engine.
    writer = pd.ExcelWriter(f'{base_path}\\IMN_{report_date.strftime("%Y_%m_%d")}.xlsx', engine='xlsxwriter', datetime_format='dd/mm/yyyy hh:mm') # pylint: disable=abstract-class-instantiated

    for _, variable in variables.iterrows():
        data_df = db.read_data_by_variable(variable["id"], initial_datetime, end_datetime)
        data_spreaded = data_df.pivot(index="DateTime", columns="Station", values="Value")
        station_in_df = sorted(set(ordered_stations) & set(data_df["Station"]), key = ordered_stations.index)
        data_spreaded = data_spreaded[station_in_df]
        if variable["Name"] == "Nivel":
            data_spreaded = pd.merge(date_range_5min,data_spreaded,on='DateTime',how='left')
        else:
            data_spreaded = pd.merge(date_range_1hr,data_spreaded,on='DateTime',how='left')
        data_spreaded.to_excel(writer, sheet_name=variable["Name"], index=False) #, startrow=4)
        worksheet = writer.sheets[variable["Name"]]
        worksheet.set_column('A:A', 18)

    writer.save()

def generate_monthly_rain_report():
    base_path = f'\\\\192.168.30.30\\Planificacion\\Estaciones_IMN'
    Path(base_path).mkdir(parents=True, exist_ok=True)
    end_date = date.today()
    end_datetime = datetime(end_date.year, end_date.month, end_date.day)
    if end_date.day == 1:
        initial_date = end_date - relativedelta(months=1)
    else:
        initial_date = end_date
    initial_datetime = datetime(initial_date.year, initial_date.month, 1)
    print("Fecha inicial: ", initial_datetime)
    print("Fecha final: ", end_datetime)
    
    variables = db.read_variables()
    stations = db.read_stations()
    
    #only rain variable
    variables = variables[variables["id"] == 3]


    ordered_stations = list(stations["Name"])
    # Create a Pandas Excel writer using XlsxWriter as the engine.
    writer = pd.ExcelWriter(f'{base_path}\\IMN_{initial_date.strftime("%Y_%m")}_month.xlsx', engine='xlsxwriter', datetime_format='dd/mm/yyyy hh:mm') # pylint: disable=abstract-class-instantiated
    # workbook  = writer.book

    for _, variable in variables.iterrows():
        data_df = db.read_data_by_variable(variable["id"], initial_datetime, end_datetime)
        print(data_df)
        data_spreaded = data_df.pivot(index="DateTime", columns="Station", values="Value")
        #data_spreaded = data_df.pivot(columns="Station", values="Value")
        station_in_df = sorted(set(ordered_stations) & set(data_df["Station"]), key = ordered_stations.index)
        #sort the columns in df by ID
        data_spreaded = data_spreaded[station_in_df]
        #new column for grouping 
        data_spreaded["group_Date"] = data_spreaded.index - pd.Timedelta(minutes=1)
        #remove the time part
        data_spreaded["group_Date"] = pd.to_datetime(data_spreaded["group_Date"]).dt.to_period('D')
        data_grouped = data_spreaded.groupby(["group_Date"]).sum()

        # print(data_spreaded)
        # print(data_grouped)
        
        
        data_grouped.to_excel(writer, sheet_name=variable["Name"], index=True) #, startrow=4)
        worksheet = writer.sheets[variable["Name"]]
        worksheet.set_column('A:A', 18)

    writer.save()


# create and update last 3 days reports
report_date = date.today()
date_range = pd.DataFrame({'Date': pd.date_range(start=report_date-relativedelta(days=3), end=report_date-relativedelta(days=1), freq="D")})
for _, actual_date in date_range.iterrows():
    print(actual_date["Date"])
    generate_day_report(actual_date["Date"])

#genrate the montly resumed rain report
generate_monthly_rain_report()
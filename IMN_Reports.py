import IMN_DB
import pandas as pd
from datetime import date
from dateutil.relativedelta import relativedelta

end_date = date.today()
initial_date = end_date - relativedelta(months=1)
date_range_5min = pd.DataFrame({'DateTime': pd.date_range(start=initial_date, end=end_date-relativedelta(minutes=5), freq="5min")})
date_range_1hr = pd.DataFrame({'DateTime': pd.date_range(start=initial_date, end=end_date-relativedelta(minutes=60), freq="60min")})

variables = IMN_DB.read_variables()

# Create a Pandas Excel writer using XlsxWriter as the engine.
writer = pd.ExcelWriter(f'weather_data.xlsx', engine='xlsxwriter', datetime_format='dd/mm/yyyy hh:mm') # pylint: disable=abstract-class-instantiated
workbook  = writer.book

for _, variable in variables.iterrows():
    data_df = IMN_DB.read_data_by_variable(variable["id"], initial_date, end_date)
    data_spreaded = data_df.pivot(index="DateTime", columns="Station", values="Value")
    if variable["Name"] == "Rio Zapote":
        data_spreaded = pd.merge(date_range_5min,data_spreaded,on='DateTime',how='left')
    else:
        data_spreaded = pd.merge(date_range_1hr,data_spreaded,on='DateTime',how='left')
    data_spreaded.to_excel(writer, sheet_name=variable["Name"], index=False) #, startrow=4)
    worksheet = writer.sheets[variable["Name"]]
    worksheet.set_column('A:A', 18)

writer.save()

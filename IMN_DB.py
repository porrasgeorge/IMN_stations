import pyodbc
import pandas as pd

stations = [{"Name":"ADIFORT", "Link":"https://www.imn.ac.cr/especial/tablas/fortuna.html", "Lat":10.4675, "Long": -84.64694, "Description":"ADIFORT en la Fortuna de San Carlos"},
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
            {"Name":"Tenorio", "Link":"https://www.imn.ac.cr/especial/tablas/tenorio.html", "Lat":10.7156, "Long": -84.98691, "Description":"Parque Nacional Volcán Tenorio, Guatuso, Alajuela"}]

def lightnings_db_connection():
    server = '192.168.4.11'
    database = 'LightningStrikes'
    password = 'lightnings'
    username = 'lightnings'
    cnxn = pyodbc.connect(driver='{SQL Server}', host=server, database=database,
                          user=username, password=password, autocommit=True)
    return cnxn


def write_stations(stations):

    cnxn = lightnings_db_connection()
    cursor = cnxn.cursor()

    for station in stations:
        sql = f"""insert into IMN_WeatherStations (Name, Link, [Desc], Lat, Long) values ('{station["Name"]}', '{station["Link"]}', '{station["Description"]}', '{station["Lat"]}', '{station["Long"]}')"""
        try:
            cursor.execute(sql)
        except:
            print("Station already exists")
    cnxn.commit()
    cnxn.close()

def read_stations():
    cnxn = lightnings_db_connection()
    sql = f"""select * from IMN_WeatherStations"""
    try:
        stations_df = pd.read_sql_query(sql, cnxn)
    except pyodbc.Error as err:
        print('Error !!!!! %s' % err)
        return None
    cnxn.close()
    return stations_df


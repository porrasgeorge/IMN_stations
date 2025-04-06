import pyodbc
import pandas as pd
import numpy as np
from os import makedirs, path
import datetime as dt
import logging

log_filename = f'logs\\weather_datalog_{dt.datetime.now().strftime("%Y_%m")}.log'
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
    #{"Name":"Cano Negro", "Link":"https://www.imn.ac.cr/especial/tablas/canonegro.html", "Lat":10.8918, "Long": -84.7877, "Description":"Refugio Caño Negro, Los Chiles - Alajuela"},
    {"Name":"San Jorge", "Link":"https://www.imn.ac.cr/especial/tablas/sanjorge.html", "Lat":10.7237, "Long": -84.6751, "Description":"Saint George (San Jorge), Los Chiles, Alajuela"},
    {"Name":"San Gerardo", "Link":"https://www.imn.ac.cr/especial/tablas/sangerardo.html", "Lat":10.4159, "Long": -84.1581, "Description":"San Gerardo, La Virgen de Sarapiquí, Heredia"},
    {"Name":"Tenorio", "Link":"https://www.imn.ac.cr/especial/tablas/tenorio.html", "Lat":10.7156, "Long": -84.98691, "Description":"Parque Nacional Volcán Tenorio, Guatuso, Alajuela"},
    {"Name":"Abopac", "Link":"https://www.imn.ac.cr/especial/tablas/abopac.html", "Lat":9.8947, "Long": -84.3844, "Description":"Abopac, Orotina, Alajuela"},
    {"Name":"Daniel Oduber 07", "Link":"https://www.imn.ac.cr/especial/tablas/lib07.html", "Lat":10.5889, "Long": -85.5522, "Description":"Aeropuerto Daniel Oduber en Liberia - Pista 07"},
    {"Name":"Daniel Oduber 25", "Link":"https://www.imn.ac.cr/especial/tablas/lib25.html", "Lat":10.5933, "Long": -85.5378, "Description":"Aeropuerto Daniel Oduber en Liberia - Pista 25"},
    {"Name":"Juan Santamaria 07", "Link":"https://www.imn.ac.cr/especial/tablas/aijs07.html", "Lat":9.9892, "Long":-84.2183, "Description":"Aeropuerto Juan Santamaría - Pista 07"},
    {"Name":"Juan Santamaria 25", "Link":"https://www.imn.ac.cr/especial/tablas/aijs25.html", "Lat":9.9953, "Long":-84.2019, "Description":"Aeropuerto Juan Santamaría - Pista 25"},
    {"Name":"Aeropuerto Limon", "Link":"https://www.imn.ac.cr/especial/tablas/aeroplimon.html", "Lat":9.9619, "Long":-83.0253, "Description":"Aeropuerto de Limón"},
    {"Name":"Tobias Bolanos 09", "Link":"https://www.imn.ac.cr/especial/tablas/aitbp09.html", "Lat":9.9733, "Long":-84.1394, "Description":"Aeropuerto Tobías Bolaños en Pavas Oeste 09"},
    {"Name":"Tobias Bolanos 27", "Link":"https://www.imn.ac.cr/especial/tablas/pave.html", "Lat":0.0, "Long":0.0, "Description":"Aeropuerto Tobías Bolaños en Pavas"},
    {"Name":"Alfredo Volio Mata", "Link":"https://www.imn.ac.cr/especial/tablas/alfredovolio.html", "Lat":9.9086, "Long":-83.9548, "Description":"Alfredo Volio en Ochomogo de Cartago"},
    {"Name":"Altamira", "Link":"https://www.imn.ac.cr/especial/tablas/altamira.html", "Lat":9.0292, "Long":-83.0078, "Description":"Parque Nacional la Amistad, Buenos Aires, Puntarenas"},
    {"Name":"Altos Tablazo Higuito", "Link":"https://www.imn.ac.cr/especial/tablas/tablazo.html", "Lat":9.8367, "Long":-84.0550, "Description":"Deslizamiento el Tablazo, Desamparados, San José"},
    {"Name":"Aranjuez Pitahaya", "Link":"https://www.imn.ac.cr/especial/tablas/aranjuez.html", "Lat":10.0558, "Long":-84.8069, "Description":"Aranjuez, Pithaya, Puntarenas"},
    {"Name":"Arunachala", "Link":"https://www.imn.ac.cr/especial/tablas/arunachala.html", "Lat":9.4099, "Long":-83.8322, "Description":"Arunachala, Pérez Zeledón, San José"},
    {"Name":"ASADA Artola", "Link":"https://www.imn.ac.cr/especial/tablas/sardinal.html", "Lat":10.5067, "Long":-85.6958, "Description":"Sardinal de Carrillo en Guanacaste"},
    {"Name":"ASADA Guayabo", "Link":"https://www.imn.ac.cr/especial/tablas/guayabob.html", "Lat":10.7150, "Long":-85.2011, "Description":"Guayabo en la Fortuna de Bagaces de Guanacaste"},
    {"Name":"ASADA Pilangosta", "Link":"https://www.imn.ac.cr/especial/tablas/pilangosta.html", "Lat":10.0319, "Long":-85.4058, "Description":"ASADA Pilangosta en Hojancha en Guanacaste"},
    {"Name":"ASADA San Gabriel", "Link":"https://www.imn.ac.cr/especial/tablas/sgabriel.html", "Lat":9.7872, "Long":-84.1053, "Description":"Asada San Gabriel, Desamparados, San José"},
    {"Name":"ASADA San Jose de la Montana", "Link":"https://www.imn.ac.cr/especial/tablas/sjmontana.html", "Lat":10.2031, "Long":-85.7031, "Description":"San José de la Montaña en Santa Cruz de Guanacaste"},
    {"Name":"ASADA Santa Marta", "Link":"https://www.imn.ac.cr/especial/tablas/stamarta.html", "Lat":9.9297, "Long":-85.4303, "Description":"Santa Marta de Hojancha en Guanacaste"},
    {"Name":"Barco Quebrado - Garza", "Link":"https://www.imn.ac.cr/especial/tablas/garza.html", "Lat":9.9161, "Long":-85.6144, "Description":"Barco Quebradero - Garza, Nosara, Guanacaste"},
    {"Name":"Baru", "Link":"https://www.imn.ac.cr/especial/tablas/baru.html", "Lat":9.2714, "Long":-83.8814, "Description":"Barú, Pérez Zeledón"},
    {"Name":"Belen", "Link":"https://www.imn.ac.cr/especial/tablas/belen.html", "Lat":9.9750, "Long":-84.1856, "Description":"Belén, Heredia"},
    {"Name":"Cabuya", "Link":"https://www.imn.ac.cr/especial/tablas/cabuya.html", "Lat":9.5938, "Long":-85.0933, "Description":"Cabuya, Cóbano, Puntarenas"},
    {"Name":"Cafetalera El Indio", "Link":"https://www.imn.ac.cr/especial/tablas/sanvito.html", "Lat":8.8328, "Long":-82.9528, "Description":"San Vito, Coto Brus, Puntareas"},
    {"Name":"Canta Gallo", "Link":"https://www.imn.ac.cr/especial/tablas/cantagallo.html", "Lat":10.4969, "Long":-83.6713, "Description":"Canta Gallo, Cariari, Limón"},
    {"Name":"Caramba Farms", "Link":"https://www.imn.ac.cr/especial/tablas/roxana.html", "Lat":10.2686, "Long":-83.7464, "Description":"Caramba Farms, Roxana, Pococí"},
    {"Name":"CATIE", "Link":"https://www.imn.ac.cr/especial/tablas/catie.html", "Lat":9.8914, "Long":-83.6531, "Description":"CATIE, Turrialba, Cartago"},
    {"Name":"Cerro Buenavista", "Link":"https://www.imn.ac.cr/especial/tablas/cmuerte.html", "Lat":9.5600, "Long":-83.7536, "Description":"Cerro Buena Vista de Pérez Zeledón"},
    {"Name":"Cerro Burio", "Link":"https://www.imn.ac.cr/especial/tablas/burio.html", "Lat":9.8403, "Long":-84.1125, "Description":"El Burío, Aserrí, San José"},
    {"Name":"Cerro Chitaria", "Link":"https://www.imn.ac.cr/especial/tablas/chitaria.html", "Lat":9.8917, "Long":-84.1937, "Description":"Cerro Chitaria, Santa Ana, San José"},
    {"Name":"Cerro Chirripo", "Link":"https://www.imn.ac.cr/especial/tablas/chirripo.html", "Lat":9.4594, "Long":-83.5081, "Description":"Cerro Chiripó, Pérez Zeledón, San José"},
    {"Name":"Cerro Huacalito", "Link":"https://www.imn.ac.cr/especial/tablas/huacalito.html", "Lat":10.3930, "Long":-85.4128, "Description":"Huacalito de Filadelfia en Carrillo Guanacaste"},
    {"Name":"Cerro Juco", "Link":"https://www.imn.ac.cr/especial/tablas/juco.html", "Lat":9.7754, "Long":-83.8647, "Description":"Cerro Juco de Orosi en Paraiso de Cartago"},
    {"Name":"Chaguites", "Link":"https://www.imn.ac.cr/especial/tablas/chaguites.html", "Lat":10.0794, "Long":-84.1642, "Description":"Chagüites, Santa Bárbara, Heredia"},
    {"Name":"CIGEFI", "Link":"https://www.imn.ac.cr/especial/tablas/cigefi.html", "Lat":9.9364, "Long":-84.0453, "Description":"CIGEFI en Montes de Oca, San José"},
    {"Name":"Ciudad de los Ninos", "Link":"https://www.imn.ac.cr/especial/tablas/ciudadninos.html", "Lat":5.8342, "Long":-83.9211, "Description":"Ciudad de los Niños, Paraiso, Cartago"},
    {"Name":"Ciudad Judicial", "Link":"https://www.imn.ac.cr/especial/tablas/ciudadjudicial.html", "Lat":10.0069, "Long":-84.1639, "Description":"Ciudad Judicial, San Joaquín, Heredia"},
    {"Name":"Copalchi", "Link":"https://www.imn.ac.cr/especial/tablas/copalchi.html", "Lat":11.2000, "Long":-85.6186, "Description":"Copalchí, La Cruz, Guanacaste"},
    {"Name":"Coto 49", "Link":"https://www.imn.ac.cr/especial/tablas/coto49.html", "Lat":8.6312, "Long":-82.9669, "Description":"Coto 49, Corredores, Puntarenas"},
    {"Name":"Dulce Nombre", "Link":"https://www.imn.ac.cr/especial/tablas/puravida.html", "Lat":10.0958, "Long":-85.4783, "Description":"Dulce Nombre, Nicoya, Guanacaste"},
    {"Name":"EARTH", "Link":"https://www.imn.ac.cr/especial/tablas/earth.html", "Lat":10.2097, "Long":-83.5942, "Description":"Earth - Guácimo Limón"},
    {"Name":"El Corral Palo Verde", "Link":"https://www.imn.ac.cr/especial/tablas/paloverde.html", "Lat":10.3475, "Long":-85.3511, "Description":"Palo Verde en Bagaces, Guanacaste"},
    {"Name":"El Rodeo", "Link":"https://www.imn.ac.cr/especial/tablas/tarrazu.html", "Lat":9.6762, "Long":-84.0184, "Description":"El Rodeo, Tarrazú, San José"},
    {"Name":"Fabio Baudrit", "Link":"https://www.imn.ac.cr/especial/tablas/fabio.html", "Lat":10.0050, "Long":-84.2656, "Description":"Fabio Baudrit - UCR, La Garita, Alajuela"},
    {"Name":"Finca Damas", "Link":"https://www.imn.ac.cr/especial/tablas/damas.html", "Lat":9.4953, "Long":-84.2147, "Description":"Finca Damas, Quepos, Puntarenas"},
    {"Name":"Finca El Carmen", "Link":"https://www.imn.ac.cr/especial/tablas/elcarmen.html", "Lat":10.1994, "Long":-83.4814, "Description":"El Carmen, Siquirres, Limón"},
    {"Name":"Finca El Patio", "Link":"https://www.imn.ac.cr/especial/tablas/jimenez.html", "Lat":8.6010, "Long":-83.4312, "Description":"Finca El Patio, Puerto Jiménez, Golfito, Puntarenas"},
    {"Name":"Finca La Ceiba", "Link":"https://www.imn.ac.cr/especial/tablas/laceiba.html", "Lat":10.1114, "Long":-85.3176, "Description":"Finca la Ceiba, Nicoya, Guanacaste"},
    {"Name":"Finca Los Macaya", "Link":"https://www.imn.ac.cr/especial/tablas/macaya.html", "Lat":9.9686, "Long":-83.9713, "Description":"Finca los Macayas, Goicoechea, San José"},
    {"Name":"Fundacion Neotropica", "Link":"https://www.imn.ac.cr/especial/tablas/neotropica.html", "Lat":8.6997, "Long":-83.5142, "Description":"Fundación Neotrópica, Osa, Puntarenas"},
    {"Name":"Guayabal Mastate", "Link":"https://www.imn.ac.cr/especial/tablas/gmastate.html", "Lat":9.9175, "Long":-84.5853, "Description":"Guayabal Mastate, Orotina, Alajuela"},
    {"Name":"Hacienda Mojica", "Link":"https://www.imn.ac.cr/especial/tablas/mojica.html", "Lat":10.4528, "Long":-85.1653, "Description":"Hacienda Mojica, Bagaces, Guanacaste"},
    {"Name":"Hacienda Taboga", "Link":"https://www.imn.ac.cr/especial/tablas/taboga.html", "Lat":10.3458, "Long":-85.1775, "Description":"Hacienda Taboga, Cañas, Guanacaste"},
    {"Name":"Hotel Mawamba", "Link":"https://www.imn.ac.cr/especial/tablas/mawamba.html", "Lat":10.5511, "Long":-83.5075, "Description":"Hotel Mawamba, Tortuguero, Pococí, Limón"},
    {"Name":"IMN Aranjuez", "Link":"https://www.imn.ac.cr/especial/tablas/imn.html", "Lat":9.9364, "Long":-84.0697, "Description":"IMN Aranjuez en San José"},
    {"Name":"ITCR Cartago", "Link":"https://www.imn.ac.cr/especial/tablas/cartagotec.html", "Lat":9.8522, "Long":-83.9086, "Description":"Instituto Tecnológico de Costa Rica, Cartago"},
    {"Name":"Iztaru", "Link":"https://www.imn.ac.cr/especial/tablas/iztaru.html", "Lat":9.8893, "Long":-83.9689, "Description":"Iztarú Tres Rios Cartago"},
    {"Name":"Jaboncillal", "Link":"https://www.imn.ac.cr/especial/tablas/jaboncillal.html", "Lat":9.9558, "Long":-83.9811, "Description":"Jaboncillal, Goichoechea, San José"},
    {"Name":"La Argentina de Grecia", "Link":"https://www.imn.ac.cr/especial/tablas/argentina.html", "Lat":10.0392, "Long":-84.3487, "Description":"La Argentina de Grecia, Alajuela"},
    {"Name":"La Cruz", "Link":"https://www.imn.ac.cr/especial/tablas/lacruz.html", "Lat":11.0536, "Long":-85.6331, "Description":"La Cruz de Guanacaste"},
    {"Name":"La Ligia", "Link":"https://www.imn.ac.cr/especial/tablas/laligia.html", "Lat":9.3046, "Long":-84.3336, "Description":"La Ligia, Parrita, Puntarenas"},
    {"Name":"La Lucha", "Link":"https://www.imn.ac.cr/especial/tablas/lalucha.html", "Lat":9.7367, "Long":-84.0006, "Description":"La Lucha, León Cortés, San José"},
    {"Name":"Laguna Fraijanes", "Link":"https://www.imn.ac.cr/especial/tablas/fraijanes.html", "Lat":10.1372, "Long":-84.1933, "Description":"Laguna Fraijanes, Alajuela, Alajuela"},
    {"Name":"Laguna Volcan Poas", "Link":"https://www.imn.ac.cr/especial/tablas/vpoas.html", "Lat":10.1903, "Long":-83.2317, "Description":"Laguna, Volcán Poás, Alajuela"},
    {"Name":"Las Brisas Sabalito", "Link":"https://www.imn.ac.cr/especial/tablas/lasbrisas.html", "Lat":8.8774, "Long":-82.8910, "Description":"Las Brisas, Coto Brus, Puntarenas"},
    {"Name":"Laurel", "Link":"https://www.imn.ac.cr/especial/tablas/laurel.html", "Lat":8.4716, "Long":-82.8606, "Description":"Laurel, Corredores, Puntarenas"},
    {"Name":"Llano Grande", "Link":"https://www.imn.ac.cr/especial/tablas/llanogrande.html", "Lat":9.9454, "Long":-83.9217, "Description":"Llano Grande de Cartago"},
    {"Name":"Mangarica", "Link":"https://www.imn.ac.cr/especial/tablas/mangarica.html", "Lat":10.6056, "Long":85.4500, "Description":"Mangarica, Liberia Guanacaste"},
    {"Name":"Manzanillo", "Link":"https://www.imn.ac.cr/especial/tablas/manzanillo.html", "Lat":10.1994, "Long":-83.4814, "Description":"Manzanillo, Talamanca, Limón"},
    {"Name":"Maravilla Juan Vinas", "Link":"https://www.imn.ac.cr/especial/tablas/juanvinas.html", "Lat":9.8961, "Long":-83.7481, "Description":"Juan Viñas, Jiménez, Cartago"},
    {"Name":"Marina Pez Vela", "Link":"https://www.imn.ac.cr/especial/tablas/quepos.html", "Lat":9.4261, "Long":-84.1708, "Description":"Marina Pez Vela, Quepos"},
    {"Name":"Maritza Volcan Orosi", "Link":"https://www.imn.ac.cr/especial/tablas/maritza.html", "Lat":10.9588, "Long":-85.49575, "Description":"Reserve Biológica Maritza, Volcán Orosi, La Cruz, Guanacaste"},
    {"Name":"Mastate", "Link":"https://www.imn.ac.cr/especial/tablas/mastate.html", "Lat":9.9181, "Long":-84.5706, "Description":"Mastate, Orotina, Alajuela"},
    {"Name":"Miel La Guinea", "Link":"https://www.imn.ac.cr/especial/tablas/guinea.html", "Lat":10.4192, "Long":-85.4733, "Description":"Ingenio el Viejo, Filadelfia Guanacaste"},
    {"Name":"Montecarlo", "Link":"https://www.imn.ac.cr/especial/tablas/montecarlo.html", "Lat":9.3581, "Long":-83.6011, "Description":"Montecarlo, Pérez Zeledón, San José"},
    {"Name":"Monumento Nacional Guayabo", "Link":"https://www.imn.ac.cr/especial/tablas/guayabo.html", "Lat":9.9694, "Long":-83.6892, "Description":"Monumento Nacional Guayabo en Turrialba de Cartago"},
    {"Name":"Muelle Herradura", "Link":"https://www.imn.ac.cr/especial/tablas/herradura.html", "Lat":9.65025, "Long":-84.6665, "Description":"Herradura, Garabito, Puntarenas"},
    {"Name":"Museo Finca 6", "Link":"https://www.imn.ac.cr/especial/tablas/museo.html", "Lat":8.9106, "Long":-83.4793, "Description":"Museo Las Esferas, Sierpe de Osa, Puntarenas"},
    {"Name":"Oroceiba", "Link":"https://www.imn.ac.cr/especial/tablas/oroceiba.html", "Lat":9.8967, "Long":-84.6000, "Description":"Oroceiba, Orotina, Alajuela"},
    {"Name":"Pacayas", "Link":"https://www.imn.ac.cr/especial/tablas/pacayas.html", "Lat":9.9153, "Long":-83.8048, "Description":"Pacayas, Alvarado, Cartago"},
    {"Name":"Palo Alto Montezuma", "Link":"https://www.imn.ac.cr/especial/tablas/montezuma.html", "Lat":10.6646, "Long":-85.0804, "Description":"Palo Alto, Montezuma, Cañas, Guanacaste"},
    {"Name":"Paquera", "Link":"https://www.imn.ac.cr/especial/tablas/paquera.html", "Lat":9.8214, "Long":-84.9383, "Description":"Paquera, Puntarenas"},
    {"Name":"Patio de Agua", "Link":"https://www.imn.ac.cr/especial/tablas/patio.html", "Lat":9.9769, "Long":-83.9700, "Description":"Patio de Agua, Vásquez de Coronado San José"},
    {"Name":"PINDECO", "Link":"https://www.imn.ac.cr/especial/tablas/pindeco.html", "Lat":9.1462, "Long":-83.3362, "Description":"Pindeco, Buenos Aires, Puntarenas"},
    {"Name":"Pococi Guapiles", "Link":"https://www.imn.ac.cr/especial/tablas/guapiles.html", "Lat":10.2117, "Long":-83.7720, "Description":"Finca Diamantes, Guápiles, Limón"},
    {"Name":"Progreso Cuajiniquil", "Link":"https://www.imn.ac.cr/especial/tablas/cuajiniquil.html", "Lat":10.0428, "Long":-85.6694, "Description":"Progreso Cuajiniquil en Santa Cruz de Guanacaste"},
    {"Name":"Puerto Vargas", "Link":"https://www.imn.ac.cr/especial/tablas/ptovargas.html", "Lat":9.7317, "Long":-82.8150, "Description":"Puerto Vargas Talamanca, Limón"},
    {"Name":"Puesto Negritos", "Link":"https://www.imn.ac.cr/especial/tablas/negritos.html", "Lat":10.3950, "Long":-85.3244, "Description":"Sitio el Aromal, Bagaces, Guanacaste"},
    {"Name":"Rain Forest Braulio Carrillo", "Link":"https://www.imn.ac.cr/especial/tablas/rainforest.html", "Lat":10.1759, "Long":-83.9179, "Description":"Rain Forest, Braulio Carrillo, Guápiles, Pococí, Limón"},
    {"Name":"RECOPE La Garita", "Link":"https://www.imn.ac.cr/especial/tablas/lagarita.html", "Lat":10.00528, "Long":-84.2958, "Description":"RECOPE La Garita, Alajuela"},
    {"Name":"Refugio Cipanci", "Link":"https://www.imn.ac.cr/especial/tablas/cipanci.html", "Lat":10.2119, "Long":-85.2344, "Description":"Refugio Nacional de Vida Silvestre Cipancí, Cañas, Guanacaste"},
    {"Name":"Reserva Biologica Greg Gund", "Link":"https://www.imn.ac.cr/especial/tablas/greggund.html", "Lat":8.4112, "Long":-83.3204, "Description":"Greg Gund, Golfito, Puntarenas"},
    {"Name":"Rio Claro", "Link":"https://www.imn.ac.cr/especial/tablas/rioclaro.html", "Lat":8.6741, "Long":-83.0621, "Description":"Río Claro, Golfito, Puntarenas"},
    {"Name":"San Jose Pinilla", "Link":"https://www.imn.ac.cr/especial/tablas/pinilla.html", "Lat":10.2601, "Long":-85.8378, "Description":"Hacienda Pinilla - Santa Cruz, Guanacaste"},
    {"Name":"San Mateo", "Link":"https://www.imn.ac.cr/especial/tablas/sanmateo.html", "Lat":9.9317, "Long":-84.5192, "Description":"San Mateo, Alajuela"},
    {"Name":"San Miguel", "Link":"https://www.imn.ac.cr/especial/tablas/naranjo.html", "Lat":10.0714, "Long":-84.1994, "Description":"San Miguel de Naranjo, Alajuela"},
    {"Name":"Santa Barbara", "Link":"https://www.imn.ac.cr/especial/tablas/stabarbara.html", "Lat":10.0333, "Long":-84.1658, "Description":"Santa Bárbara de Heredia"},
    {"Name":"Santa Cecilia", "Link":"https://www.imn.ac.cr/especial/tablas/stacecilia.html", "Lat":11.0752, "Long":-85.4534, "Description":"Santa Cecilia, La Cruz, Guanacaste"},
    {"Name":"Santa Elena", "Link":"https://www.imn.ac.cr/especial/tablas/staelena.html", "Lat":10.9200, "Long":-85.6107, "Description":"Santa Elena, La Cruz, Guanacaste"},
    {"Name":"Santa Lucia", "Link":"https://www.imn.ac.cr/especial/tablas/stalucia.html", "Lat":10.0230, "Long":-84.11175, "Description":"Santa Lucía, UNA Heredia"},
    {"Name":"Santa Rita", "Link":"https://www.imn.ac.cr/especial/tablas/nandayure.html", "Lat":10.0189, "Long":-85.2619, "Description":"Santa Rita de Nandayure Guanacaste"},
    {"Name":"Santa Rosa", "Link":"https://www.imn.ac.cr/especial/tablas/santarosa.html", "Lat":10.8411, "Long":-85.6194, "Description":"Santa Rosa Guanacaste"},
    #{"Name":"Sartalillo Potrero Cerrado", "Link":"https://www.imn.ac.cr/especial/tablas/potrero.html", "Lat":9.9022, "Long":-83.8858, "Description":"Potrero Cerrado, Sartalillo de Cartago"},
    {"Name":"Sitio La Cruz", "Link":"https://www.imn.ac.cr/especial/tablas/sitiolacruz.html", "Lat":10.4831, "Long":-85.4142, "Description":"Sitio La Cruz, Bagaces, Guanacaste"},
    {"Name":"Sitio Mata", "Link":"https://www.imn.ac.cr/especial/tablas/sitiomata.html", "Lat":9.8792, "Long":83.6258, "Description":"Sitio Matas, Turrialba, Cartago"},
    {"Name":"Sixaola", "Link":"https://www.imn.ac.cr/especial/tablas/sixaola.html", "Lat":9.5275, "Long":-82.6336, "Description":"Sixaola, Talamanca, Limón"},
    {"Name":"Turrialba Centro", "Link":"https://www.imn.ac.cr/especial/tablas/turrialba.html", "Lat":9.9100, "Long":-83.6789, "Description":"Turrialba, Cartago"},
    {"Name":"UCR Santa Cruz", "Link":"https://www.imn.ac.cr/especial/tablas/stacruz.html", "Lat":10.2856, "Long":-85.5917, "Description":"UCR Santa Cruz, Guanacaste"},
    {"Name":"Universidad INVENIO", "Link":"https://www.imn.ac.cr/especial/tablas/invenio.html", "Lat":10.4766, "Long":-85.0598, "Description":"Universidad Invenio, Cañas, Guanacaste"},
    {"Name":"Universidad para la Paz", "Link":"https://www.imn.ac.cr/especial/tablas/upaz.html", "Lat":9.9203, "Long":-84.2728, "Description":"Universidad de la Paz, Mora, San José"},
    {"Name":"UTN, Balsa de Atenas", "Link":"https://www.imn.ac.cr/especial/tablas/utn.html", "Lat":9.9356, "Long":-84.3798, "Description":"UTN, Atenas, Alajuela"},
    {"Name":"Volcan Irazu", "Link":"https://www.imn.ac.cr/especial/tablas/virazu.html", "Lat":9.9800, "Long":-83.8378, "Description":"Volcán Irazú, Alvarado, Cartago"},
    {"Name":"Volcan Turrialba", "Link":"https://www.imn.ac.cr/especial/tablas/vturri.html", "Lat":10.0192, "Long":-83.7564, "Description":"Volcán Turrialba, Turrialba de Cartago"},
    #{"Name":"Zapote", "Link":"https://www.imn.ac.cr/especial/tablas/zapote.html", "Lat":9.9256, "Long":-84.0589, "Description":"Radio Columbia, Zapote, San José"}
    ]

# loc_stations = [ {"Name":"ASADA San Gabriel", "Link":"https://www.imn.ac.cr/especial/tablas/sgabriel.html", "Lat":9.7872, "Long":-84.1053, "Description":"Asada San Gabriel, Desamparados, San José"},]

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
        sql = f"""select [id], [Name], [Desc], [Lat], [Long], [Link]  from IMN_WeatherStations order by id"""
    else:
        sql = f"""select [id], [Name], [Desc], [Lat], [Long], [Link] from IMN_WeatherStations where [Enabled] = 1 order by id"""

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
                if np.isfinite(row[i]):
                    values = (station_name, i, row["Fecha"], row[i])
                    try:
                        cursor.execute(sql, values)
                    except pyodbc.Error as err:
                        print("sql", sql)
                        
                        print("values",values)
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
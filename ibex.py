from bs4 import BeautifulSoup
import requests
import pandas as pd 
import datetime
import os
from pymongo import MongoClient
import json

url = "https://www.infobolsa.es/acciones/ibex35"
mongo_client = MongoClient('localhost', 27017)

res = requests.get(url)
soup = BeautifulSoup(res.text, "lxml")
data = pd.DataFrame()


db = mongo_client['Noticias']  # base de datos
res = db['valores']  # como una sección


for rows in soup.find('table', attrs={'class': 'fullTable'}).find_all('tr')[1:36]:
    tds = [row.text for row in rows.find_all('td')[2:11]]
    add_list_to_df = pd.DataFrame([tds])
    data = data.dropna(axis=1, how='all')
    data = data.reset_index(drop=True)
    data = data.replace(r'\r\n','', regex=True)
    data = data.replace(r'\n','', regex=True)
    data = data.replace(r'\s+', '', regex=True)
    
    data = data.append(add_list_to_df)

df = pd.DataFrame({"Header1":data[0],"Header2":data[1],"Header3":data[2],"Header4":data[3],
                    "Header5":data[4],"Header6":data[5], "Header7":data[6], 
                    "Header8":data[7], "Header9":data[8]})
df.columns = ['Nombre', 'Ultimo', "Dif %", "Maximo", "Minimo", "Rent Div", "Volumen","Efectivo", "Dia"]

date = "ibex_" + datetime.datetime.today().strftime("%d-%m-%Y") +".csv"

outdir = os.path.expanduser("~/cotizaciones")

if not os.path.exists(outdir):
    os.mkdir(outdir)

fullname = os.path.join(outdir, date)
df.to_csv(fullname, index=False)

df.reset_index(inplace=True)

records = json.loads(df.T.to_json()).values()
db.valores.insert_many(records)

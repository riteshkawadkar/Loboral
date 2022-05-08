import requests
import re
import json
from bs4 import BeautifulSoup
from datetime import datetime
from unidecode import unidecode
import sys
import logging


# PROVIDE OFFSET VALUE HERE
offset = 0
# PROVIDE LAST PAGE VALUE HERE
_RANGE = 22150


folio = 0
maindict = {}
MemoryDict = {}
base_url = 'http://187.160.245.251:8088'
next_url = '/acuerdos/Listasaltillo.jsp?offset=' + str(offset)
format = '%Y/%m/%d'


MemoryDict['next_url'] = next_url
MemoryDict['offset'] = offset
MemoryDict['timestamp'] = str(datetime.now())
MemoryDict['count'] = 0

logging.basicConfig(filename='app.log', filemode='w', format='%(name)s - %(levelname)s - %(message)s')

def fetch_data_from_offset(offset):

  try:
    url = 'http://187.160.245.251:8088/acuerdos/Listasaltillo.jsp?offset=' + str(offset)
    
    page = requests.get(url)

    soup = BeautifulSoup(page.text, 'html.parser')

    rows = soup.find('table').find_all('tr') # Grab the first table

    # check if next page button is there
    Siguiente = soup.findAll('a', href=True, text='Siguiente')[0]    
    next_url = Siguiente['href']
    offset = next_url.split('=')[1]
    # print('Got Next URL = ' + next_url)

    for i in range(1, 51):
      folio = rows[i].find_all('td')[0].text.upper()
      expendiente = rows[i].find_all('td')[2].text.upper()
      actor  = rows[i].find_all('td')[3].text.upper()
      demandado = rows[i].find_all('td')[4].text.upper()
      tipo = rows[i].find_all('td')[1].text.upper()
      acuerdos  = rows[i].find_all('td')[7].text.upper()
      fecha = rows[i].find_all('td')[6].text.upper()

      re.sub(' +', ' ', expendiente)
      re.sub(' +', ' ', actor)
      re.sub(' +', ' ', demandado)
      re.sub(' +', ' ', tipo)
      re.sub(' +', ' ', acuerdos)
      re.sub(' +', ' ', fecha)
      if fecha == '':
        fecha = ''
      elif '-' in fecha:
        try:
          fecha = datetime.strptime(fecha, "%d-%m-%Y").strftime("%Y/%m/%d")
        except:
          fecha = datetime.strptime(fecha, "%d-%b-%Y").strftime("%Y/%m/%d")
      else:
        try:
            fecha = datetime.strptime(fecha, "%d/%m/%Y").strftime("%Y/%m/%d")
        except:
            fecha = datetime.strptime(fecha, "%d/%b/%Y").strftime("%Y/%m/%d")

      thisdict = {
        "actor": unidecode(actor),
        "demandado": unidecode(demandado),
        "entidad": " COAHUILA ",
        "expediente": unidecode(expendiente),
        "fecha": fecha,
        "fuero": "COMUN",
        "juzgado": " JUNTA ESPECIAL SALTILLO ",
        "tipo": unidecode(tipo),
        "acuerdos": unidecode(acuerdos),
        "monto": "",
        "fecha_presentacion": "",
        "actos_reclamados": "",
        "actos_reclamados_especificos": "",
        "Naturaleza_procedimiento": "",
        "Prestación_demandada": "",
        "Organo_jurisdiccional_origen": "",
        "expediente_origen": "",
        "materia": "LABORAL",
        "submateria": "",
        "fecha_sentencia": "",
        "sentido_sentencia": "",
        "resoluciones": "",
        "origen": " SECRETARIA DEL TRABAJO DEL ESTADO DE COAHUILA ",
        "fecha_insercion": datetime.now().isoformat(),
        "fecha_tecnica": datetime.now().isoformat().split('T')[0]
      }

      maindict[folio] = thisdict.copy()
      thisdict.clear()      
  except Exception as e:
      print('Scraping URl = ' + url)
      print(str(e) + ' = ' + str(folio))
      logging.error('Scraping URl = ' + url)
      logging.error(str(e) + ' = ' + str(folio))
        
      return {
              "folio": folio,
              "next_url": next_url,
              "offset": offset,
              "timestamp": str(datetime.now()),
              "count": MemoryDict['count']
              }

  return {
            "folio": folio,
            "next_url": next_url,
            "offset": offset,
            "timestamp": str(datetime.now()),
            "count": MemoryDict['count'] + i
            }



    
from concurrent import futures
    
offsets = []
for i in range(_RANGE): 
  offsets.append((i+1)*50) 


try:
  with futures.ThreadPoolExecutor() as executor: # default/optimized number of threads
    list(executor.map(fetch_data_from_offset, offsets))
except:
  # Serializing json 
  print(MemoryDict)
  
  json_object = json.dumps(maindict, indent = 4, ensure_ascii=False)

  with open("laboral_50.json", "w", encoding="utf8") as outfile:
      outfile.write(json_object)   
  
  
  index_object = json.dumps(MemoryDict, indent = 4)
  # Writing to sample.json
  with open('index.json', "w") as outfile:
      outfile.write(index_object)
  

  sys.exit(0)
    
    
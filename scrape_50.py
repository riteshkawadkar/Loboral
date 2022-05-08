import time
import requests
import re
import json
from bs4 import BeautifulSoup
from datetime import datetime
from unidecode import unidecode
import sys
import logging


def transformChars(uni_string):
  uni_chars={
            'á':'a',
             'é':'e',
             'í':'i',
             'ó':'o',
             'ö':'o',
             'ú':'u',
             'ü':'u',
             'Á':'A',
             'É':'E',
             'Í':'I',
             'Ó':'O',
             'Ö':'O',
             'Ú':'U',
             'Ü':'U'
             }

  # Iterate over index
  
  # for element in range(0, len(uni_string)):
  #   if element in uni_chars:
  #     uni_string = uni_string.replace(element, uni_chars[element])
  
  for element in uni_chars:
    if element in uni_string:
      uni_string = uni_string.replace(element, uni_chars[element])
         
  return uni_string
    
start_time = datetime.now()

# PROVIDE OFFSET VALUE HERE
offset = 0

scrape_key = 'Listamonclova'
# 'Listasaltillo',
# 'Listasaltilloe',
# 'Listatorreon', # later
# 'Listamonclova', # later
# 'Listapiedras',
# 'Listasabinas',
# 'Listaacuna'


dic_juzgado = {
  'Listasaltillo':'JUNTA LOCAL DE CONCILIACION Y ARBITRAJE DE SALTILLO',
  'Listasaltilloe':'JUNTA ESPECIAL DE CONCILIACION Y ARBITRAJE DE SALTILLO',
  'Listatorreon': 'JUNTA LOCAL DE DE CONCILIACION Y ARBITRAJE DE CONCILIACION Y ARBITRAJE DE TORREON',
  'Listamonclova': 'JUNTA LOCAL DE CONCILIACION Y ARBITRAJE DE MONCLOVA',
  'Listapiedras': 'JUNTA LOCAL DE CONCILIACION Y ARBITRAJE DE PIEDRAS NEGRAS',
  'Listasabinas': 'JUNTA LOCAL DE CONCILIACION Y ARBITRAJE DE SABINAS',
  'Listaacuna': 'JUNTA LOCAL DE CONCILIACION Y ARBITRAJE DE ACUÑA'
}

folio = 0
maindict = {}
MemoryDict = {}
base_url = 'http://187.160.245.251:8088'
next_url = '/acuerdos/' + scrape_key + '.jsp?offset=' + str(offset)
format = '%Y/%m/%d'


MemoryDict['next_url'] = next_url
MemoryDict['offset'] = offset
MemoryDict['timestamp'] = str(datetime.now())
MemoryDict['count'] = 0

logging.basicConfig(filename=scrape_key+'_app.log', filemode='w', format='%(name)s - %(levelname)s - %(message)s')

def fetch_data_from_offset(next_url, offset):

  try:
    url = base_url + next_url
    print('Scraping URl = ' + url)
    if 'Listatorreon' in url or 'Listamonclova' in url:
      page = requests.get(url, timeout=(50, 50))
    else:
      page = requests.get(url)

    soup = BeautifulSoup(page.text, 'html.parser')

    rows = soup.find('table').find_all('tr') # Grab the first table

    if 'Listatorreon' in url or 'Listamonclova' in url:
      next_url = None
      offset = None
    else:
      # check if next page button is there
      try:
        Siguiente = soup.findAll('a', href=True, text='Siguiente')[0]    
        next_url = Siguiente['href']
        offset = next_url.split('=')[1]
        # print('Got Next URL = ' + next_url)
      except:
        next_url = None
        offset = None
    
        
    counter = 0    
    if len(rows)-1 == 1:
      folio = rows[1].find_all('td')[0].text.upper()
      expendiente = rows[1].find_all('td')[2].text.upper()
      actor  = rows[1].find_all('td')[3].text.upper()
      demandado = rows[1].find_all('td')[4].text.upper()
      tipo = rows[1].find_all('td')[1].text.upper()
      acuerdos  = rows[1].find_all('td')[7].text.upper()
      fecha = rows[1].find_all('td')[6].text.upper()
      
      re.sub(' +', ' ', expendiente)
      re.sub(' +', ' ', actor)
      re.sub(' +', ' ', demandado)
      re.sub(' +', ' ', tipo)
      re.sub(' +', ' ', acuerdos)
      acuerdos = ' '.join(acuerdos.split())
      re.sub(' +', ' ', fecha)

      if fecha == '':
            logging.error('Folio has blank fecha = ' + str(folio))
            logging.error('fecha = ' + str(fecha))
            fecha = 'ERROR'
      elif '-' in fecha:
        try:
          fecha = datetime.strptime(fecha, "%d-%m-%Y").strftime("%Y/%m/%d")
        except:
          try:
            fecha = datetime.strptime(fecha, "%d-%b-%Y").strftime("%Y/%m/%d")
          except:
            try:
              fecha = datetime.strptime(fecha, "%d-%m-%y").strftime("%Y/%m/%d")
            except:
              try:
                fecha = datetime.strptime(fecha, "%d-%b-%y").strftime("%Y/%m/%d")
              except Exception as p:
                logging.error(str(p) + ' = ' + str(folio))
                logging.error('fecha = ' + str(fecha))
                fecha = 'ERROR'
      else:
        try:
            fecha = datetime.strptime(fecha, "%d/%m/%Y").strftime("%Y/%m/%d")
        except:
          try:
            fecha = datetime.strptime(fecha, "%d/%b/%Y").strftime("%Y/%m/%d")
          except:
            try:
              fecha = datetime.strptime(fecha, "%d/%m/%y").strftime("%Y/%m/%d")
            except:
              try:
                fecha = datetime.strptime(fecha, "%d/%b/%y").strftime("%Y/%m/%d")
              except Exception as p:
                logging.error(str(p) + ' = ' + str(folio))
                logging.error('fecha = ' + str(fecha))
                fecha = 'ERROR'
      

      if scrape_key in dic_juzgado:
        juzgado = dic_juzgado.get(scrape_key)
        
      isodate = "ISODate(" + datetime.now().isoformat()[:23] + "Z" + ")"
        
      thisdict = {
        "actor": transformChars(actor),
        "demandado": transformChars(demandado),
        "entidad": " COAHUILA ",
        "expediente": transformChars(expendiente),
        "fecha": fecha,
        "fuero": "COMUN",
        "juzgado": juzgado,
        "tipo": transformChars(tipo),
        "acuerdos": transformChars(acuerdos),
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
        "fecha_insercion": isodate,
        "fecha_tecnica": isodate
      }

      if 'ERROR' == fecha:
        thisdict.clear()  
      else:
        counter+=1
        maindict[folio] = thisdict.copy()
        thisdict.clear()  
        
      return {
            "folio": folio,
            "next_url": next_url,
            "offset": offset,
            "timestamp": str(datetime.now()),
            "count": MemoryDict['count'] + counter
            }
      
    for i in range(1, len(rows)):
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
      acuerdos = ' '.join(acuerdos.split())
      re.sub(' +', ' ', fecha)

      if fecha == '':
            logging.error('Folio has blank fecha = ' + str(folio))
            logging.error('fecha = ' + str(fecha))
            fecha = 'ERROR'
      elif '-' in fecha:
        try:
          fecha = datetime.strptime(fecha, "%d-%m-%Y").strftime("%Y/%m/%d")
        except:
          try:
            fecha = datetime.strptime(fecha, "%d-%b-%Y").strftime("%Y/%m/%d")
          except:
            try:
              fecha = datetime.strptime(fecha, "%d-%m-%y").strftime("%Y/%m/%d")
            except:
              try:
                fecha = datetime.strptime(fecha, "%d-%b-%y").strftime("%Y/%m/%d")
              except Exception as p:
                logging.error(str(p) + ' = ' + str(folio))
                logging.error('fecha = ' + str(fecha))
                fecha = 'ERROR'
      else:
        try:
            fecha = datetime.strptime(fecha, "%d/%m/%Y").strftime("%Y/%m/%d")
        except:
          try:
            fecha = datetime.strptime(fecha, "%d/%b/%Y").strftime("%Y/%m/%d")
          except:
            try:
              fecha = datetime.strptime(fecha, "%d/%m/%y").strftime("%Y/%m/%d")
            except:
              try:
                fecha = datetime.strptime(fecha, "%d/%b/%y").strftime("%Y/%m/%d")
              except Exception as p:
                logging.error(str(p) + ' = ' + str(folio))
                logging.error('fecha = ' + str(fecha))
                fecha = 'ERROR'
      
      if scrape_key in dic_juzgado:
            juzgado = dic_juzgado.get(scrape_key)
      
      isodate = "ISODate(" + datetime.now().isoformat()[:23] + "Z" + ")"
                
      thisdict = {
        "actor": transformChars(actor),
        "demandado": transformChars(demandado),
        "entidad": " COAHUILA ",
        "expediente": transformChars(expendiente),
        "fecha": fecha,
        "fuero": "COMUN",
        "juzgado": juzgado,
        "tipo": transformChars(tipo),
        "acuerdos": transformChars(acuerdos),
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
        "fecha_insercion": isodate,
        "fecha_tecnica": isodate
      }

      if 'ERROR' == fecha:
        thisdict.clear()  
      else:
        counter+=1
        maindict[folio] = thisdict.copy()
        thisdict.clear()  
      
      
    
  
  except Exception as e:
      print(str(e) + ' = ' + str(folio))
      logging.error('Scraping URl = ' + url)
      logging.error(str(e) + ' = ' + str(folio))
      return {
              "folio": folio,
              "next_url": next_url,
              "offset": offset,
              "timestamp": str(datetime.now()),
              "count": MemoryDict['count'] + counter
              }

  return {
            "folio": folio,
            "next_url": next_url,
            "offset": offset,
            "timestamp": str(datetime.now()),
            "count": MemoryDict['count'] + counter
            }



try:
  while MemoryDict['next_url']:  
    MemoryDict = fetch_data_from_offset(MemoryDict['next_url'], MemoryDict['offset'])
    # if MemoryDict['offset']=='100':
    #   break
    # print(MemoryDict)
except Exception as ee:  
  # Serializing json 
  print(MemoryDict)
  logging.error(MemoryDict)

  
  json_object = json.dumps(maindict, indent = 4, ensure_ascii=False)

  with open(scrape_key + ".json", "w", encoding="utf8") as outfile:
      outfile.write(json_object)   
  
  
  index_object = json.dumps(MemoryDict, indent = 4)
  # Writing to sample.json
  with open(scrape_key + '_index.json', "w") as outfile:
      outfile.write(index_object)
  
  end_time = datetime.now()
  print('Duration: {}'.format(end_time - start_time))
  sys.exit(0)


# Serializing json 
index_object = json.dumps(MemoryDict, indent = 4)
json_object = json.dumps(maindict, indent = 4, ensure_ascii=False)

# Writing to sample.json
with open(scrape_key + '_index.json', "w") as outfile:
    outfile.write(index_object)

with open(scrape_key + ".json", "w", encoding="utf8") as outfile:
    outfile.write(json_object)   


end_time = datetime.now()
print('Duration: {}'.format(end_time - start_time))
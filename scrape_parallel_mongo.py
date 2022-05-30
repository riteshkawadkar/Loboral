# coding=utf-8

import time
import requests
import re
import json
from bs4 import BeautifulSoup
from datetime import datetime
from unidecode import unidecode
import sys
import logging
from pymongo import MongoClient



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

  
  for element in uni_chars:
    if element in uni_string:
      uni_string = uni_string.replace(element, uni_chars[element])
         
  return uni_string
    


def fetch_data_from_offset(next_url, offset, scrape_key ):

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
      # offset = None
    else:
      # check if next page button is there
      try:
        Siguiente = soup.findAll('a', href=True, text='Siguiente')[0]    
        next_url = Siguiente['href']
        offset = next_url.split('=')[1]
        # print('Got Next URL = ' + next_url)
      except:
        next_url = None
        # offset = None
    
        
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
      
      duplicate_check = {
        "actor": transformChars(actor),
        "demandado": transformChars(demandado),
        "expediente": transformChars(expendiente),
        "fecha": fecha,
        "juzgado": juzgado,
      }

      if 'ERROR' == fecha:
        thisdict.clear()  
      else:
        
        maindict[folio] = thisdict.copy()
        result = records.update_one(duplicate_check, {'$set': thisdict.copy()}, upsert=True)
        if result.matched_count > 0:
          # its a duplicate, no need to add counter
          pass
        else:
          counter+=1
        thisdict.clear()  
        
      return {
            "folio": folio,
            "next_url": next_url,
            "offset": offset,
            "timestamp": str(datetime.now()),
            "collection": scrape_key,
            "count": MemoryDict['count'] + counter,
            "total_count": MemoryDict['total_count'] + MemoryDict['count'] + counter
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
      
      duplicate_check = {
        "actor": transformChars(actor),
        "demandado": transformChars(demandado),
        "expediente": transformChars(expendiente),
        "fecha": fecha,
        "juzgado": juzgado,
      }

      if 'ERROR' == fecha:
        thisdict.clear()  
      else:
        # counter+=1
        maindict[folio] = thisdict.copy()
        # records.update_one(thisdict.copy(), upsert=True)
        result = records.update_one(duplicate_check, {'$set': thisdict.copy()}, upsert=True)
        if result.matched_count > 0:
              # its a duplicate, no need to add counter
          pass
        else:
          counter+=1
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
              "collection": scrape_key,
              "count": MemoryDict['count'] + counter,
              "total_count": MemoryDict['total_count'] + MemoryDict['count'] + counter
              }

  return {
            "folio": folio,
            "next_url": next_url,
            "offset": offset,
            "timestamp": str(datetime.now()),
            "collection": scrape_key,
            "count": MemoryDict['count'] + counter,
            "total_count": MemoryDict['total_count'] + MemoryDict['count'] + counter
            }















start_time = datetime.now()


scrape_keys = ['Listamonclova', 'Listasaltillo','Listasaltilloe', 'Listatorreon', 'Listamonclova', 'Listapiedras','Listasabinas', 'Listaacuna']
# scrape_keys = ['Listamonclova']


client = MongoClient("mongodb+srv://mongoadmin:mongoadmin@cluster0.keqe2go.mongodb.net/?retryWrites=true&w=majority")

db = client.get_database("Crudo")

records = db.Laboral_Coahuila
index = db.index




dic_juzgado = {
  'Listasaltillo':'JUNTA LOCAL DE CONCILIACION Y ARBITRAJE DE SALTILLO',
  'Listasaltilloe':'JUNTA ESPECIAL DE CONCILIACION Y ARBITRAJE DE SALTILLO',
  'Listatorreon': 'JUNTA LOCAL DE DE CONCILIACION Y ARBITRAJE DE CONCILIACION Y ARBITRAJE DE TORREON',
  'Listamonclova': 'JUNTA LOCAL DE CONCILIACION Y ARBITRAJE DE MONCLOVA',
  'Listapiedras': 'JUNTA LOCAL DE CONCILIACION Y ARBITRAJE DE PIEDRAS NEGRAS',
  'Listasabinas': 'JUNTA LOCAL DE CONCILIACION Y ARBITRAJE DE SABINAS',
  'Listaacuna': 'JUNTA LOCAL DE CONCILIACION Y ARBITRAJE DE ACUÑA'
}

logging.basicConfig(filename='app.log', filemode='w', format='%(name)s - %(levelname)s - %(message)s')

for scrape_key in scrape_keys:
      
  # PROVIDE OFFSET VALUE HERE
  #Update OFFSET
  try:
    offsetno = index.find_one({'collection':scrape_key})
    offset = offsetno['offset']
  except:
    offset = 0
    
  try:
    total_count = offsetno['total_count']
  except:
    total_count = 0

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
  MemoryDict['total_count'] = total_count
  MemoryDict['collection'] = ''


  
  try:
    
    while MemoryDict['next_url']:  
      MemoryDict = fetch_data_from_offset(MemoryDict['next_url'], MemoryDict['offset'], scrape_key)
      # print(scrape_key)
      index.update_one({'collection':scrape_key}, {'$set': MemoryDict}, upsert=True)
  except Exception as ee:  
    # Serializing json 
    print(MemoryDict)
    logging.error(MemoryDict)

    end_time = datetime.now()
    print('Duration: {}'.format(end_time - start_time))
    sys.exit(0)

  # index.update_one({'collection':scrape_key}, {'$set': MemoryDict})
  # records.insert_one(maindict)

  end_time = datetime.now()
  print('Duration: {}'.format(end_time - start_time))
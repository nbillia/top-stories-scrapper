from selenium import webdriver
import chromedriver_autoinstaller
import sys
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.keys import Keys
import pandas as pd
import re
from lxml import etree
from lxml import html
import time
import datetime
import requests
from unidecode import unidecode
import json
import warnings
warnings.filterwarnings('ignore')
import matplotlib.pyplot as plt
import pytz
from datetime import datetime
import sqlalchemy
import mysql.connector

options = webdriver.ChromeOptions()
options.add_argument('--headless')
options.add_argument('--no-sandbox')
options.add_argument('--disable-dev-shm-usage')
user_agent = 'Mozilla/5.0 (iPhone; CPU iPhone OS 14_0 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) CriOS/85.0.4183.109 Mobile/15E148 Safari/604.1'
options.add_argument(f'user-agent={user_agent}')
options.add_argument('--disable-blink-features=AutomationControlled')
driver = webdriver.Chrome(options=options)

def timestamp_fecha(country_code):
  timestamp = pytz.country_timezones.get(country_code)[0]
  time_in_country = pytz.timezone(timestamp)
  current_time = datetime.now(time_in_country)
  current_time = current_time.strftime('%Y-%m-%d')

  return current_time

def timestamp_hora(country_code):
  timestamp = pytz.country_timezones.get(country_code)[0]
  time_in_country = pytz.timezone(timestamp)
  current_time = datetime.now(time_in_country)
  current_time = current_time.strftime('%H')

  return current_time

def organic_results(query,country_code):
  url_google = 'https://www.google.com/search?q='
  url = f'{url_google}{query}&gl={country_code}'
  driver.get(url)
  #organic_container = WebDriverWait(driver,10).until(
         #EC.presence_of_element_located((By.XPATH, "//div[@class='center_col s6JM6d']"))
      #)
  organic = driver.find_elements(By.XPATH,"//div[@class='P8ujBc v5yQqb jqWpsc']/a")
  twitter_results = driver.find_elements(By.XPATH,"//g-inner-card//a")
  enlaces_organic = [blue_link.get_attribute('href') for blue_link in organic]
  enlaces_twitter = [twitter_link.get_attribute('href') for twitter_link in twitter_results]
  enlaces_organic.extend(enlaces_twitter)
  for idx, blue_link in enumerate(enlaces_organic, 1):
      print(idx,blue_link)
      if idx == 30:
        break

def get_title_and_headline(url):
  
  from unicodedata import normalize
  try:
    response = requests.get(url)
    tree = html.fromstring(response.content)
    title = tree.xpath('//title')[0].text
    
    title = re.sub(r'[^A-Za-záéíóúÁÉÍÓÚüÜÑñ ]+', '', title)
    title = title.replace('ñ', '\001');
    title = normalize('NFKD', title).encode('ASCII', 'ignore').decode().replace('\001', 'ñ')
    

      
    return title

  except Exception as e:

            return None

def update_table(table_name, db_name,df):
  engine = sqlalchemy.create_engine(f'mysql+pymysql://root:@localhost/{db_name}')

  df['fecha'] = pd.to_datetime(df['fecha'])
  df['fecha'] = df['fecha'].apply(lambda x: x.strftime('%Y-%m-%d %H:%M:%S'))

  df.to_sql(
      name = table_name,
      con = engine,
      index = False,
      if_exists = 'append'
  )





def serp_analysis(query,country_code):
  serp = []
  url_google = 'https://www.google.com/search?q='
  url = f'{url_google}{query}&gl={country_code}'
  driver.get(url)
  # Suponiendo que tienes un elemento que contiene las top stories
  top_stories_container = WebDriverWait(driver, 10).until(
        EC.presence_of_element_located((By.XPATH, "//g-scrolling-carousel"))
    )

  # Luego, puedes obtener los elementos <a> dentro de ese contenedor
  top_stories_links = top_stories_container.find_elements(By.XPATH,"//a[@class='WlydOe']")

  # Ahora, puedes obtener los enlaces href de cada elemento <a> y almacenarlos en una lista
  enlaces_top_stories = [enlace.get_attribute('href') for enlace in top_stories_links]

  # Filtra la lista para eliminar elementos que sean None y contengan "google.com"
  enlaces_filtrados = [enlace for enlace in enlaces_top_stories if enlace is not None and "google.com" not in enlace]

  organic = driver.find_elements(By.XPATH,"//div[@class='P8ujBc v5yQqb jqWpsc']/a")
  twitter_results = driver.find_elements(By.XPATH,"//g-inner-card//a")
  enlaces_organic = [blue_link.get_attribute('href') for blue_link in organic]
  enlaces_twitter = [twitter_link.get_attribute('href') for twitter_link in twitter_results]
  enlaces_organic.extend(enlaces_twitter)
  # Agrega las tuplas a la lista serp con índices y categorías
  serp.extend([(idx, enlace, 'Top Stories') for idx, enlace in enumerate(enlaces_filtrados, 1)])
  serp.extend([(idx, enlace, 'Organic Results') for idx, enlace in enumerate(enlaces_organic, 1)])



  # Convierte la lista serp en un DataFrame
  df_serp = pd.DataFrame(serp, columns=['Índice', 'Enlace', 'Categoría'])
  df_serp['sitio'] = df_serp['Enlace'].apply(lambda x: x.split('/')[2])
  df_serp['title'] = df_serp['Enlace'].apply(get_title_and_headline)
  df_serp['date'] = timestamp_fecha(country_code)
  df_serp['hour'] = timestamp_hora(country_code)
  df_serp['query'] = query
  df_serp['country_code'] = country_code



  # Renombrar columnas y reordenar
  df_serp.columns = ['position', 'url', 'search_type', 'domain', 'title', 'date', 'hour','query','country']
  # Agregar la función para extraer el tipo de contenido



  return df_serp


def top_queries_trends(country,language):

  sys.path.insert(0,'/Users/nicolasbillia/Documents/chromedriver')
  chrome_options = webdriver.ChromeOptions()
  chrome_options.add_argument('--headless') # this is must
  chrome_options.add_argument('--no-sandbox')
  chrome_options.add_argument('--disable-dev-shm-usage')
  # Definir el agente de usuario de un dispositivo móvil (por ejemplo, un iPhone)
  user_agent = 'Mozilla/5.0 (iPhone; CPU iPhone OS 14_0 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) CriOS/85.0.4183.109 Mobile/15E148 Safari/604.1'
  chrome_options.add_argument(f'user-agent={user_agent}')
  chromedriver_autoinstaller.install()

  driver = webdriver.Chrome(options=chrome_options)
  country = country.upper()
  #driver = webdriver.Chrome()
  driver.get(f'https://trends.google.com/trends/trendingsearches/daily?geo={country}&hl={language}-419')
  queries = [query.text.lower() for query in driver.find_elements(By.XPATH,"//div[@class='details']/div[@class='details-top']/div[@class='title']/span/a")]

  df_trends = pd.DataFrame()

  for keyword in queries[:1]:
    try:
      print(f'Analizando {keyword}')
      df = serp_analysis(keyword,country)
      df_trends = pd.concat([df_trends,df])
    except Exception as e:
      print(f'Error al analizar {keyword}')


  return df_trends

df_trends = top_queries_trends('es','es') 

def update_table(table_name, db_name, df):
    engine = sqlalchemy.create_engine(f'mysql+pymysql://root:root@localhost/{db_name}')

    try:
        df.to_sql(
            name=table_name,
            con=engine,
            index=False,
            if_exists='append'
            
        )
        
        print("Datos insertados correctamente en la tabla.")
    except Exception as e:
        print("Error al insertar datos en la tabla:", e)

update_table('trends_top_stories','scrapper_top_stories',df_trends)
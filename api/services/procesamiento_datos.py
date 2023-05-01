import pandas as pd
from sqlalchemy import create_engine;
from django.http import HttpResponse;
from api.models.migracion_colombia import MigracionColombia
from api.controllers.migracion_serializer import MigracionSerializer

class ProcesamientoDatos():
  df=0
    #Cargar archivo csv ------------------------------------------------
  def cargar_csv(self,ruta):
      #IMPORTACION DEL DATASET-------------------------------------------------
      global df
      ds = pd.read_csv(ruta)
      print(ds)
      df = ds.copy()
      #CASTEO DE TIPOS EN EL DATASET-------------------------------------------
      df['Mes'] = df['Mes'].astype('string')
      df['Nacionalidad'] = df['Nacionalidad'].astype('string')
      df['Latitud - Longitud'] = df['Latitud - Longitud'].astype('string')
      df.info()
      #RENOMBRAR COLUMNAS------------------------------------------------------
      df.rename(columns={'Año':'ANIO'},inplace=True)
      df.rename(columns={'Mes':'MES'},inplace=True)
      df.rename(columns={'Nacionalidad':'NACIONALIDAD'},inplace=True)
      df.rename(columns={'Codigo Iso 3166':'ISO_3166'},inplace=True)
      df.rename(columns={'Femenino':'FEMENINO'},inplace=True)
      df.rename(columns={'Masculino':'MASCULINO'},inplace=True)
      df.rename(columns={'Indefinido':'INDEFINIDO'},inplace=True)
      df.rename(columns={'Total':'TOTAL'},inplace=True)
      df.rename(columns={'Latitud - Longitud':'LATITUD_LONGITUD'},inplace=True)
      df.reset_index(inplace=True)
      df = df.applymap(lambda x: x.upper() if isinstance(x, str) else x)
      df.info()
      #INSERCION EN BASE DE DATOS DE MYSQL-------------------------------------
      engine = create_engine("mysql+pymysql://{user}:{pw}@localhost/{db}"
                             .format(user="root",
                                     pw="admin",
                                     db="especializacion"))
      df.to_sql(con=engine, name='MIGRACION_COLOMBIA', if_exists='replace')
      return;
  #obtener datos filtrados por años--------------------------------------------------
  def datos_anio(self,anio,cantidad,nacionalidad,mes):
      lista_df = []
      d1=df.copy()
      if(len(cantidad)==0):
         cantidad='1'
      if(len(anio)!=0):
        d1 = df.query('ANIO=={}'.format(anio))
      if(len(nacionalidad)!=0):
        d1 = d1.query('NACIONALIDAD=="{}"'.format(nacionalidad))
      if(len(mes)!=0):
        d1 = d1.query('MES=="{}"'.format(mes))
      d2 = d1.head(int(cantidad))
      for indice, fila in d2.iterrows():
          lista_df.append(ProcesamientoDatos.df2model(fila))
      serializer = MigracionSerializer(lista_df, many = True)
      return serializer

  #convertir de df a model-------------------
  def df2model(registro):
      migracion = MigracionColombia()
      migracion.ANIO = registro['ANIO']
      migracion.MES = registro['MES']
      migracion.NACIONALIDAD = registro['NACIONALIDAD']
      migracion.ISO_3166 = registro['ISO_3166']
      migracion.FEMENINO = registro['FEMENINO']
      migracion.MASCULINO = registro['MASCULINO']
      migracion.INDEFINIDO = registro['INDEFINIDO']
      migracion.TOTAL = registro['TOTAL']
      migracion.LATITUD_LONGITUD = registro['LATITUD_LONGITUD']
      migracion.index = registro['index']
      return migracion
import pandas as pd
from sqlalchemy import create_engine;

class ProcesamientoDatos():
    #Cargar archivo csv ------------------------------------------------
  def cargar_csv(self,ruta):
      #IMPORTACION DEL DATASET-------------------------------------------------
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
      df.info()
      #INSERCION EN BASE DE DATOS DE MYSQL-------------------------------------
      engine = create_engine("mysql+pymysql://{user}:{pw}@localhost/{db}"
                             .format(user="root",
                                     pw="admin",
                                     db="especializacion"))
      df.to_sql(con=engine, name='MIGRACION_COLOMBIA', if_exists='replace')
      return;
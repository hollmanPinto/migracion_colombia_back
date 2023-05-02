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
  #obtener cantidad de personas por anio------------
  def personasXanio(self,rangoFechas):
    anios = str(rangoFechas)
    rangos = anios.split('-',2)
    primerAnio = int(rangos[0])
    segundoAnio = int(rangos[1])
    dfFiltrado = df.loc[df['ANIO'].between(primerAnio, segundoAnio)]
    df_grouped = dfFiltrado.groupby('ANIO')['TOTAL'].sum()
    dicc = df_grouped.to_dict()
    return dicc
  #proporcion de hombres y mujeres por anios------------
  def proporcionHM(self,rangoAnios):
    anios = str(rangoAnios)
    rangos = anios.split('-',2)
    primerAnio = int(rangos[0])
    segundoAnio = int(rangos[1])
    dfFiltrado = df.loc[df['ANIO'].between(primerAnio, segundoAnio)]
    df_grouped = dfFiltrado.groupby('ANIO')['FEMENINO'].sum().reset_index()
    df_grouped_masc = dfFiltrado.groupby('ANIO')['MASCULINO'].sum().reset_index()
    df_concatenado = pd.concat([df_grouped, df_grouped_masc], axis=1)
    suma_hombres = df_concatenado['MASCULINO'].sum()
    suma_mujeres = df_concatenado['FEMENINO'].sum()
    dicc = {'Hombres':int(suma_hombres),'Mujeres':int(suma_mujeres)}
    return dicc
  #migrantes por trimestres-------------------------------
  def migrantesXtrimestres(self,rangoAnios):
    anios = str(rangoAnios)
    rangos = anios.split('-',2)
    primerAnio = int(rangos[0])
    segundoAnio = int(rangos[1])
    dfFiltrado = df.loc[df['ANIO'].between(primerAnio, segundoAnio)]
    df1Tri = dfFiltrado[(dfFiltrado['MES'] == 'ENERO') | (dfFiltrado['MES'] == 'FEBRERO') | (dfFiltrado['MES'] == 'MARZO')]
    df2Tri = dfFiltrado[(dfFiltrado['MES'] == 'ABRIL') | (dfFiltrado['MES'] == 'MAYO') | (dfFiltrado['MES'] == 'JUNIO')]
    df3Tri = dfFiltrado[(dfFiltrado['MES'] == 'JULIO') | (dfFiltrado['MES'] == 'AGOSTO') | (dfFiltrado['MES'] == 'SEPTIEMBRE')]
    df4Tri = dfFiltrado[(dfFiltrado['MES'] == 'OCTUBRE') | (dfFiltrado['MES'] == 'NOVIEMBRE') | (dfFiltrado['MES'] == 'DICIEMBRE')]
    d1Sum = df1Tri['TOTAL'].sum()
    d2Sum = df2Tri['TOTAL'].sum()
    d3Sum = df3Tri['TOTAL'].sum()
    d4Sum = df4Tri['TOTAL'].sum()
    dicc = {'trim1':int(d1Sum),'trim2':int(d2Sum),'trim3':int(d3Sum),'trim4':int(d4Sum)}
    return dicc;
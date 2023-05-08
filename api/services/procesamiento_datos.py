import pandas as pd
from sqlalchemy import create_engine;
from django.http import HttpResponse;
from api.models.migracion_colombia import MigracionColombia
from api.controllers.migracion_serializer import MigracionSerializer
from unidecode import unidecode

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
    #ELIMINAR ACENTOS------------------------------------------------------
    df['NACIONALIDAD'] = df['NACIONALIDAD'].apply(lambda x: unidecode(x))
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
#GRAFICOS DE BARRAS-----------------------------------------------------------------
  def topPaises(self):
    dfGroupPaises = df.groupby('NACIONALIDAD')['TOTAL'].sum().reset_index()
    dSort = dfGroupPaises.sort_values(by=['TOTAL'], ascending=False)
    dSort = dSort.head(20)
    json_string = dSort.to_json(orient='records')
    return json_string
#GRAFICOS DE FUNCION------------------------------------------------------------------
  def cantidadMesesAnios(self):
    listaObjects=[]
    #2012----------------------------------------------------
    df2012 = df.loc[df['ANIO']==2012]
    df_grouped_2012 = df2012.groupby('MES')['TOTAL'].sum().reset_index()
    dicc2012 = dict(zip(df_grouped_2012['MES'], df_grouped_2012['TOTAL']))  # Convert to dictionary with month as key
    dicc2012['anio'] = 2012
    meses = ['ENERO', 'FEBRERO', 'MARZO', 'ABRIL', 'MAYO', 'JUNIO', 'JULIO', 'AGOSTO', 'SEPTIEMBRE', 'OCTUBRE', 'NOVIEMBRE', 'DICIEMBRE']
    nuevo_obj_2012 = {}
    for mes in meses:
      nuevo_obj_2012[mes] = dicc2012[mes]
    nuevo_obj_2012['anio'] = dicc2012['anio']
    nuevo_obj_2012['media'] = round(float(df_grouped_2012['TOTAL'].mean()),2)
    nuevo_obj_2012['moda'] = float(df_grouped_2012['TOTAL'].max())

    keys = list(dicc2012.keys())
    indice = df_grouped_2012['TOTAL'].idxmax()
    clave = keys[indice]
    nuevo_obj_2012['modaMes'] = clave
    junio = float(dicc2012['JUNIO'])
    julio = float(dicc2012['JULIO'])
    mediana = (junio+julio)/2
    nuevo_obj_2012['mediana'] = round(mediana,2)
    nuevo_obj_2012['desviacion'] = round(df_grouped_2012['TOTAL'].std(),2)
    listaObjects.append(nuevo_obj_2012)
    #2013------------------------------------------------------------
    df2013 = df.loc[df['ANIO']==2013]
    df_grouped_2013 = df2013.groupby('MES')['TOTAL'].sum().reset_index()
    dicc2013 = dict(zip(df_grouped_2013['MES'], df_grouped_2013['TOTAL']))  # Convert to dictionary with month as key
    dicc2013['anio'] = 2013
    meses = ['ENERO', 'FEBRERO', 'MARZO', 'ABRIL', 'MAYO', 'JUNIO', 'JULIO', 'AGOSTO', 'SEPTIEMBRE', 'OCTUBRE', 'NOVIEMBRE', 'DICIEMBRE']
    nuevo_obj_2013 = {}
    for mes in meses:
      nuevo_obj_2013[mes] = dicc2013[mes]
    nuevo_obj_2013['anio'] = dicc2013['anio']
    nuevo_obj_2013['media'] = round(float(df_grouped_2013['TOTAL'].mean()),2)
    nuevo_obj_2013['moda'] = float(df_grouped_2013['TOTAL'].max())

    keys = list(dicc2013.keys())
    indice = df_grouped_2013['TOTAL'].idxmax()
    clave = keys[indice]
    nuevo_obj_2013['modaMes'] = clave
    junio = float(dicc2013['JUNIO'])
    julio = float(dicc2013['JULIO'])
    mediana = (junio+julio)/2
    nuevo_obj_2013['mediana'] = mediana
    nuevo_obj_2013['desviacion'] = round(df_grouped_2013['TOTAL'].std(),2)
    listaObjects.append(nuevo_obj_2013)
    #2014------------------------------------------------------------
    df2014 = df.loc[df['ANIO']==2014]
    df_grouped_2014 = df2014.groupby('MES')['TOTAL'].sum().reset_index()
    dicc2014 = dict(zip(df_grouped_2014['MES'], df_grouped_2014['TOTAL']))  # Convert to dictionary with month as key
    dicc2014['anio'] = 2014
    meses = ['ENERO', 'FEBRERO', 'MARZO', 'ABRIL', 'MAYO', 'JUNIO', 'JULIO', 'AGOSTO', 'SEPTIEMBRE', 'OCTUBRE', 'NOVIEMBRE', 'DICIEMBRE']
    nuevo_obj_2014 = {}
    for mes in meses:
      nuevo_obj_2014[mes] = dicc2014[mes]
    nuevo_obj_2014['anio'] = dicc2014['anio']
    nuevo_obj_2014['media'] = round(float(df_grouped_2014['TOTAL'].mean()),2)
    nuevo_obj_2014['moda'] = float(df_grouped_2014['TOTAL'].max())

    keys = list(dicc2014.keys())
    indice = df_grouped_2014['TOTAL'].idxmax()
    clave = keys[indice]
    nuevo_obj_2014['modaMes'] = clave
    junio = float(dicc2014['JUNIO'])
    julio = float(dicc2014['JULIO'])
    mediana = (junio+julio)/2
    nuevo_obj_2014['mediana'] = mediana
    nuevo_obj_2014['desviacion'] = round(df_grouped_2014['TOTAL'].std(),2)
    listaObjects.append(nuevo_obj_2014)
    #2015------------------------------------------------------------
    df2015 = df.loc[df['ANIO']==2015]
    df_grouped_2015 = df2015.groupby('MES')['TOTAL'].sum().reset_index()
    dicc2015 = dict(zip(df_grouped_2015['MES'], df_grouped_2015['TOTAL']))  # Convert to dictionary with month as key
    dicc2015['anio'] = 2015
    meses = ['ENERO', 'FEBRERO', 'MARZO', 'ABRIL', 'MAYO', 'JUNIO', 'JULIO', 'AGOSTO', 'SEPTIEMBRE', 'OCTUBRE', 'NOVIEMBRE', 'DICIEMBRE']
    nuevo_obj_2015 = {}
    for mes in meses:
      nuevo_obj_2015[mes] = dicc2015[mes]
    nuevo_obj_2015['anio'] = dicc2015['anio']
    nuevo_obj_2015['media'] = round(float(df_grouped_2015['TOTAL'].mean()),2)
    nuevo_obj_2015['moda'] = float(df_grouped_2015['TOTAL'].max())

    keys = list(dicc2015.keys())
    indice = df_grouped_2015['TOTAL'].idxmax()
    clave = keys[indice]
    nuevo_obj_2015['modaMes'] = clave
    junio = float(dicc2015['JUNIO'])
    julio = float(dicc2015['JULIO'])
    mediana = (junio+julio)/2
    nuevo_obj_2015['mediana'] = mediana
    nuevo_obj_2015['desviacion'] = round(df_grouped_2015['TOTAL'].std(),2)
    listaObjects.append(nuevo_obj_2015)
    #2016------------------------------------------------------------
    df2016 = df.loc[df['ANIO']==2016]
    df_grouped_2016 = df2016.groupby('MES')['TOTAL'].sum().reset_index()
    dicc2016 = dict(zip(df_grouped_2016['MES'], df_grouped_2016['TOTAL']))  # Convert to dictionary with month as key
    dicc2016['anio'] = 2016
    meses = ['ENERO', 'FEBRERO', 'MARZO', 'ABRIL', 'MAYO', 'JUNIO', 'JULIO', 'AGOSTO', 'SEPTIEMBRE', 'OCTUBRE', 'NOVIEMBRE', 'DICIEMBRE']
    nuevo_obj_2016 = {}
    for mes in meses:
      nuevo_obj_2016[mes] = dicc2016[mes]
    nuevo_obj_2016['anio'] = dicc2016['anio']
    nuevo_obj_2016['media'] = round(float(df_grouped_2016['TOTAL'].mean()),2)
    nuevo_obj_2016['moda'] = float(df_grouped_2016['TOTAL'].max())

    keys = list(dicc2016.keys())
    indice = df_grouped_2016['TOTAL'].idxmax()
    clave = keys[indice]
    nuevo_obj_2016['modaMes'] = clave
    junio = float(dicc2016['JUNIO'])
    julio = float(dicc2016['JULIO'])
    mediana = (junio+julio)/2
    nuevo_obj_2016['mediana'] = mediana
    nuevo_obj_2016['desviacion'] = round(df_grouped_2016['TOTAL'].std(),2)
    listaObjects.append(nuevo_obj_2016)
    #2017------------------------------------------------------------
    df2017 = df.loc[df['ANIO']==2017]
    df_grouped_2017 = df2017.groupby('MES')['TOTAL'].sum().reset_index()
    dicc2017 = dict(zip(df_grouped_2017['MES'], df_grouped_2017['TOTAL']))  # Convert to dictionary with month as key
    dicc2017['anio'] = 2017
    meses = ['ENERO', 'FEBRERO', 'MARZO', 'ABRIL', 'MAYO', 'JUNIO', 'JULIO', 'AGOSTO', 'SEPTIEMBRE', 'OCTUBRE', 'NOVIEMBRE', 'DICIEMBRE']
    nuevo_obj_2017 = {}
    for mes in meses:
      nuevo_obj_2017[mes] = dicc2017[mes]
    nuevo_obj_2017['anio'] = dicc2017['anio']
    nuevo_obj_2017['media'] = round(float(df_grouped_2017['TOTAL'].mean()),2)
    nuevo_obj_2017['moda'] = float(df_grouped_2017['TOTAL'].max())

    keys = list(dicc2017.keys())
    indice = df_grouped_2017['TOTAL'].idxmax()
    clave = keys[indice]
    nuevo_obj_2017['modaMes'] = clave
    junio = float(dicc2017['JUNIO'])
    julio = float(dicc2017['JULIO'])
    mediana = (junio+julio)/2
    nuevo_obj_2017['mediana'] = mediana
    nuevo_obj_2017['desviacion'] = round(df_grouped_2017['TOTAL'].std(),2)
    listaObjects.append(nuevo_obj_2017)
    #2018------------------------------------------------------------
    df2018 = df.loc[df['ANIO']==2018]
    df_grouped_2018 = df2018.groupby('MES')['TOTAL'].sum().reset_index()
    dicc2018 = dict(zip(df_grouped_2018['MES'], df_grouped_2018['TOTAL']))  # Convert to dictionary with month as key
    dicc2018['anio'] = 2018
    meses = ['ENERO', 'FEBRERO', 'MARZO', 'ABRIL', 'MAYO', 'JUNIO', 'JULIO', 'AGOSTO', 'SEPTIEMBRE', 'OCTUBRE', 'NOVIEMBRE', 'DICIEMBRE']
    nuevo_obj_2018 = {}
    for mes in meses:
      nuevo_obj_2018[mes] = dicc2018[mes]
    nuevo_obj_2018['anio'] = dicc2018['anio']
    nuevo_obj_2018['media'] = round(float(df_grouped_2018['TOTAL'].mean()),2)
    nuevo_obj_2018['moda'] = float(df_grouped_2018['TOTAL'].max())

    keys = list(dicc2018.keys())
    indice = df_grouped_2018['TOTAL'].idxmax()
    clave = keys[indice]
    nuevo_obj_2018['modaMes'] = clave
    junio = float(dicc2018['JUNIO'])
    julio = float(dicc2018['JULIO'])
    mediana = (junio+julio)/2
    nuevo_obj_2018['mediana'] = mediana
    nuevo_obj_2018['desviacion'] = round(df_grouped_2018['TOTAL'].std(),2)
    listaObjects.append(nuevo_obj_2018)
    #2019------------------------------------------------------------
    df2019 = df.loc[df['ANIO']==2019]
    df_grouped_2019 = df2019.groupby('MES')['TOTAL'].sum().reset_index()
    dicc2019 = dict(zip(df_grouped_2019['MES'], df_grouped_2019['TOTAL']))  # Convert to dictionary with month as key
    dicc2019['anio'] = 2019
    meses = ['ENERO', 'FEBRERO', 'MARZO', 'ABRIL', 'MAYO', 'JUNIO', 'JULIO', 'AGOSTO', 'SEPTIEMBRE', 'OCTUBRE', 'NOVIEMBRE', 'DICIEMBRE']
    nuevo_obj_2019 = {}
    for mes in meses:
      nuevo_obj_2019[mes] = dicc2019[mes]
    nuevo_obj_2019['anio'] = dicc2019['anio']
    nuevo_obj_2019['media'] = round(float(df_grouped_2019['TOTAL'].mean()),2)
    nuevo_obj_2019['moda'] = float(df_grouped_2019['TOTAL'].max())

    keys = list(dicc2019.keys())
    indice = df_grouped_2019['TOTAL'].idxmax()
    clave = keys[indice]
    nuevo_obj_2019['modaMes'] = clave
    junio = float(dicc2019['JUNIO'])
    julio = float(dicc2019['JULIO'])
    mediana = (junio+julio)/2
    nuevo_obj_2019['mediana'] = mediana
    nuevo_obj_2019['desviacion'] = round(df_grouped_2019['TOTAL'].std(),2)
    listaObjects.append(nuevo_obj_2019)
    #2020------------------------------------------------------------
    df2020 = df.loc[df['ANIO']==2020]
    df_grouped_2020 = df2020.groupby('MES')['TOTAL'].sum().reset_index()
    dicc2020 = dict(zip(df_grouped_2020['MES'], df_grouped_2020['TOTAL']))  # Convert to dictionary with month as key
    dicc2020['anio'] = 2020
    meses = ['ENERO', 'FEBRERO', 'MARZO', 'ABRIL', 'MAYO', 'JUNIO', 'JULIO', 'AGOSTO', 'SEPTIEMBRE', 'OCTUBRE', 'NOVIEMBRE', 'DICIEMBRE']
    nuevo_obj_2020 = {}
    for mes in meses:
      nuevo_obj_2020[mes] = dicc2020[mes]
    nuevo_obj_2020['anio'] = dicc2020['anio']
    nuevo_obj_2020['media'] = round(float(df_grouped_2020['TOTAL'].mean()),2)
    nuevo_obj_2020['moda'] = float(df_grouped_2020['TOTAL'].max())

    keys = list(dicc2020.keys())
    indice = df_grouped_2020['TOTAL'].idxmax()
    clave = keys[indice]
    nuevo_obj_2020['modaMes'] = clave
    junio = float(dicc2020['JUNIO'])
    julio = float(dicc2020['JULIO'])
    mediana = (junio+julio)/2
    nuevo_obj_2020['mediana'] = mediana
    nuevo_obj_2020['desviacion'] = round(df_grouped_2020['TOTAL'].std(),2)
    listaObjects.append(nuevo_obj_2020)
    #2021------------------------------------------------------------
    df2021 = df.loc[df['ANIO']==2021]
    df_grouped_2021 = df2021.groupby('MES')['TOTAL'].sum().reset_index()
    dicc2021 = dict(zip(df_grouped_2021['MES'], df_grouped_2021['TOTAL']))  # Convert to dictionary with month as key
    dicc2021['anio'] = 2021
    meses = ['ENERO', 'FEBRERO', 'MARZO', 'ABRIL', 'MAYO', 'JUNIO', 'JULIO', 'AGOSTO', 'SEPTIEMBRE', 'OCTUBRE', 'NOVIEMBRE', 'DICIEMBRE']
    nuevo_obj_2021 = {}
    for mes in meses:
      nuevo_obj_2021[mes] = dicc2021[mes]
    nuevo_obj_2021['anio'] = dicc2021['anio']
    nuevo_obj_2021['media'] = round(float(df_grouped_2021['TOTAL'].mean()),2)
    nuevo_obj_2021['moda'] = float(df_grouped_2021['TOTAL'].max())

    keys = list(dicc2021.keys())
    indice = df_grouped_2021['TOTAL'].idxmax()
    clave = keys[indice]
    nuevo_obj_2021['modaMes'] = clave
    junio = float(dicc2021['JUNIO'])
    julio = float(dicc2021['JULIO'])
    mediana = (junio+julio)/2
    nuevo_obj_2021['mediana'] = mediana
    nuevo_obj_2021['desviacion'] = round(df_grouped_2021['TOTAL'].std(),2)
    listaObjects.append(nuevo_obj_2021)
    #2022------------------------------------------------------------
    df2022 = df.loc[df['ANIO']==2022]
    df_grouped_2022 = df2022.groupby('MES')['TOTAL'].sum().reset_index()
    dicc2022 = dict(zip(df_grouped_2022['MES'], df_grouped_2022['TOTAL']))  # Convert to dictionary with month as key
    dicc2022['anio'] = 2022
    meses = ['ENERO', 'FEBRERO', 'MARZO', 'ABRIL', 'MAYO', 'JUNIO', 'JULIO', 'AGOSTO', 'SEPTIEMBRE', 'OCTUBRE', 'NOVIEMBRE', 'DICIEMBRE']
    nuevo_obj_2022 = {}
    for mes in meses:
      nuevo_obj_2022[mes] = dicc2022[mes]
    nuevo_obj_2022['anio'] = dicc2022['anio']
    nuevo_obj_2022['media'] = round(float(df_grouped_2022['TOTAL'].mean()),2)
    nuevo_obj_2022['moda'] = float(df_grouped_2022['TOTAL'].max())

    keys = list(dicc2022.keys())
    indice = df_grouped_2022['TOTAL'].idxmax()
    clave = keys[indice]
    nuevo_obj_2022['modaMes'] = clave
    junio = float(dicc2022['JUNIO'])
    julio = float(dicc2022['JULIO'])
    mediana = (junio+julio)/2
    nuevo_obj_2022['mediana'] = mediana
    nuevo_obj_2022['desviacion'] = round(df_grouped_2022['TOTAL'].std(),2)
    listaObjects.append(nuevo_obj_2022)
    return listaObjects

   
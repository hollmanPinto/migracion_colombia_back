import json
import pandas as pd
from sqlalchemy import create_engine;
from django.http import HttpResponse;
from api.models.migracion_colombia import MigracionColombia
from api.controllers.migracion_serializer import MigracionSerializer
from unidecode import unidecode
from datetime import datetime

class ProcesamientoDatos():
  df=0
    #Cargar archivo csv ------------------------------------------------
  def cargar_csv(self,ruta):
    #IMPORTACION DEL DATASET-------------------------------------------------
    global df
    ds = pd.read_csv(ruta,dtype=str)
    ds.info()
    ds['delito'] = ds['delito'].fillna('ARTÍCULO 229. Violencia Intrafamiliar')
    ds['DEPARTAMENTO'] = ds['DEPARTAMENTO'].apply(lambda x: unidecode(x))
    ds['MUNICIPIO'] = ds['MUNICIPIO'].apply(lambda x: unidecode(x))
    ds['ARMAS MEDIOS'] = ds['ARMAS MEDIOS'].apply(lambda x: unidecode(x))
    ds['GENERO'] = ds['GENERO'].apply(lambda x: unidecode(x))
    ds['delito'] = ds['delito'].apply(lambda x: unidecode(x))
    ds = ds.applymap(lambda x: 'NO REPORTA'  if (x == 'NO APLICA') else (x) )
    ds = ds.applymap(lambda x: 'NO REPORTA'  if (x == '-') else (x) )
    ds = ds.applymap(lambda x: 'NO REPORTA'  if (x == 'NO REPORTADO') else (x) )
    ds = ds.fillna('NO REPORTA')

    condicion1 = ds['CODIGO DANE'] == 'CODIGO DANE'
    condicion2 = ds['MUNICIPIO'] == 'MUNICIPIO'
    condicion3 = ds['DEPARTAMENTO'] == 'NO REPORTA'
    condicion4 = ds['MUNICIPIO'] == 'NO REPORTA'

    ds = ds[~(condicion1 & condicion2)]
    ds = ds[~(condicion3 & condicion4)]

    filas_a_actualizar = ds['delito'].str.contains('ARTICULO 211.')
    ds.loc[filas_a_actualizar,'delito'] ='ARTICULO 211. DELITO SEXUAL AGRAVADO'

    filas_a_actualizar = ds['delito'].str.contains('ARTICULO 216.')
    ds.loc[filas_a_actualizar,'delito']='ARTICULO 216. DELITO RELACIONADO A PROSTITUCION AGRAVADO'
    #verificar formato fecha-----------------------------------------------------------
    def verificar_formato_fecha(fecha):
      try:
        datetime.strptime(fecha, '%d/%m/%Y')
        return True
      except ValueError:
        return False
      
    f_validas = ds['FECHA HECHO'].apply(verificar_formato_fecha)
    ds[['DIA','MES','ANIO']] = ds.loc[f_validas,'FECHA HECHO'].str.split('/',expand=True)
    ds['DIA'] = ds['DIA'].astype(int)
    ds['MES'] = ds['MES'].astype(int)
    ds['ANIO'] = ds['ANIO'].astype(int)

    #Eliminamos columnas innecesarias
    ds = ds.fillna('NO REPORTA')
    ds= ds.drop('CODIGO DANE',axis=1)
    ds= ds.drop('FECHA HECHO',axis=1)
    ds= ds.drop('CANTIDAD',axis=1)
    ds = ds.fillna('NO REPORTA')

    #INSERCION EN BASE DE DATOS DE MYSQL-------------------------------------
    engine = create_engine("mysql+pymysql://{user}:{pw}@localhost/{db}"
                           .format(user="root",
                                   pw="admin",
                                   db="especializacion"))
    ds.to_sql(con=engine, name='schema_policia', if_exists='replace')
    ds = ds.rename(columns={'ARMAS MEDIOS': 'ARMAS_MEDIOS'})
    ds = ds.rename(columns={'GRUPO ETARIO': 'GRUPO_ETARIO'})
    ds = ds.rename(columns={'delito': 'DELITO'})

    df = ds.copy()
    return

  #obtener datos filtrados por años--------------------------------------------------
  def datos_anio(self,anio,cantidad,nacionalidad,mes):
    lista_df = []
    d1=df.copy()
    if(len(cantidad)==0):
       cantidad='1'
    if(len(anio)!=0):
      d1 = df.query('ANIO=={}'.format(anio))
    if(len(nacionalidad)!=0):
      d1 = d1.query('DEPARTAMENTO=="{}"'.format(nacionalidad))
    if(len(mes)!=0):
      d1 = d1.query('MES=={}'.format(mes))
    d2 = d1.head(int(cantidad))
    for indice, fila in d2.iterrows():
        lista_df.append(ProcesamientoDatos.df2model(fila))
    serializer = MigracionSerializer(lista_df, many = True)
    return serializer

  #convertir de df a model-------------------
  def df2model(registro):
    migracion = MigracionColombia()
    migracion.DEPARTAMENTO = registro['DEPARTAMENTO']
    migracion.MUNICIPIO = registro['MUNICIPIO']
    migracion.ARMAS_MEDIOS = registro['ARMAS_MEDIOS']
    migracion.GENERO = registro['GENERO']
    migracion.GRUPO_ETARIO = registro['GRUPO_ETARIO']
    migracion.DELITO = registro['DELITO']
    migracion.DIA = registro['DIA']
    migracion.MES = registro['MES']
    migracion.ANIO = registro['ANIO']
    return migracion
  
  #obtener cantidad de crimenes por anio------------
  def crimenesXanio(self,rangoFechas):
    #Crímenes reportados por año
    anios = rangoFechas.split('-',2)
    anio_inicial= int(anios[0])
    anio_final = int(anios[1])
    df_filtrado = df[(df['ANIO'] >= anio_inicial) & (df['ANIO'] <= anio_final)]
    reportes_por_anio = df_filtrado['ANIO'].value_counts().to_dict()
    return reportes_por_anio
  #Víctimas separadas por género por año
  def proporcionHM(self,rangoAnios):
    anios = str(rangoAnios)
    rangos = anios.split('-',2)
    anio_inicial= int(rangos[0])
    anio_final = int(rangos[1])
    df_filtrado = df[(df['ANIO'] >= anio_inicial) & (df['ANIO'] <= anio_final)]
    proporcion_victimas = df_filtrado.groupby('GENERO').size().to_dict()
    return proporcion_victimas
  #Delitos más cometidos por anio
  def delitosXanio(self, rangoAnios):
    anios = str(rangoAnios)
    rangos = anios.split('-',2)
    anio_inicial= int(rangos[0])
    anio_final = int(rangos[1])
    df_filtrado = df[(df['ANIO'] >= anio_inicial) & (df['ANIO'] <= anio_final)]
    delitos_mas_cometidos = df_filtrado['DELITO'].apply(lambda x: x.split('. ')[-1]).value_counts().to_dict()
    return delitos_mas_cometidos
#GRAFICOS DE BARRAS-----------------------------------------------------------------
  def topPaises(self,top):
    #Reporte departamentos con más denuncias
    reportes_por_departamento = df['DEPARTAMENTO'].value_counts()
    reportes_por_departamento = reportes_por_departamento.head(top)
    reportes_por_departamento_dicc = reportes_por_departamento.to_dict()
    return reportes_por_departamento_dicc
  
#GRAFICOS DE FUNCION------------------------------------------------------------------
  def cantidadMesesAnios(self):
    #delitos por año mes total
    nombres_meses = {
        1: 'enero',
        2: 'febrero',
        3: 'marzo',
        4: 'abril',
        5: 'mayo',
        6: 'junio',
        7: 'julio',
        8: 'agosto',
        9: 'septiembre',
        10: 'octubre',
        11: 'noviembre',
        12: 'diciembre'
    }
    df_result = df.groupby(['ANIO', 'MES']).size().reset_index(name='total_delitos')   


    df_result = df_result.sort_values(by=['ANIO', 'MES'], ascending=[True, True])
    df_result['MES'] = df_result['MES'].map(nombres_meses)
    result_dict = df_result.to_dict(orient='records')
    print(result_dict)
    return result_dict
  #ESTADISTICA DESCRIPTVA------------------------------------
  def estadistica(self):
    df_grouped = df.groupby('ANIO')

    nombres_meses = {
        1: 'enero',
        2: 'febrero',
        3: 'marzo',
        4: 'abril',
        5: 'mayo',
        6: 'junio',
        7: 'julio',
        8: 'agosto',
        9: 'septiembre',
        10: 'octubre',
        11: 'noviembre',
        12: 'diciembre'
    }

    # Lista para almacenar los objetos por año
    resultado = []

    # Recorrer cada grupo de datos por año
    for year, group in df_grouped:
        # Calcular las estadísticas
        promedio_denuncias_mes = group.groupby('MES').size().mean()
        max_denuncias_mes = group.groupby('MES').size().max()
        mes_mas_denuncias = group.groupby('MES').size().idxmax()
        mediana_denuncias = group.groupby('MES').size().median()
        desviacion_estandar = group.groupby('MES').size().std()

        # Traducir el mes con el nombre correspondiente
        mes_mas_denuncias_nombre = nombres_meses[mes_mas_denuncias]

        promedio_denuncias_mes = float(promedio_denuncias_mes)
        max_denuncias_mes = float(max_denuncias_mes)
        mes_mas_denuncias = int(mes_mas_denuncias)
        mediana_denuncias = float(mediana_denuncias)
        desviacion_estandar = float(desviacion_estandar)
        # Crear el objeto con los datos
        objeto = {
            'agno': year,
            'promedio_denuncias_mes': round(promedio_denuncias_mes,2),
            'cantidad_denuncias_mes_mayor': round(max_denuncias_mes,2),
            'mes_mas_denuncias': mes_mas_denuncias_nombre,
            'mediana_denuncias': round(mediana_denuncias,2),
            'desviacion_estandar': round(desviacion_estandar,2)
        }

        # Agregar el objeto a la lista de resultados
        resultado.append(objeto)

    resultado_ordenado = sorted(resultado, key=lambda x: x['agno'])
    cadena_json = json.dumps(resultado_ordenado)
    lista_objetos = json.loads(cadena_json)
    #resultado_dict = dict(enumerate(resultado_ordenado, start=1))
    return cadena_json
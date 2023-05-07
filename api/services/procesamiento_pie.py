from api.services.procesamiento_datos import ProcesamientoDatos
import pandas as pd

class ProcesamientoPie():
     #obtener cantidad de personas por anio------------
    def personasXanio(self,rangoFechas):
      self.procDatos = ProcesamientoDatos()
      df = self.procDatos.obtenerDf()
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
      self.procDatos = ProcesamientoDatos()
      df = self.procDatos.obtenerDf()
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
      self.procDatos = ProcesamientoDatos()
      df = self.procDatos.obtenerDf()
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
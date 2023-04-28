from rest_framework import viewsets
from .serializers import MigracionSerializer
from .models import MigracionColombia
from django.http import HttpResponse, HttpResponseNotAllowed
from django.views.decorators.csrf import csrf_exempt
import json
import pandas as pd
import mysql.connector
from sqlalchemy import create_engine;

# Create your views here.
class ProgrammerViewSet(viewsets.ModelViewSet):
    queryset = MigracionColombia.objects.all()
    serializer_class = MigracionSerializer

#cargar ruta del archivo csv ---------------------------------------
@csrf_exempt
def ejemplo_vista(request):
    if request.method == 'POST':
        json_data = json.loads(request.body)
        cargar_csv(json_data['ruta'])
        return HttpResponse(status=200)
    return HttpResponseNotAllowed(['POST'])

#Cargar archivo csv ------------------------------------------------
def cargar_csv(ruta):
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
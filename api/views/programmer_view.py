from rest_framework import viewsets
from api.controllers.migracion_serializer import MigracionSerializer
from api.models.migracion_colombia import MigracionColombia
from django.http import HttpResponse, HttpResponseNotAllowed
from django.views.decorators.csrf import csrf_exempt
from api.services.procesamiento_datos import ProcesamientoDatos
from rest_framework.renderers import JSONRenderer
from rest_framework.decorators import api_view
from rest_framework.response import Response
import json


# Create your views here.
class ProgrammerViewSet(viewsets.ModelViewSet):
    queryset = MigracionColombia.objects.all()
    serializer_class = MigracionSerializer
#cargar ruta del archivo csv ---------------------------------------
@api_view(['POST'])
@csrf_exempt
def ejemplo_vista(request):
    if request.method == 'POST':
        json_data = json.loads(request.body)
        ruta = json_data['ruta']
        print(ruta)
        procesamientoDatos = ProcesamientoDatos()
        try:
            procesamientoDatos.cargar_csv(ruta)
            res = {
                'valor':True
            }
        except:
            res = {
                'valor':False
            }
        json_res = json.dumps(res)
        return HttpResponse(json_res,content_type="application/json",status=200)
    return HttpResponseNotAllowed(['POST'])
#obtener tabla de datos por año---------------------------------------
@csrf_exempt
def vista_anios(request):
    procesamientoDatos = ProcesamientoDatos()
    if request.method == 'POST':
        json_data = json.loads(request.body)
        procesamientoDatos = ProcesamientoDatos()
        anios=json_data['anio']
        cantidad=str(json_data['cantidad'])
        nacionalidad=json_data['nacionalidad'].upper()
        mes = json_data['mes'].upper()
        proc = procesamientoDatos.datos_anio(anios,cantidad,nacionalidad,mes)
        json_out = JSONRenderer().render(proc.data)
        print(json_out)
        return HttpResponse(json_out,content_type="application/json",status=200)
    return HttpResponseNotAllowed(['POST'])
#obtener personas por años---------------------------------------------------
@csrf_exempt
def crimenesXanio(request):
    procesamientoDatos = ProcesamientoDatos()
    if request.method == 'POST':
        json_data = json.loads(request.body)
        rangoAnios = json_data['rangoAnios']#[{"anio":"2012", "personas":"500"},{{"anio":"2013", "personas":"800"}}]
        proc = procesamientoDatos.crimenesXanio(rangoAnios)
        json_out = json.dumps(proc)
        return HttpResponse(json_out,content_type="application/json",status=200)
#obtener top de paises con mayor migracion---------------------------------------
@csrf_exempt
def proporcion_hombres_mujeres(request):
    procesamientoDatos = ProcesamientoDatos()
    if request.method == 'POST':
        json_data = json.loads(request.body)
        rangoAnios = json_data['rangoAnios']
        proc = procesamientoDatos.proporcionHM(rangoAnios)
        json_out = json.dumps(proc)
        return HttpResponse(json_out,content_type="application/json",status=200)
#obtener ingresos de migrantes por trimestres------------------------------------
@csrf_exempt
def delistos_x_anio(request):
    procesamientoDatos = ProcesamientoDatos()
    if request.method == 'POST':
        json_data = json.loads(request.body)
        rangoAnios = json_data['rangoAnios']
        proc = procesamientoDatos.delitosXanio(rangoAnios)
        json_out = json.dumps(proc)
        return HttpResponse(json_out,content_type="application/json",status=200)
#obtener top de paises-----------------------------------------------------------
@csrf_exempt
def top_paises(request):
    procesamientoDatos = ProcesamientoDatos()
    if request.method == 'POST':
        json_data = json.loads(request.body)
        top = json_data['top']
        proc = procesamientoDatos.topPaises(top)
        json_out = json.dumps(proc)
        return HttpResponse(json_out,content_type="application/json",status=200)
#GRAFICAS DE FUNCIONES-----------------------------------------------
#obtener cantidadXmesesXaños-----------------------------------------
@csrf_exempt
def cantidad_meses_anios(request):
    procesamientoDatos = ProcesamientoDatos()
    if request.method == 'GET':
        proc = procesamientoDatos.cantidadMesesAnios()
        json_out = json.dumps(proc)
        return HttpResponse(json_out,content_type="application/json",status=200)
#obtener estadisticas-------------------------------------------------
@csrf_exempt
def estadistica(request):
    procesamientoDatos = ProcesamientoDatos()
    if request.method == 'GET':
        proc = procesamientoDatos.estadistica()
        json_out = json.dumps(proc)
        return HttpResponse(proc,content_type="application/json",status=200)
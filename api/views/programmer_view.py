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
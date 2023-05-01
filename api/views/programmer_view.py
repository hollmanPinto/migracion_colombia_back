from rest_framework import viewsets
from api.controllers.migracion_serializer import MigracionSerializer
from api.models.migracion_colombia import MigracionColombia
from django.http import HttpResponse, HttpResponseNotAllowed
from django.views.decorators.csrf import csrf_exempt
from api.services.procesamiento_datos import ProcesamientoDatos
from rest_framework.renderers import JSONRenderer
import json


# Create your views here.
class ProgrammerViewSet(viewsets.ModelViewSet):
    queryset = MigracionColombia.objects.all()
    serializer_class = MigracionSerializer
#cargar ruta del archivo csv ---------------------------------------
@csrf_exempt
def ejemplo_vista(request):
    if request.method == 'POST':
        json_data = json.loads(request.body)
        ruta = json_data['ruta']
        print(ruta)
        procesamientoDatos = ProcesamientoDatos()
        procesamientoDatos.cargar_csv(ruta)
        return HttpResponse(status=200)
    return HttpResponseNotAllowed(['POST'])
#obtener tabla de datos por año---------------------------------------
@csrf_exempt
def vista_anios(request):
    procesamientoDatos = ProcesamientoDatos()
    if request.method == 'POST':
        json_data = json.loads(request.body)
        procesamientoDatos = ProcesamientoDatos()
        anios=json_data['anio']
        cantidad=json_data['cantidad']
        nacionalidad=json_data['nacionalidad'].upper()
        proc = procesamientoDatos.datos_anio(anios,cantidad,nacionalidad)
        json_out = JSONRenderer().render(proc.data)
        print(json_out)
        return HttpResponse(json_out,content_type="application/json",status=200)
    return HttpResponseNotAllowed(['POST'])
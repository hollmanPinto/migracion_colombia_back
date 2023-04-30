from rest_framework import viewsets
from api.controllers.migracion_serializer import MigracionSerializer
from api.models.migracion_colombia import MigracionColombia
from django.http import HttpResponse, HttpResponseNotAllowed
from django.views.decorators.csrf import csrf_exempt
from api.services.procesamiento_datos import ProcesamientoDatos
import json


# Create your views here.
class ProgrammerViewSet(viewsets.ModelViewSet):
    queryset = MigracionColombia.objects.all()
    serializer_class = MigracionSerializer

#cargar ruta del archivo csv ---------------------------------------
@csrf_exempt
def ejemplo_vista(request):
    procesamientoDatos = ProcesamientoDatos()
    if request.method == 'POST':
        json_data = json.loads(request.body)
        ruta = json_data['ruta']
        print(ruta)
        procesamientoDatos.cargar_csv(ruta)
        return HttpResponse(status=200)
    return HttpResponseNotAllowed(['POST'])
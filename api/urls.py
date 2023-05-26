from django.urls import path, include
from rest_framework import routers
from api.views.programmer_view import ProgrammerViewSet
from api.views.programmer_view import ejemplo_vista
from api.views.programmer_view import vista_anios
from api.views.programmer_view import crimenesXanio
from api.views.programmer_view import proporcion_hombres_mujeres
from api.views.programmer_view import delistos_x_anio
from api.views.programmer_view import top_paises
from api.views.programmer_view import cantidad_meses_anios
from api.views.programmer_view import estadistica

router = routers.DefaultRouter()

router.register(r'migracion',ProgrammerViewSet)

urlpatterns = [
    path('',include(router.urls)),
    path('loadCsv',ejemplo_vista),
    path('verData',vista_anios),
    path('crimenesXanio',crimenesXanio),
    path('proporcionHm',proporcion_hombres_mujeres),
    path('delitosXanio',delistos_x_anio),
    path('topPaises',top_paises),
    path('cantidadMesesAnios',cantidad_meses_anios),
    path('estadistica',estadistica)
]
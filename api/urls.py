from django.urls import path, include
from rest_framework import routers
from api.views.programmer_view import ProgrammerViewSet
from api.views.programmer_view import ejemplo_vista
from api.views.programmer_view import vista_anios
from api.views.programmer_view import personas_x_anios
from api.views.programmer_view import proporcion_hombres_mujeres
from api.views.programmer_view import ingresos_migrantes_trimestres
from api.views.programmer_view import top_paises
from api.views.programmer_view import cantidad_meses_anios

router = routers.DefaultRouter()

router.register(r'migracion',ProgrammerViewSet)

urlpatterns = [
    path('',include(router.urls)),
    path('loadCsv',ejemplo_vista),
    path('verData',vista_anios),
    path('personasXanio',personas_x_anios),
    path('proporcionHm',proporcion_hombres_mujeres),
    path('ingresosMigrantesTr',ingresos_migrantes_trimestres),
    path('topPaises',top_paises),
    path('cantidadMesesAnios',cantidad_meses_anios)
]
from django.urls import path, include
from rest_framework import routers
from api.views.programmer_view import ProgrammerViewSet
from api.views.programmer_view import ejemplo_vista
from api.views.programmer_view import vista_anios

router = routers.DefaultRouter()

router.register(r'migracion',ProgrammerViewSet)

urlpatterns = [
    path('',include(router.urls)),
    path('loadCsv',ejemplo_vista),
    path('verData',vista_anios)
]
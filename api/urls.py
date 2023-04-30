from django.urls import path, include
from rest_framework import routers
from api.views.programmer_view import ProgrammerViewSet
from api.views.programmer_view import ejemplo_vista

router = routers.DefaultRouter()

router.register(r'migracion',ProgrammerViewSet)

urlpatterns = [
    path('',include(router.urls)),
    path('loadCsv',ejemplo_vista),
]
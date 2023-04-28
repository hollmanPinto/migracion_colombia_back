from django.urls import path, include
from rest_framework import routers
from api import views

router = routers.DefaultRouter()

router.register(r'migracion',views.ProgrammerViewSet)

urlpatterns = [
    path('',include(router.urls)),
    path('hello_world',views.ejemplo_vista),
]
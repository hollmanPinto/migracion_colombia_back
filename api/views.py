from rest_framework import viewsets
from .serializers import MigracionSerializer
from .models import MigracionColombia

# Create your views here.
class ProgrammerViewSet(viewsets.ModelViewSet):
    queryset = MigracionColombia.objects.all()
    serializer_class = MigracionSerializer

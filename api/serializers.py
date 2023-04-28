from rest_framework import serializers
from .models import MigracionColombia

class MigracionSerializer(serializers.ModelSerializer):
    class Meta:
        model = MigracionColombia
        fields = '__all__'
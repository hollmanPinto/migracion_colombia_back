from rest_framework import serializers
from api.models.migracion_colombia import MigracionColombia

class MigracionSerializer(serializers.ModelSerializer):
    class Meta:
        model = MigracionColombia
        fields = '__all__'
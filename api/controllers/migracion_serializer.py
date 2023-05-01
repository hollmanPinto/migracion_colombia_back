from rest_framework import serializers
from api.models.migracion_colombia import MigracionColombia

class MigracionSerializer(serializers.ModelSerializer):
    #index = serializers.SerializerMethodField()
    class Meta:
        model = MigracionColombia
        fields = '__all__'
    #def get_index(self, obj):
     #   return 0

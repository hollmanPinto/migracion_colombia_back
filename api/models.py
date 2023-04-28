from django.db import models

# Create your models here.
class MigracionColombia(models.Model):
    index = models.IntegerField(primary_key=True, null=False, default=0)
    ANIO = models.IntegerField(null=False, default=0)
    MES = models.CharField(max_length=20, null=False, default='')
    NACIONALIDAD = models.CharField(max_length=50, null=False, default='')
    ISO_3166 = models.IntegerField(null=True)
    FEMENINO = models.IntegerField(null=True)
    MASCULINO = models.IntegerField(null=True)
    INDEFINIDO = models.CharField(max_length=10, null=True)
    TOTAL = models.IntegerField(null=True)
    LATITUD_LONGITUD = models.CharField(max_length=100, null=True)
from django.db import models

# Create your models here.
class MigracionColombia(models.Model):
    DEPARTAMENTO = models.CharField(max_length=2000, default='')
    MUNICIPIO = models.CharField(max_length=2000, default='')
    ARMAS_MEDIOS = models.CharField(max_length=2000, default='')
    GENERO = models.CharField(max_length=2000, default='')
    GRUPO_ETARIO = models.CharField(max_length=2000, default='')
    DELITO = models.CharField(max_length=2000, default='')
    DIA = models.IntegerField()
    MES = models.IntegerField()
    ANIO = models.IntegerField()






    

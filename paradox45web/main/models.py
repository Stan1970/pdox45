from django.db import models

class Employee(models.Model):
    osobni_cislo = models.IntegerField(primary_key=True)
    Jmeno = models.CharField(max_length=50)
    Prijmeni = models.CharField(max_length=50)
    pozice = models.CharField(max_length=50)
    plat = models.FloatField()

    def __str__(self):
        return f"{self.osobni_cislo} - {self.Jmeno} {self.Prijmeni}"


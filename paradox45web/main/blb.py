from main.models import Employee

jmena = ['Jan', 'Petr', 'Lucie', 'Eva', 'Martin', 'Jana', 'Tomáš', 'Kateřina', 'Michal', 'Veronika']
prijmeni = ['Novák', 'Svoboda', 'Dvořáková', 'Procházková', 'Černý', 'Králová', 'Kučera', 'Veselá', 'Horák', 'Němcová']
pozice = ['Programátor', 'Analytik', 'Tester', 'Manažer']
plat_base = [52000, 48000, 41000, 65000]

for i in range(1, 101):
    Employee.objects.create(
        osobni_cislo=i,
        Jmeno=jmena[i % len(jmena)],
        Prijmeni=prijmeni[i % len(prijmeni)],
        pozice=pozice[i % len(pozice)],
        plat=plat_base[i % len(plat_base)] + (i * 100) % 5000
    )

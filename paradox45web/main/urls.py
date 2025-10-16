from django.urls import path
from . import views

urlpatterns = [
    path('', views.home, name='home'),
    path('view/', views.view, name='view'),
    path('view/<str:table_name>/', views.view_table, name='view_table'),
    path('create/table/', views.createtable, name='createtable'),
    path('ask/', views.ask, name='ask'),  # nová URL pro ASK

]
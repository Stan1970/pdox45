from django.urls import path
from . import views

urlpatterns = [
    path('', views.home, name='home'),
    path('view/', views.view, name='view'),
    path('view/<str:table_name>/', views.view_table, name='view_table'),
    path('create/table/', views.createtable, name='createtable'),
    path('edit/<str:table_name>/', views.edit_table, name='edit_table'),
    path('edit/<str:table_name>/row/<int:rowid>/', views.edit_row, name='edit_row'),
    path('ask/', views.ask, name='ask'),  # nová URL pro ASK
    path('import/', views.imports_view, name='import'),  # URL pro Import
]
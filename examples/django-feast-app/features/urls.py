from django.urls import path
from . import views

urlpatterns = [
    path('features/<int:user_id>/', views.get_features, name='get_features'),
    path('stats/<int:user_id>/', views.get_stats, name='get_stats'),
]

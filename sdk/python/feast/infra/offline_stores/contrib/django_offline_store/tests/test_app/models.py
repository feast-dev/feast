from django.db import models


class Driver(models.Model):
    """Test model for driver features."""
    driver_id = models.IntegerField(primary_key=True)
    event_timestamp = models.DateTimeField()
    created = models.DateTimeField(auto_now_add=True)
    conv_rate = models.FloatField()
    acc_rate = models.FloatField()
    avg_daily_trips = models.IntegerField()

    class Meta:
        app_label = 'test_app'
        db_table = 'driver_stats'


class Order(models.Model):
    """Test model for order features."""
    order_id = models.IntegerField(primary_key=True)
    driver = models.ForeignKey(Driver, on_delete=models.CASCADE)
    event_timestamp = models.DateTimeField()
    created = models.DateTimeField(auto_now_add=True)
    amount = models.DecimalField(max_digits=10, decimal_places=2)
    status = models.CharField(max_length=20)

    class Meta:
        app_label = 'test_app'
        db_table = 'order_stats'

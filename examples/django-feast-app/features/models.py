from django.db import models

class UserStats(models.Model):
    """User statistics model for feature store."""
    user_id = models.IntegerField(primary_key=True)
    event_timestamp = models.DateTimeField()
    total_orders = models.IntegerField(default=0)
    average_order_value = models.FloatField(default=0.0)
    last_order_date = models.DateTimeField(null=True)
    is_active = models.BooleanField(default=True)

    class Meta:
        db_table = 'user_stats'
        ordering = ['-event_timestamp']

class Order(models.Model):
    """Order model for generating user statistics."""
    order_id = models.AutoField(primary_key=True)
    user_id = models.IntegerField()
    amount = models.DecimalField(max_digits=10, decimal_places=2)
    created_at = models.DateTimeField(auto_now_add=True)
    status = models.CharField(max_length=20)

    class Meta:
        db_table = 'orders'
        ordering = ['-created_at']

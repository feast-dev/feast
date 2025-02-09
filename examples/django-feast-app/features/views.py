from django.http import JsonResponse
from django.views.decorators.http import require_http_methods
from .feast_store import get_user_features
from .models import UserStats, Order

@require_http_methods(["GET"])
def get_features(request, user_id):
    """Get features for a specific user from the feature store."""
    try:
        features = get_user_features([user_id])
        return JsonResponse({
            "user_id": user_id,
            "features": features
        })
    except Exception as e:
        return JsonResponse({
            "error": str(e)
        }, status=400)

@require_http_methods(["GET"])
def get_stats(request, user_id):
    """Get raw statistics for a user from the database."""
    try:
        stats = UserStats.objects.get(user_id=user_id)
        return JsonResponse({
            "user_id": stats.user_id,
            "total_orders": stats.total_orders,
            "average_order_value": stats.average_order_value,
            "last_order_date": stats.last_order_date.isoformat() if stats.last_order_date else None,
            "is_active": stats.is_active,
        })
    except UserStats.DoesNotExist:
        return JsonResponse({
            "error": "User stats not found"
        }, status=404)

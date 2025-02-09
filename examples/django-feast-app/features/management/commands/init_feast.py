from django.core.management.base import BaseCommand
from features.feast_store import get_feature_store

class Command(BaseCommand):
    help = 'Initialize Feast feature store and apply feature views'

    def handle(self, *args, **options):
        try:
            # Initialize feature store
            store = get_feature_store()
            self.stdout.write(self.style.SUCCESS('Successfully initialized Feast feature store'))
        except Exception as e:
            self.stdout.write(self.style.ERROR(f'Failed to initialize Feast feature store: {str(e)}'))

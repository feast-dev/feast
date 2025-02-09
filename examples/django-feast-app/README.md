# Django Feast Application Example

This example demonstrates how to use Feast as a package in a Django application.

## Setup

1. Set required environment variables:
```bash
# Required environment variables
export DJANGO_SECRET_KEY=  # Generate a secure key for your deployment
export DJANGO_DEBUG=False  # Set to True only in development
export DJANGO_ALLOWED_HOSTS=localhost,127.0.0.1  # Comma-separated list of allowed hosts
```

2. Install requirements:
```bash
pip install -r requirements.txt
```

3. Run migrations:
```bash
python manage.py migrate
```

4. Initialize Feast:
```bash
python manage.py init_feast
```

## API Endpoints

- `/api/features/<user_id>/` - Get user features from Feast
- `/api/stats/<user_id>/` - Get raw statistics from Django

## Security Notes

- Use environment variables for all sensitive configuration
- Configure proper authentication and authorization for production use
- Review Django's deployment checklist before going to production

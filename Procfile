web: gunicorn -k uvicorn.workers.UvicornWorker main:app --timeout 180 --preload
worker: celery -A main.celery worker --loglevel=info -Q tasks

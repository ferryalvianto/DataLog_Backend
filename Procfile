web: gunicorn -k uvicorn.workers.UvicornWorker main:app --timeout 180 --preload
worker: 
    command: celery -A tasks worker
    environment:
      - broker_url=redis://redis:6379/
      - result_backend=redis://redis:6379/
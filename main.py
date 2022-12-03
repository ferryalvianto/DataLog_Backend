from fastapi import FastAPI

from config.celery_utils import create_celery
from routers import datalog
import time
import uvicorn as uvicorn
# an HTTP-specific exception class  to generate exception information
from fastapi.middleware.cors import CORSMiddleware

def create_app() -> FastAPI:
    current_app = FastAPI(title="DataLog Webapp")
    current_app.celery_app = create_celery()
    current_app.include_router(datalog.router)
    return current_app


app = create_app()
celery = app.celery_app

origins = [
    "http://localhost:3000",
    'https://datalogwebapp.netlify.app'
    ]

# what is a middleware?
# software that acts as a bridge between an operating system or database and applications, especially on a network.
app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.middleware("http")
async def add_process_time_header(request, call_next):
    print('inside middleware!')
    start_time = time.time()
    response = await call_next(request)
    process_time = time.time() - start_time
    response.headers["X-Process-Time"] = str(f'{process_time:0.4f} sec')
    return response

if __name__ == "__main__":
    uvicorn.run("main:app", reload=True)

    



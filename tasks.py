from celery import Celery
import pandas as pd

from models.timeseries import save_timeseries_to_db
from models.ml_model_regression import save_model_to_db

app = Celery("tasks", broker='redis://redis:6379/0')

def read_oa_csv():
    oa1 = pd.read_pickle('https://drive.google.com/uc?id=1l7qRdYza9LF80UZrKDJ7ucmdNYMBPvbB', compression='gzip')
    oa2= pd.read_pickle('https://drive.google.com/uc?id=1jNfSca67sEvvkLkZkUEtgRxcwHEmbjTC', compression='gzip')
    oa3 = pd.read_pickle('https://drive.google.com/uc?id=1Qfm7iv_-msBqE8xs_gt-2UBwWndheOxi', compression='gzip')
    oa4 = pd.read_pickle('https://drive.google.com/uc?id=1BGT8fjo-bD4trGOGBfZk7S3DCtp9z-sb', compression='gzip')
    oa = pd.concat([oa1, oa2], ignore_index=True, axis=0)
    oa = pd.concat([oa, oa3], ignore_index=True, axis=0)
    oa = pd.concat([oa, oa4], ignore_index=True, axis=0)
    return oa

def read_cy_csv():
    cy1 = pd.read_pickle('https://drive.google.com/uc?id=1Ujp7hbfnP5nz0Ceo8cjsfxFPIWAaAe2Q', compression='gzip')
    cy2 = pd.read_pickle('https://drive.google.com/uc?id=17XfesauEIBESFvGdOgQg4neWxHTVRJYP', compression='gzip')
    cy3 = pd.read_pickle('https://drive.google.com/uc?id=1ln9nPdZAI0d8dj2qa_w0Lc0Yma1HzqlS', compression='gzip')
    cy4 = pd.read_pickle('https://drive.google.com/uc?id=1uv_Pt2bOYTMfoUWrnFfUErjJY7NIVy7S', compression='gzip')
    cy = pd.concat([cy1, cy2], ignore_index=True, axis=0)
    cy = pd.concat([cy, cy3], ignore_index=True, axis=0)
    cy = pd.concat([cy, cy4], ignore_index=True, axis=0)
    return cy

@app.task (name='tasks.add')
def add(x, y):
    return x + y

@app.task (name='tasks.train_models_task')
def train_models_task(db:str, yyyy:str, mm:str, dd:str):
    responses = []
    cy = read_cy_csv()
    oa = read_oa_csv()
    timeseries = save_timeseries_to_db(db, cy, oa, yyyy, mm, dd)
    responses.append(timeseries)
    linreg = save_model_to_db(db, yyyy, mm, dd)
    responses.append('Linear Regression has been updated')
    return responses

from celery import shared_task
import pandas as pd
import pymongo
from models.timeseries import save_timeseries_to_db
from models.ml_model_regression import save_model_to_db
from typing import List

# @shared_task(bind=True,autoretry_for=(Exception,), retry_backoff=True, retry_kwargs={"max_retries": 5},
#              name='universities:get_all_universities_task')
# def get_all_universities_task(self, countries: List[str]):
#     data: dict = {}
#     for cnt in countries:
#         data.update(universities.get_all_universities_for_country(cnt))
#     return data


# @shared_task(bind=True,autoretry_for=(Exception,), retry_backoff=True, retry_kwargs={"max_retries": 5},
#              name='university:get_university_task')
# def get_university_task(self, country: str):
#     university = universities.get_all_universities_for_country(country)
#     return university

def read_oa_csv():
    oa1 = pd.read_pickle('../pkls/OA19.pkl', compression='gzip')
    oa2= pd.read_pickle('../pkls/OA20.pkl', compression='gzip')
    oa3 = pd.read_pickle('../pkls/OA21.pkl', compression='gzip')
    oa4 = pd.read_pickle('../pkls/OA22.pkl', compression='gzip')
    oa = pd.concat([oa1, oa2], ignore_index=True, axis=0)
    oa = pd.concat([oa, oa3], ignore_index=True, axis=0)
    oa = pd.concat([oa, oa4], ignore_index=True, axis=0)
    return oa

def read_cy_csv():
    cy1 = pd.read_pickle('../pkls/CY19.pkl', compression='gzip')
    cy2 = pd.read_pickle('../pkls/CY20.pkl', compression='gzip')
    cy3 = pd.read_pickle('../pkls/CY21.pkl', compression='gzip')
    cy4 = pd.read_pickle('../pkls/CY22.pkl', compression='gzip')
    cy = pd.concat([cy1, cy2], ignore_index=True, axis=0)
    cy = pd.concat([cy, cy3], ignore_index=True, axis=0)
    cy = pd.concat([cy, cy4], ignore_index=True, axis=0)
    return cy

@shared_task(name='tasks:add')
def add( x:int, y:int):
    return x + y

@shared_task(name='tasks:train_models_task')
def train_models_task( db:str, yyyy:str, mm:str, dd:str):
    responses = []
    cy = read_cy_csv()
    oa = read_oa_csv()
    timeseries = save_timeseries_to_db(db, cy, oa, yyyy, mm, dd)
    responses.append(timeseries)
    linreg = save_model_to_db(db, yyyy, mm, dd)
    responses.append('Linear Regression has been updated')
    client = pymongo.MongoClient('mongodb+srv://DataLog:DataLog@cluster0.jzr1zc7.mongodb.net/')
    client[db]['training_log'].update_one({'flag': 'training_log' }, { "$set": { 'isDone': True } })
    client[db]['training_log'].update_one({'flag': 'training_log' }, { "$set": { 'results': responses } })
    client[db]['training_log'].update_one({'flag': 'training_log' }, { "$set": { 'latest_date_in_model': yyyy+'-'+mm+'-'+dd } })
    return responses

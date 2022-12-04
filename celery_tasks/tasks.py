from celery import shared_task
import pandas as pd
import pymongo
from models.timeseries import save_timeseries_to_db
from models.ml_model_regression import save_model_to_db
from typing import List
from pathlib import Path

# def read_oa_csv():
#     path1 = Path(__file__).parent / "../pkls/OA19.pkl"
#     oa1 = pd.read_pickle(path1, compression='gzip')
#     path2 = Path(__file__).parent / "../pkls/OA20.pkl"
#     oa2= pd.read_pickle(path2, compression='gzip')
#     path3 = Path(__file__).parent / "../pkls/OA21.pkl"
#     oa3 = pd.read_pickle(path3, compression='gzip')
#     path4 = Path(__file__).parent / "../pkls/OA22.pkl"
#     oa4 = pd.read_pickle(path4, compression='gzip')
#     oa = pd.concat([oa1, oa2], ignore_index=True, axis=0)
#     oa = pd.concat([oa, oa3], ignore_index=True, axis=0)
#     oa = pd.concat([oa, oa4], ignore_index=True, axis=0)
#     return oa

# def read_cy_csv():
#     path1 = Path(__file__).parent / "../pkls/CY19.pkl"
#     cy1 = pd.read_pickle(path1, compression='gzip')
#     path2 = Path(__file__).parent / "../pkls/CY20.pkl"
#     cy2 = pd.read_pickle(path2, compression='gzip')
#     path3 = Path(__file__).parent / "../pkls/CY21.pkl"
#     cy3 = pd.read_pickle(path3, compression='gzip')
#     path4 = Path(__file__).parent / "../pkls/CY22.pkl"
#     cy4 = pd.read_pickle(path4, compression='gzip')
#     cy = pd.concat([cy1, cy2], ignore_index=True, axis=0)
#     cy = pd.concat([cy, cy3], ignore_index=True, axis=0)
#     cy = pd.concat([cy, cy4], ignore_index=True, axis=0)
#     return cy

@shared_task(name='tasks:add')
def add( x:int, y:int):
    return x + y

@shared_task(name='tasks:train_models_task')
def train_models_task( db:str, yyyy:str, mm:str, dd:str):
    responses = []
    # cy = read_cy_csv()
    # oa = read_oa_csv()
    timeseries = save_timeseries_to_db(db, yyyy, mm, dd)
    responses.append(timeseries)
    linreg = save_model_to_db(db, yyyy, mm, dd)
    responses.append('Linear Regression has been updated')
    client = pymongo.MongoClient('mongodb+srv://DataLog:DataLog@cluster0.jzr1zc7.mongodb.net/')
    client[db]['training_log'].update_one({'flag': 'training_log' }, { "$set": { 'isDone': True } })
    client[db]['training_log'].update_one({'flag': 'training_log' }, { "$set": { 'results': responses } })
    client[db]['training_log'].update_one({'flag': 'training_log' }, { "$set": { 'latest_date_in_model': yyyy+'-'+mm+'-'+dd } })
    return responses

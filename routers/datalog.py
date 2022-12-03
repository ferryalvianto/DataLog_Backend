from fastapi import APIRouter
from starlette.responses import JSONResponse
from fastapi import Depends, HTTPException
from celery_tasks.tasks import train_models_task, add, read_oa_csv, read_cy_csv
from config.celery_utils import get_task_info

from fastapi.security import OAuth2PasswordRequestForm
from datetime import timedelta,datetime
from fastapi.encoders import jsonable_encoder

from models.clean_csv import cleancsv
from models.model import Sentiments, User, UserInDB, Token
from models.ml_model_regression import load_saved_model_from_db, load_saved_model_from_db_with_category
from services.authentication import get_db_names, create_access_token, get_current_active_user, get_access_token,update_user_db
from services.api_weather import get_weather

from dbs.db_forecast_revenue import fetch_latest_forecast_revenues
from dbs.db_revenue import fetch_by_range_revenue, fetch_all_revenue
from dbs.db_sentiments import create_sentiments,fetch_by_range_sentiments,fetch_all_sentiments
from dbs.db_wastage import fetch_all_wastage,fetch_date_range_wastage
from dbs.db_generalproducts import fetch_general_products, fetch_products_by_date

import pandas as pd
import pymongo
from dateutil.relativedelta import relativedelta
from datetime import datetime, timedelta
from passlib.context import CryptContext

router = APIRouter(prefix='/datalog', tags=['datalog'], responses={404: {"description": "Not found"}})

@router.get('/hi')
def test():
    df = read_oa_csv()
    df = df.head()
    df = df.to_dict(orient='records')
    return df

# APIs

#-------------------------------------------#
# authentication

@router.get('/')
async def test():
    task = add.delay(1,2)
    return JSONResponse({"task_id": task.id})

@router.get("/task/{task_id}")
async def get_task_status(task_id: str) -> dict:
    """
    Return the status of the submitted Task
    """
    return get_task_info(task_id)


@router.get('/api/get-dbs')
def get_dbs():
    response = get_db_names()
    if response:
        return response
    raise HTTPException(400, f"Something went wrong")


@router.post('/token', response_model=Token)
async def login_for_access_token(form_data: OAuth2PasswordRequestForm = Depends()):
    client = pymongo.MongoClient('mongodb+srv://DataLog:DataLog@cluster0.jzr1zc7.mongodb.net/')
    db = form_data.client_id
    mydb = client[db]
    mycol = mydb["users"]
    cursor = mycol.find({}, {'_id': 0})
    users = list(cursor)
    
    user_found = next((u for u in users if u['username'] == form_data.username), {})

    if form_data.username in user_found.get('username'):
        user_dict = user_found
        user = UserInDB(**user_dict)
    else:
       raise HTTPException(status_code=400,detail="Incorrect username") 

    pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")
    if pwd_context.verify(form_data.password, user.password):
        user
    else:
        raise HTTPException(status_code=400,detail="Incorrect password")
    expiry = get_access_token()
    access_token_expires = timedelta(minutes=expiry)
    access_token = create_access_token(
        data={'username': user.username, 'db':user.db}, expires_delta=access_token_expires
    )
    return {"access_token": access_token, "token_type": "bearer", "user":user_dict}

@router.get("/api/users/me/", response_model=User)
async def read_users_me(current_user: User = Depends(get_current_active_user)):
    return current_user

@router.put("/api/users/{username}", response_model=User)
async def update_user(username: str, db:str, user: User):
    client = pymongo.MongoClient('mongodb+srv://DataLog:DataLog@cluster0.jzr1zc7.mongodb.net/')
    mydb = client[db]
    mycol = mydb["users"]
    cursor = mycol.find({}, {'_id': 0})
    users = list(cursor)

    updated_user = jsonable_encoder(user)
    user_found = next((u for u in users if u['username'] == username), {})
    user_found = updated_user

    updated_user_db = await update_user_db(username, db, updated_user)
    return user_found

#-------------------------------------------#
# getting wastages

# all wastage


@router.get("/api/wastage")
async def get_wastage():
    response = await fetch_all_wastage()
    return response

# wastage by range


@router.get("/api/wastage/")
async def get_by_range_wastage(start_date: str, end_date: str):
    response = await fetch_date_range_wastage(start_date, end_date)
    if response:
        return response
    raise HTTPException(
        404, f"There is no wastage from {start_date} and {end_date}")

#-------------------------------------------#
# getting sentiments by count
# getting sentiments


@router.get("/api/sentiments")
async def get_sentiment():
    response = await fetch_all_sentiments()
    return response


@router.get("/api/sentiments/")
async def get_sentiment_by_range(start_date: str, end_date: str):
    response = await fetch_by_range_sentiments(start_date, end_date)
    if response:
        return response
    raise HTTPException(
        404, f"There is no sentiments from {start_date} and {end_date}")

#inserting sentiments


@router.post("/api/insert_sentiments/", response_model=Sentiments)
async def post_todo(sentiments: Sentiments):
    response = await create_sentiments(sentiments.dict())
    if response:
        return response
    raise HTTPException(400, "Something went wrong")


#-------------------------------------------#
# revenue


@router.get("/api/revenues")
def get_revenues(db:str):
    cy = read_cy_csv()
    oa = read_oa_csv()
    response = fetch_all_revenue(db, cy, oa)
    return response


@router.get("/api/revenues/")
async def get_revenue_by_range(db:str, start_date: str, end_date: str):
    response = await fetch_by_range_revenue(db, start_date, end_date)
    if response:
        return response
    raise HTTPException(
        404, f"There is no revenues from {start_date} and {end_date}")


@router.get("/api/quantity_forecast")
async def put_model(db:str):
    response = load_saved_model_from_db(db, get_weather())
    if response:
        return response
    raise HTTPException(400, f"Something went wrong")

@router.get("/api/quantity_forecast/")
async def put_model_cat(db:str, category: str):
    response = load_saved_model_from_db_with_category(db, get_weather(),category)
    if response:
        return response
    raise HTTPException(400, f"Something went wrong")


@router.get("/api/revenue_forecast")
async def get_revenue_forecast(db:str):
    response = await fetch_latest_forecast_revenues(db)
    return response

#-------------------------------------------#
# weather api

@router.get("/api/forecasted_weather")
def get_weather_forecast():
    response = get_weather()
    return response

#-------------------------------------------#
# model api

# @router.get("/api/model_regression")
# async def put_model(db:str, yyyy:str, mm:str, dd:str):
#     response = save_model_to_db(db, yyyy, mm, dd)
#     if response:
#         return response
#     raise HTTPException(400, f"Something went wrong")

@router.get("/api/model_regression_result")
async def put_model(db:str):
    response = load_saved_model_from_db(db, get_weather())
    if response:
        return response
    raise HTTPException(400, f"Something went wrong")

#get forecast by category  
@router.get("/api/model_regression_result/")
async def put_model_cat(db:str, category: str):
    response = load_saved_model_from_db_with_category(db, get_weather(),category)
    if response:
        return response
    raise HTTPException(400, f"Something went wrong")

@router.get("/api/clean_csv")
async def clean_csv(db:str, id_inventory:str, id_payment:str, year:str, month:str, day:str):
    if db == 'BeFresh':
        return cleancsv(db, id_inventory, id_payment, year, month, day)
    elif db == 'Datalog':
        return 'Not available on the DataLog database'
    raise HTTPException(400, f"Something went wrong")


# @router.get("/api/timeseries")
# async def timeseries_model(db:str, yyyy:str, mm:str, dd:str):
#     cy = await read_cy_csv()
#     oa = await read_oa_csv()
#     response = save_timeseries_to_db(db, cy, oa, yyyy, mm, dd)
#     if response:
#         return response
#     raise HTTPException(400, f"Something went wrong")

@router.get("/api/upload_date_log")
async def upload_date_log(db:str, yyyy:str, mm:str, dd:str):
    client = pymongo.MongoClient('mongodb+srv://DataLog:DataLog@cluster0.jzr1zc7.mongodb.net/')
    mydb = client[db]
    col = mydb['df_sales']
    results = col.find({"Date": yyyy+'-'+mm+'-'+dd})
    for res in results:
        date = res['Date']
        establishment = res['Establishment']
        if yyyy+'-'+mm+'-'+dd in date:
            return 'True'
        else:
            return 'False'

@router.get("/api/upload_log")
async def upload_log(db:str, yyyy:str, mm:str, dd:str):
    client = pymongo.MongoClient('mongodb+srv://DataLog:DataLog@cluster0.jzr1zc7.mongodb.net/')
    mydb = client[db]
    col = mydb['df_sales']
    inptstr = yyyy+'-'+mm+'-'+dd
    results = col.find({"Date": inptstr, '$or':[{"Establishment":0}, {"Establishment":1}]}, {"_id": 0,"Date":1, "Establishment":1})
    results = pd.DataFrame(results)

    yesterday = (datetime.now() - timedelta(1)).strftime('%Y-%m-%d')
    inpt = datetime.strptime(inptstr, '%Y-%m-%d').date()
    dates = []
    finds = client[db]['df_sales'].find({},{"_id": 0,"Date":1})
    finds = pd.DataFrame(finds)
    latestdatestr = finds['Date'].values[-1]
    latestdate = datetime.strptime(latestdatestr, '%Y-%m-%d').date()
    diff = inpt - latestdate
    diff = diff.days

    trained = client[db]['timeseries_models'].find({},{"_id": 0,"latest_date_in_model":1})
    trained = pd.DataFrame(trained)

    if 'latest_date_in_model' in trained.columns:
        latestdatemodel = trained['latest_date_in_model'].values[-1]
        latestdatemodel = datetime.strptime(latestdatemodel, '%Y-%m-%d').date()
        if latestdatemodel == inpt:
            return 'Files and models have been uploaded and trained on the selected date.'
    
    if (latestdate + timedelta(days=1)) == inpt or latestdate == inpt:
        if 'Establishment' and 'Date' in results.columns:
            establishments = results['Establishment'].unique()
            dates = results['Date'].unique()
            if inptstr in dates:
                if '0' not in str(establishments):
                    return '1'
                if '1' not in str(establishments):
                    return '0'
                if '0' and '1' in str(establishments):
                    return 'Train models now.'
            if inptstr not in dates:
                return '2'
        else:
            res = col.find({"Date": latestdatestr, '$or':[{"Establishment":0}, {"Establishment":1}]}, {"_id": 0,"Date":1, "Establishment":1})
            res = pd.DataFrame(res)
            establishments = res['Establishment'].unique()
            if((latestdate + timedelta(days=1)) == inpt) and ('0' and '1' not in str(establishments)):
                day_count = 0
                while day_count < diff:
                    nextdate = latestdate + relativedelta(days=day_count)
                    dates.append(str(nextdate))
                    day_count += 1
                return 'Please upload all files from:', dates
            else:
                return 'No files for selected date has been uploaded.'
    elif latestdate > inpt:
        return 'Files for this date have been uploaded.'
    elif latestdate < inpt:
        day_count = 1
        while day_count < diff:
            day = latestdate + relativedelta(days=day_count)
            dates.append(str(day))
            day_count += 1
        return 'Please upload CSV files for these dates:', dates
    

@router.post("/api/train_models")
def train_models(db:str, yyyy:str, mm:str, dd:str):
    task = train_models_task.delay(db, yyyy, mm, dd)
    return JSONResponse({"task_id": task.id})

@router.get('/api/get_training')
def get_training_status(db:str):
    client = pymongo.MongoClient('mongodb+srv://DataLog:DataLog@cluster0.jzr1zc7.mongodb.net/')
    collection = client[db]['training_log']
    for x in collection.find({ "flag": "training_log" },{ "_id": 0 }):
        if x['isDone'] == True:
            response = x['results']
            return '1', response
        else:
            return '0'

@router.post('/api/update_training_log')
def update_training_log(db:str):
    client = pymongo.MongoClient('mongodb+srv://DataLog:DataLog@cluster0.jzr1zc7.mongodb.net/')
    collection = client[db]['training_log']
    collection.update_one({'flag': 'training_log' }, { "$set": { 'isDone': False } })
    return '1'

#-------------------------------------------#
# General Products
@router.get("/api/general_products")
async def fetch_products(db:str):
    response = await fetch_general_products(db)
    if response:
        return response
    raise HTTPException(
        404, f"There are not products to show")

@router.get("/api/general_products_by_date")
async def fetch_products_filtered(db:str, start_date: str, end_date: str):
    response = await fetch_products_by_date(db, start_date, end_date)
    if response:
        return response
    raise HTTPException(
        404, f"There are not products to show")
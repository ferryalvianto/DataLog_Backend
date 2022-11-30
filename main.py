from fastapi import Depends, FastAPI, HTTPException
from fastapi.security import OAuth2PasswordRequestForm
from datetime import timedelta,datetime
from fastapi.encoders import jsonable_encoder

from models.timeseries import save_timeseries_to_db
from models.clean_csv import cleancsv
from models.model import Sentiments, User, UserInDB, Token
from models.ml_model_regression import save_model_to_db, load_saved_model_from_db, load_saved_model_from_db_with_category
from authentication import get_db_names, create_access_token, get_current_active_user, get_access_token,update_user_db, client, pwd_context
from api_weather import get_weather

from dbs.db_forecast_revenue import fetch_latest_forecast_revenues
from dbs.db_revenue import fetch_by_range_revenue, fetch_all_revenue
from dbs.db_sentiments import create_sentiments,fetch_by_range_sentiments,fetch_all_sentiments
from dbs.db_wastage import fetch_all_wastage,fetch_date_range_wastage
from dbs.db_heatmap import fetch_all_ht_category, fetch_date_range_ht_category

import pandas as pd
import dask
import dask.dataframe as dd
import pymongo
from dateutil.relativedelta import relativedelta
from datetime import datetime, timedelta

import pandas as pd
import dask
import dask.dataframe as dd
import pymongo
from dateutil.relativedelta import relativedelta
from datetime import datetime, timedelta

# an HTTP-specific exception class  to generate exception information
from fastapi.middleware.cors import CORSMiddleware
app = FastAPI()

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

client = pymongo.MongoClient('mongodb+srv://DataLog:DataLog@cluster0.jzr1zc7.mongodb.net/')

# APIs

#-------------------------------------------#
# authentication

@app.get('/')
async def test():
    return {"hello":"world"}


@app.get('/api/get-dbs')
async def get_dbs():
    response = get_db_names()
    if response:
        return response
    raise HTTPException(400, f"Something went wrong")


@app.post('/token', response_model=Token)
async def login_for_access_token(form_data: OAuth2PasswordRequestForm = Depends()):
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

@app.get("/api/users/me/", response_model=User)
async def read_users_me(current_user: User = Depends(get_current_active_user)):
    return current_user

@app.put("/api/users/{username}", response_model=User)
async def update_user(username: str, db:str, user: User):
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


@app.get("/api/wastage")
async def get_wastage(db:str):
    response = await fetch_all_wastage(db)
    return response

# wastage by range


@app.get("/api/wastage/")
async def get_by_range_wastage(db,start_date: str, end_date: str):
    response = await fetch_date_range_wastage(db,start_date, end_date)
    if response:
        return response
    raise HTTPException(
        404, f"There is no wastage from {start_date} and {end_date}")

#-------------------------------------------#
# getting sentiments by count
# getting sentiments


@app.get("/api/sentiments")
async def get_sentiment(db:str):
    response = await fetch_all_sentiments(db)
    return response


@app.get("/api/sentiments/")
async def get_sentiment_by_range(db:str,start_date: str, end_date: str):
    response = await fetch_by_range_sentiments(db,start_date, end_date)
    if response:
        return response
    raise HTTPException(
        404, f"There is no sentiments from {start_date} and {end_date}")

#inserting sentiments
@app.post("/api/insert_sentiments/", response_model=Sentiments)
async def post_todo(sentiments: Sentiments):
    response = await create_sentiments(sentiments.dict())
    if response:
        return response
    raise HTTPException(400, "Something went wrong")


#-------------------------------------------#
# revenue


@app.get("/api/revenues")
async def get_revenues(db:str):
    response = await fetch_all_revenue(db)
    return response


@app.get("/api/revenues/")
async def get_revenue_by_range(db:str, start_date: str, end_date: str):
    response = await fetch_by_range_revenue(db, start_date, end_date)
    if response:
        return response
    raise HTTPException(
        404, f"There is no revenues from {start_date} and {end_date}")


#-------------------------------------------#
# quantity forecast


@app.get("/api/quantity_forecast")
async def put_model(db:str):
    response = load_saved_model_from_db(db, get_weather())
    if response:
        return response
    raise HTTPException(400, f"Something went wrong")

@app.get("/api/quantity_forecast/")
async def put_model_cat(db:str, category: str):
    response = load_saved_model_from_db_with_category(db, get_weather(),category)
    if response:
        return response
    raise HTTPException(400, f"Something went wrong")

#-------------------------------------------#


@app.get("/api/revenue_forecast")
async def get_revenue_forecast(db:str):
    response = await fetch_latest_forecast_revenues(db)
    return response

#-------------------------------------------#
# weather api

@app.get("/api/forecasted_weather")
def get_weather_forecast():
    response = get_weather()
    return response

#-------------------------------------------#
# model api

async def read_oa_csv():
    oa_urls = ['https://drive.google.com/uc?id=1pdMJvWVmV8KLwpIkfBmD0yLZd1Zq9Hou',
            'https://drive.google.com/uc?id=1sd7UEHk8fhHk52W61aupnnxRBnP2cTxg',
            'https://drive.google.com/uc?id=11XGlbZknAqXxhB3D-ZDGC7kBWIYG-wLv',
            'https://drive.google.com/uc?id=1XBsq2mwjpzppivuODh8Z8imAVmLoYgjx']
    oa_dfs = [dask.delayed(pd.read_csv)(url) for url in oa_urls]
    oa = dd.from_delayed(oa_dfs)
    return oa

async def read_cy_csv():
    cy_urls= ['https://drive.google.com/uc?id=1loqBDfWZQ96Z3-KoJSSzNb0mngGo-YQf',
            'https://drive.google.com/uc?id=1FMg0L0ia5pRqQzctqVQDJFxFQyXTPf6y',
            'https://drive.google.com/uc?id=1o0cvR0ZrhyA3Ekeoy9HF2J8IRamqeRb-',
            'https://drive.google.com/uc?id=1_sPA-bnAWnkAvNUGljJoTxXPaNZiLI_B']
    cy_dfs = [dask.delayed(pd.read_csv)(url) for url in cy_urls]
    cy = dd.from_delayed(cy_dfs)
    return cy

@app.get("/api/model_regression")
async def put_model(db:str, yyyy:str, mm:str, dd:str):
    response = save_model_to_db(db, yyyy, mm, dd)
    if response:
        return response
    raise HTTPException(400, f"Something went wrong")

@app.get("/api/model_regression_result")
async def put_model(db:str):
    response = load_saved_model_from_db(db, get_weather())
    if response:
        return response
    raise HTTPException(400, f"Something went wrong")

#get forecast by category  
@app.get("/api/model_regression_result/")
async def put_model_cat(db:str, category: str):
    response = load_saved_model_from_db_with_category(db, get_weather(),category)
    if response:
        return response
    raise HTTPException(400, f"Something went wrong")

@app.get("/api/clean_csv")
async def clean_csv(db:str, id_inventory:str, id_payment:str, year:str, month:str, day:str):
    if db == 'BeFresh':
        return cleancsv(db, id_inventory, id_payment, year, month, day)
    elif db == 'Datalog':
        return 'Not available on the DataLog database'
    raise HTTPException(400, f"Something went wrong")


@app.get("/api/timeseries")
async def timeseries_model(db:str, yyyy:str, mm:str, dd:str):
    cy = await read_cy_csv()
    oa = await read_oa_csv()
    response = save_timeseries_to_db(db, cy, oa, yyyy, mm, dd)
    if response:
        return response
    raise HTTPException(400, f"Something went wrong")

    #get heatmap location and quantity
@app.get("/api/heatmap")
async def get_heatmap(db:str):
    response = await fetch_all_ht_category()
    return response

#get heatmap location and quantity with date range filter
@app.get("/api/heatmap/")
async def get_by_range_heatmap(db:str,start_date: str, end_date: str):
    response = await fetch_date_range_ht_category(db,start_date, end_date)
    if response:
        return response
    raise HTTPException(
        404, f"There is no records from {start_date} and {end_date}")
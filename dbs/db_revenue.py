import motor.motor_asyncio
from models.model import Revenue
import pymongo
import pandas as pd

#fetch all revenues
def fetch_all_revenue(db:str):
    client = pymongo.MongoClient('mongodb+srv://DataLog:DataLog@cluster0.jzr1zc7.mongodb.net/')
    database = client[db]
    collection = database.df_sales

    max_date = collection.find({'$and':[{"Establishment":0}, {"Establishment":1}]})
    
    for maxDate in max_date:
        maxDate

    max_month = int(maxDate["Date"][5:7])
    max_year = int(maxDate["Date"][0:4])

    cursor = collection.aggregate([
        {
            "$project":
                {
                    "year" :{"$year": { "$toDate": "$Date"}},
                    "month": {"$month": { "$toDate": "$Date"}},
                    "Date" : "$Date",
                    "revenue" : "$dailyRevenue",
                    'establishment': '$Establishment',
                    "_id":0
                }
        }
        ,
        {
            "$match" : { "month" :max_month, "year": max_year }
        },
        { '$sort': {'Date':1,  "establishment": 1 } },
    ])

    df = pd.DataFrame(cursor)
    df = df.drop_duplicates()
    df = df.to_dict(orient='records')
    
    return df
    
#detch revenue by range
def fetch_by_range_revenue(db, start_date,end_date):
    client = motor.motor_asyncio.AsyncIOMotorClient('mongodb+srv://DataLog:DataLog@cluster0.jzr1zc7.mongodb.net/')
    database = client[db]
    collection = database.Revenue

    revenues = []
    cursor = collection.find({'ymd': { "$gte": start_date, "$lte":  end_date}}) 
    for document in cursor:
        revenues.append(Revenue(**document))
    return revenues


   
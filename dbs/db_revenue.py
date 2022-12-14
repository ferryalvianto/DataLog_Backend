import pymongo
import pandas as pd
import motor.motor_asyncio
from models.model import Revenue

#detch revenue by range
async def fecth_by_range_revenue(db,start_date,end_date):
    client = motor.motor_asyncio.AsyncIOMotorClient('mongodb+srv://DataLog:DataLog@cluster0.jzr1zc7.mongodb.net/test')
    database = client[db]
    collection = database.revenue
    revenues = []
    cursor = collection.find({'Date': { "$gte": start_date, "$lte":  end_date}}) 
    async for document in cursor:
        revenues.append(Revenue(**document))
    return revenues

def fetch_revenue_in_db(db:str):
    client = pymongo.MongoClient('mongodb+srv://DataLog:DataLog@cluster0.jzr1zc7.mongodb.net/')
    database = client[db]
    collection = database.revenue
    cursor = collection.aggregate([
        {
            "$project":
                {
                    "Date" : "$Date",
                    "dailyRevenue" : "$dailyRevenue",
                    'Establishment': '$Establishment',
                    "_id":0
                }
        }
        ,
        { '$sort': {'Date':1,  "Establishment": 1 } },
    ])
    df = pd.DataFrame(cursor)
    df = df.drop_duplicates()
    df = df[-14:]
    return df
   
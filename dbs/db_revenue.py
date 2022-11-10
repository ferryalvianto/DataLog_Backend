from sqlite3 import Cursor
from typing import Collection
import motor.motor_asyncio
from models.model import Revenue
from models.model import RevenueMaxDate

client = motor.motor_asyncio.AsyncIOMotorClient('mongodb+srv://DataLog:DataLog@cluster0.jzr1zc7.mongodb.net/test')
database = client.DataLog
collection = database.Revenue


#fetch all revenues
async def fetch_all_revenue():
    revenues = []
    # cursor = collection.find({})
    max_date = collection.find().sort([("ymd",-1)]).limit(1)

    async for maxDate in max_date:
        maxDate


    max_month = int(maxDate["ymd"][5:7])
    max_year = int(maxDate["ymd"][0:4])
   
    cursor = collection.aggregate([
        {
            "$project":
                {
                    "year" :{"$year": { "$toDate": "$ymd"}},
                    "month": {"$month": { "$toDate": "$ymd"}},
                    "ymd" : "$ymd",
                    "revenue" : "$dailyRevenue"
                }
        }
        ,
        {
            "$match" : { "month" :max_month, "year": max_year }
        }

    ])


    async for document in cursor:
        revenues.append(RevenueMaxDate(**document))
    return revenues

    


#detch revenue by range
async def fecth_by_range_revenue(start_date,end_date):
    revenues = []
    cursor = collection.find({'ymd': { "$gte": start_date, "$lte":  end_date}}) 
    async for document in cursor:
        revenues.append(Revenue(**document))
    return revenues


   
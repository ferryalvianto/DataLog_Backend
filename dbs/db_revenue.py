import motor.motor_asyncio
from models.model import Revenue
from models.model import RevenueMaxDate

client = motor.motor_asyncio.AsyncIOMotorClient('mongodb+srv://DataLog:DataLog@cluster0.jzr1zc7.mongodb.net/')

#fetch all revenues
async def fetch_all_revenue(db:str):
    database = client[db]
    collection = database.df_sales

    revenues = []
    # cursor = collection.find({})
    max_date = collection.find().sort([("Date",-1)]).limit(1)

    async for maxDate in max_date:
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
async def fetch_by_range_revenue(db, start_date,end_date):
    database = client[db]
    collection = database.Revenue

    revenues = []
    cursor = collection.find({'ymd': { "$gte": start_date, "$lte":  end_date}}) 
    async for document in cursor:
        revenues.append(Revenue(**document))
    return revenues


   
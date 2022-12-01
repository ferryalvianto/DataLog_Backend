import motor.motor_asyncio

client = motor.motor_asyncio.AsyncIOMotorClient('mongodb+srv://DataLog:DataLog@cluster0.jzr1zc7.mongodb.net')

#fetch all revenues
async def fetch_all_revenue(db):
    revenues = []
    mydb = client[db]
    collection =mydb['wastage']


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
        {'$group': {"_id" : "$Date", "Revenue": {"$first": "$revenue"},  "year": {"$first": "$year"}, "month": {"$first": "$month"} }}
        ,
        {
            "$match" : { "month" :max_month, "year": max_year }
        },
        {
            "$project":
                {
                    "Date" : "$id",
                    "revenue" : "$Revenue"
                }
        }

    ])


    async for document in cursor:
        revenues.append(document)
    return revenues


#detch revenue by range
async def fetch_by_range_revenue(db, start_date,end_date):
    revenues = []
    mydb = client[db]
    collection =mydb['df_sales']
    revenues = []
    # cursor = collection.find({'Date': { "$gte": start_date, "$lte":  end_date}}) 
    cursor = collection.aggregate([
         {'$group': {"_id" : "$Date", "Revenue": {"$first": "$dailyRevenue"} }}
        ,
        {
            "$match" : {'_id': { "$gte": start_date, "$lte":  end_date}
        }
        }

    ])


    async for document in cursor:
         revenues.append(document)
    return revenues


   
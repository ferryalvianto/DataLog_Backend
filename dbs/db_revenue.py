import motor.motor_asyncio
import pymongo
import pandas as pd

#fetch all revenues
def fetch_all_revenue(db:str, cy ,oa):
    client = pymongo.MongoClient('mongodb+srv://DataLog:DataLog@cluster0.jzr1zc7.mongodb.net/')
    database = client[db]
    collection = database.df_sales
    
    for max in collection.find().sort([("ymd",-1)]).limit(1):
        maxDate = max

    max_month = int(maxDate["Date"][5:7])
    max_year = int(maxDate["Date"][0:4])

    cursor = collection.aggregate([
        {
            "$project":
                {
                    "year" :{"$year": { "$toDate": "$Date"}},
                    "month": {"$month": { "$toDate": "$Date"}},
                    "Date" : "$Date",
                    "dailyRevenue" : "$dailyRevenue",
                    'Establishment': '$Establishment',
                    "_id":0
                }
        }
        ,
        {
            "$match" : { "month" :max_month, "year": max_year }
        },
        { '$sort': {'Date':1,  "Establishment": 1 } },
    ])

    df = pd.DataFrame(cursor)
    df = df.drop_duplicates()
    
    df_all = pd.concat([cy, oa], ignore_index=True, axis=0)
    df_all = df_all[['year', 'month', 'Date', 'dailyRevenue', 'Establishment']].copy()
    df_all = df_all.drop_duplicates()
    df_all = pd.concat([df_all, df], ignore_index=True, axis=0)
    df_all = df_all.sort_values(['Date', 'Establishment'], ascending=[True, True])
    df_all = df_all.to_dict(orient='records')
    
    return df_all
    
#detch revenue by range
def fetch_by_range_revenue(db, cy, oa, start_date,end_date):
    df = fetch_all_revenue(db, cy, oa)
    df = pd.DataFrame.from_dict(df)
    df = df.loc[(df['Date'] >= start_date) & (df['Date'] <= end_date)]
    df = df.to_dict(orient='records')
    return df


   
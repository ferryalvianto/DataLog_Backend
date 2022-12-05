import motor.motor_asyncio
from datetime import datetime

#fetch all sentiments
async def fetch_all_sentiments():
    client = motor.motor_asyncio.AsyncIOMotorClient('mongodb+srv://DataLog:DataLog@cluster0.jzr1zc7.mongodb.net/test')
    database = client.DataLog
    collection = database.Sentiments_Analysis
    sentiments = []
    mydb = client[db]
    collection = mydb['Sentiments_Analysis']
    cursor = collection.aggregate([
        {'$group': {"_id": "$Classification", "Total_Count": {"$sum": 1}}}
    ])
    async for document in cursor:
        sentiments.append(document)
    return sentiments

# count for each sentiments


async def fetch_by_range_sentiments(db, start_date, end_date):
    sentiments = []
    mydb = client[db]
    collection = mydb['Sentiments_Analysis']
    cursor = collection.aggregate([{'$match': {'Date': {"$gte": start_date, "$lte":  end_date}}},
                                   {'$group': {"_id": "$Classification",
                                               "Total_Count": {"$sum": 1}}}
                                   ])
    async for document in cursor:
        sentiments.append(document)
    return sentiments

# insert sentiments


async def create_sentiments(db, rating, comment):
    sentiments = []
    mydb = client[db]
    collection = mydb['Sentiments_Analysis']

    info = collection.insert_one(
        {'rating': rating, 'comment': comment, 'created_time': datetime.today().strftime('%Y-%m-%d')})
    return rating

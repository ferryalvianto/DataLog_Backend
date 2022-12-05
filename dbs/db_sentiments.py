import motor.motor_asyncio
from datetime import datetime

#fetch all sentiments
async def fetch_all_sentiments(db):
    client = motor.motor_asyncio.AsyncIOMotorClient('mongodb+srv://DataLog:DataLog@cluster0.jzr1zc7.mongodb.net')
    database = client.DataLog
    collection = database.Sentiments_Analysis
    sentiments = []
    mydb = client[db]
    collection = mydb['Sentiments_Analysis']
    cursor = collection.aggregate([
        {'$group': {"_id": "$rating", "Total_Count": {"$sum": 1}}}
    ])
    async for document in cursor:
        sentiments.append(document)
    return sentiments

# count for each sentiments


async def fetch_by_range_sentiments(db, start_date, end_date):
    client = motor.motor_asyncio.AsyncIOMotorClient(
        'mongodb+srv://DataLog:DataLog@cluster0.jzr1zc7.mongodb.net')
    sentiments = []
    mydb = client[db]
    collection = mydb['Sentiments_Analysis']
    cursor = collection.aggregate([
        {'$match': {'created_time': {"$gte": start_date, "$lte":  end_date}}},
        {'$group': {"_id": "$rating",
                    "Total_Count": {"$sum": 1}}}
    ])


    async for document in cursor:
        sentiments.append(document)
    return sentiments

# insert sentiments


async def create_sentiments(db, rating, comment):

    client = motor.motor_asyncio.AsyncIOMotorClient('mongodb+srv://DataLog:DataLog@cluster0.jzr1zc7.mongodb.net')
    sentiments = []
    mydb = client[db]
    collection = mydb['Sentiments_Analysis']

    info = collection.insert_one(
        {'rating': rating, 'comment': comment, 'created_time': datetime.today().strftime('%Y-%m-%d')})
    return rating

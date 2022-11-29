
import motor.motor_asyncio

client = motor.motor_asyncio.AsyncIOMotorClient('mongodb+srv://DataLog:DataLog@cluster0.jzr1zc7.mongodb.net/test')
database = client.DataLog
collection = database.Sentiments_Analysis


#fetch all sentiments
async def fetch_all_sentiments():
    sentiments = []
    cursor = collection.aggregate([
                                    {'$group': {"_id" : "$Classification", "Total_Count": {"$sum": 1}   }}
                                ])
    async for document in cursor:
        sentiments.append(document)
    return sentiments

#count for each sentiments
async def fetch_by_range_sentiments(start_date,end_date):
    sentiments = []
    cursor = collection.aggregate([ {'$match': {'Date': { "$gte": start_date, "$lte":  end_date} }},
                                    {'$group': {"_id" : "$Classification", "Total_Count": {"$sum": 1}   }}
                                ])
    async for document in cursor:
        sentiments.append(document)
    return sentiments

#insert sentiments
async def create_sentiments(Sentiments):
    document = Sentiments
    result = await collection.insert_one(document)
    return document

   

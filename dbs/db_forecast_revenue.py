import motor.motor_asyncio

client = motor.motor_asyncio.AsyncIOMotorClient('mongodb+srv://DataLog:DataLog@cluster0.jzr1zc7.mongodb.net/test')

async def fetch_latest_forecast_revenues(db:str):
    database = client[db]
    collection = database.revenue_forecast
    revenues = []
    cursor = collection.find({}, {"_id": 0})

    async for document in cursor:
        revenues.append(document)
    return revenues
    

    

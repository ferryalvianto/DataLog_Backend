import motor.motor_asyncio


async def fetch_all_wastage(db):
    client = motor.motor_asyncio.AsyncIOMotorClient(
        'mongodb+srv://DataLog:DataLog@cluster0.jzr1zc7.mongodb.net/')
    database = client[db]
    collection = database.wastage
    wastages = []
    mydb = client[db]
    collection = mydb['wastage']
    cursor = collection.aggregate([
        {'$group': {"_id": "$Name", "Total_Quantity": {"$sum": "$Quantity"}}},
        {'$sort': {"Total_Quantity": -1}},
        {'$limit': 10}
    ])
    async for document in cursor:
        wastages.append(document)
    return wastages


async def fetch_date_range_wastage(db, start_date, end_date):
    client = motor.motor_asyncio.AsyncIOMotorClient(
        'mongodb+srv://DataLog:DataLog@cluster0.jzr1zc7.mongodb.net/')
    database = client[db]
    collection = database.wastage
    wastages = []
    mydb = client[db]
    collection = mydb['wastage']
    cursor = collection.aggregate([{'$match': {'Date': {"$gte": start_date, "$lte":  end_date}}},
                                   {'$group': {"_id": "$Name",
                                               "Total_Quantity": {"$sum": "$Quantity"}}},
                                   {'$sort': {"Total_Quantity": -1}},
                                   {'$limit': 10},
                                   ])
    async for document in cursor:
        wastages.append(document)
    return wastages

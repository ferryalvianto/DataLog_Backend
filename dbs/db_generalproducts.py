from sqlite3 import Cursor
from typing import Collection
import motor.motor_asyncio
from models.model import General_Products

client = motor.motor_asyncio.AsyncIOMotorClient('mongodb+srv://DataLog:DataLog@cluster0.jzr1zc7.mongodb.net/test')
database = client.DataLog
collection = database.Order_Item_Transaction

async def fetch_general_products(category, start_date, end_date):
    products = []


    cursor = collection.aggregate([
        {
            "$match" : 
            {
                "$and": [
                    {"Category" : category},
                    {"Date": {"$gte" : start_date, '$lte': end_date}}
                ]
            }
        },
        {
            "$group": 
            {
                "_id": {
                    "Product_Category" : "$Category",
                    "Product": "$Product_Name"}, 
                    "Quantity_Sold": {"$sum": "$Quantity"}
            }
        },
        {
            "$sort": {"Quantity_Sold": 1}
        },
        {
            "$limit": 10
        }
    ])
        
    async for document in cursor:
        products.append(document)
    return products
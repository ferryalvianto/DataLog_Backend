from sqlite3 import Cursor
from datetime import datetime
from typing import Collection
import motor.motor_asyncio
from models.model import General_Products

client = motor.motor_asyncio.AsyncIOMotorClient('mongodb+srv://DataLog:DataLog@cluster0.jzr1zc7.mongodb.net/test')
database = client.DataLog
collection = database.Order_Item_Transaction

async def fetch_general_products(date: str):
    products = []

    cursor = collection.aggregate([
        {
            "$project":
            {
                 "Quantity": "$Quantity",
                 "Category": "$Category",
                 "Product_Name": "$Product_Name",
                 "Date": "$Date"
            }
        },
        {
            "$match":
            {
                "Date": date
            }

        },
        {
            "$sort": {"Quantity": 1}
        },
        {
            "$limit": 10
        }
    ])
        
    async for document in cursor:
        products.append(General_Products(**document))
    return products
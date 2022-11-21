from sqlite3 import Cursor
from datetime import datetime
from typing import Collection
import motor.motor_asyncio
from models.model import General_Products

client = motor.motor_asyncio.AsyncIOMotorClient('mongodb+srv://DataLog:DataLog@cluster0.jzr1zc7.mongodb.net/test')
database = client.DataLog
collection = database.Order_Item_Transaction

async def fetch_general_products():
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
            "$sort": {"Quantity": 1}
        },
        {
            "$limit": 15
        }
    ])
        
    async for document in cursor:
        products.append(General_Products(**document))
    return products


# Fetch general products by date
async def fetch_products_by_date(start_date,end_date):
    products = []
    cursor = collection.aggregate([
        {
            "$match":
            {
                "Date": {"$gte": start_date, "$lte": end_date}
            }
        },
        {
            "$group":
            {
                "_id": "$Product_Name",
                "Category": {"$first":"$Category"},
                "Date": {"$first":"$Date"},
                "Quantity": {"$sum":"$Quantity"}   
            }
        },
        {
            "$sort": {"Quantity": 1}
        },
        {
            "$limit": 15
        }
    ])

    async for document in cursor:
        products.append(document)
    return products
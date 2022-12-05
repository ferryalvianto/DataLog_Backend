from sqlite3 import Cursor
from datetime import datetime
from typing import Collection
import motor.motor_asyncio
from models.model import General_Products


async def fetch_general_products(db: str):
    client = motor.motor_asyncio.AsyncIOMotorClient(
        'mongodb+srv://DataLog:DataLog@cluster0.jzr1zc7.mongodb.net/')
    database = client[db]
    collection = database.df_sales

    products = []

    cursor = collection.aggregate([
        {
            '$project':
            {
                "Quantity": {"$sum": "$Quantity"},
                "Category": "$Category",
                "Name": "$Name",
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
async def fetch_products_by_date(db, start_date, end_date):
    client = motor.motor_asyncio.AsyncIOMotorClient(
        'mongodb+srv://DataLog:DataLog@cluster0.jzr1zc7.mongodb.net/')
    database = client[db]
    collection = database.df_sales

    products = []

    cursor = collection.aggregate([
        {
            "$project":
            {
                "Name": "$Name",
                "Category": "$Category",
                "Quantity": {"$sum": "$Quantity"},
                "Date": "$Date"
            }

        },
        {
            "$match":
            {
                "Date": {"$gte": start_date, "$lte": end_date}
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

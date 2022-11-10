from typing import Collection
import motor.motor_asyncio
from models.model import DailyRevenueForecast

client = motor.motor_asyncio.AsyncIOMotorClient('mongodb+srv://DataLog:DataLog@cluster0.jzr1zc7.mongodb.net/test')
database = client.DataLog
collection = database.Forecast_Revenue

async def fetch_latest_forecast_revenues():
    revenues = []
    cursor = collection.find({ 'Flag_latest':'1'})
    async for document in cursor:
        revenues.append(DailyRevenueForecast(**document))
    return revenues
import motor.motor_asyncio
from models.model import ProductQuantityForecast

client = motor.motor_asyncio.AsyncIOMotorClient('mongodb+srv://DataLog:DataLog@cluster0.jzr1zc7.mongodb.net/test')
database = client.DataLog
collection = database.Forecast_Product_Quantity

async def fetch_latest_forecast_quantity():
    quantities = []
    cursor = collection.find({})
    async for document in cursor:
        quantities.append(ProductQuantityForecast(**document))
    return quantities
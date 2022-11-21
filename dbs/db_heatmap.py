import motor.motor_asyncio

client = motor.motor_asyncio.AsyncIOMotorClient('mongodb+srv://DataLog:DataLog@cluster0.jzr1zc7.mongodb.net/test')
database = client.DataLog
collection = database.Order_Item_Transaction

#fetch all HT_Category
async def fetch_all_ht_category():
    ht_category = []
    cursor = collection.aggregate([
                                {'$match': {'Action':'Sales'}},
                                {'$group': {"_id" : "$HT_Category", "Total_Quantity" : {"$sum": "$Quantity"}, 
                                    "X_Coordinate":{"$first":"$ht_x_coor" }, "Y_Coordinate":{"$first":"$ht_y_coor" }}},
                                {
                                    '$project': {
                                        "HT_Category":"$id",
                                        "Coordinates": ["$X_Coordinate", "$Y_Coordinate"],
                                        "Total_Quantity" : 1
                                    }
                                }
                                ])

    async for document in cursor:
        ht_category.append(document)
    return ht_category



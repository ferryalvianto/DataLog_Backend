import motor.motor_asyncio

client = motor.motor_asyncio.AsyncIOMotorClient(
    'mongodb+srv://DataLog:DataLog@cluster0.jzr1zc7.mongodb.net')


# fetch all HT_Category
async def fetch_all_ht_category(db):
    ht_category = []
    mydb = client[db]
    collection = mydb['df_sales']
    cursor = collection.aggregate([
        {'$group': {"_id": "$HT_Category", "Total_Quantity": {"$sum": "$Quantity"},
                    "X_Coordinate": {"$first": "$x_coor"}, "Y_Coordinate": {"$first": "$y_coor"}}},
        {
            '$project': {
                "HT_Category": "$id",
                "Coordinates": ["$X_Coordinate", "$Y_Coordinate"],
                "Total_Quantity": 1
            }
        }
    ])

    async for document in cursor:
        ht_category.append(document)
    return ht_category


async def fetch_date_range_ht_category(db, start_date, end_date):
    ht_category = []
    mydb = client[db]
    collection = mydb['df_sales']
    cursor = collection.aggregate([{'$match': {'Date': {"$gte": start_date, "$lte":  end_date}}},
                                   {'$group': {"_id": "$HT_Category", "Total_Quantity": {"$sum": "$Quantity"},
                                               "X_Coordinate": {"$first": "$x_coor"}, "Y_Coordinate": {"$first": "$y_coor"}}},
                                   {
        '$project': {
            "HT_Category": "$id",
            "Coordinates": ["$X_Coordinate", "$Y_Coordinate"],
            "Total_Quantity": 1
        }
    }
    ])
    async for document in cursor:
        ht_category.append(document)
    return ht_category

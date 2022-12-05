import numpy as np
import pandas as pd
import pymongo


def nps_score(db: str):
    client = pymongo.MongoClient('mongodb+srv://DataLog:DataLog@cluster0.jzr1zc7.mongodb.net/')
    mydb = client[db]
    collection = mydb['Sentiments_Analysis']

    sentiments = []

    for x in collection.find():
        sentiments.append(x)

    df = pd.DataFrame(sentiments)

    # classification_code = {'Very Dissatisfied': 1,'Dissatisfied': 2,'Neutral' : 3,'Satisfied' : 4,'Very Satisfied' : 5}
    # df['NPS_Code'] = df['Classification'].map(classification_code)

    def nps_bucket(x):
        if x > 3:
            bucket = 'promoter'
        elif x > 2:
            bucket = 'passive'
        elif x >= 1:
            bucket = 'detractor'
        else:
            bucket = 'no score'
        return bucket


    df['NPS_Score'] = df['rating'].apply(nps_bucket)

    grouped_df = df.groupby(['created_time'])['NPS_Score'].apply(lambda x: (x.str.contains('promoter').sum() - x.str.contains('detractor').sum()) / 
                    (x.str.contains('promoter').sum() + x.str.contains('passive').sum() + x.str.contains('detractor').sum())).reset_index()

    
    results = grouped_df.to_dict("records")

    return results
    
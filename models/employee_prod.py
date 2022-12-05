import pandas as pd
import numpy as np
from sklearn.metrics import mean_absolute_error, mean_absolute_percentage_error
import pymongo
from datetime import datetime, timedelta


def cleancsv_order_hist(db: str, id_order_hist: str):
    client = pymongo.MongoClient('mongodb+srv://DataLog:DataLog@cluster0.jzr1zc7.mongodb.net/')

    url = 'https://drive.google.com/uc?id=' + id_order_hist
    #1mWYqRj056bkxfUG3yRsQrqp1mXLtLr6v
    df = pd.read_csv(url)
    df = df[['Order No', 'Created By','First Opened', 'Last Closed']].copy()
    df = df.astype({'Order No': int})
    df.dropna(inplace=True)

    steps_l = list(np.arange(0, len(df), 5000)) + [len(df)]

    for start, end in zip(steps_l, steps_l[1:]):
        client[db]['df_order_hist'].insert_many(df.iloc[start:end].to_dict(orient="records"))

    df_return = df.head()
    df_return = df_return.to_dict(orient="records")
    
    return df_return

def employee_speed(db: str):
    client = pymongo.MongoClient('mongodb+srv://DataLog:DataLog@cluster0.jzr1zc7.mongodb.net/')
    mydb = client[db]
    orders_collection = mydb['df_order_hist']
    sales_collection = mydb['df_sales']

    orders = []
    sales =[]

    for x in orders_collection.find({}):
        orders.append(x)

    for x in sales_collection.find({}):
        sales.append(x)

    df_orders = pd.DataFrame(orders)
    df_sales = pd.DataFrame(sales)

    df_orders = df_orders[['Order No','First Opened','Last Closed']].copy()

    df_orders.rename(columns={'Order No': 'OrderID'}, inplace=True)

    df_orders = df_orders.dropna()

    df_sales.rename(columns={'orderID': 'OrderID'}, inplace=True)
    df_orders_hist = pd.merge(df_orders, df_sales[['OrderID','Quantity','Employee']],on='OrderID',how='right')

    for i, qty in enumerate(df_orders_hist['Quantity']):
        if qty % 1 != 0:
            df_orders_hist['Quantity'][i] = 1

    
    orderquantity_df = df_orders_hist.groupby(['OrderID'])['Quantity'].sum().reset_index(name ='Quantity')
    orderquantity_df = pd.merge(orderquantity_df, df_orders_hist[['OrderID','Employee','First Opened','Last Closed']], on='OrderID', how='right')
    orderquantity_df.drop_duplicates(subset=['OrderID'], keep='first', inplace=True, ignore_index=True)

    orderquantity_df.dropna(inplace=True)

    orderquantity_df['First Opened'] = pd.to_datetime(orderquantity_df['First Opened'])
    orderquantity_df['Last Closed'] = pd.to_datetime(orderquantity_df['Last Closed'])

    orderquantity_df['Seconds'] = orderquantity_df['Last Closed'] - orderquantity_df['First Opened']

    orderquantity_df['Seconds'] = orderquantity_df['Seconds'] / np.timedelta64(1, 's')
    totalitems = orderquantity_df.groupby(['Employee'])['Quantity'].sum().reset_index(name ='Total Items')
    totalseconds = orderquantity_df.groupby(['Employee'])['Seconds'].sum().reset_index(name ='Total Seconds')

    result1 = pd.merge(totalitems, totalseconds, on='Employee', how='right')
    result1['ItemPerSecond'] = round((result1['Total Items'] / result1['Total Seconds']),2)
    result1.sort_values('ItemPerSecond', ignore_index=True, inplace=True)
    results = pd.DataFrame(result1)

    results = results.to_dict(orient='records')
    
    return results



    
    
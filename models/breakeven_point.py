import numpy as np
import pandas as pd
import pymongo

def breakeven_point(db:str):
    client = pymongo.MongoClient('mongodb+srv://DataLog:DataLog@cluster0.jzr1zc7.mongodb.net/')
    mydb = client[db]
    sales_collection = mydb['df_sales']
    costs_collection = mydb['df_costs']

    sales = []
    costs = []

    for x in sales_collection.find({}):
        sales.append(x)
    
    for x in costs_collection.find({}):
        costs.append(x)

    df_sales = pd.DataFrame(sales)
    df_costs = pd.DataFrame(costs)

    rel_col = ['Date', 'Name','itemPrice', 'dailyRevenue']
    df_breakeven = df_sales[rel_col]

    # Get the weekly costs
    df_costs['Weekly_Costs'] = df_costs['CY']/4

    fixed_costs = df_costs['Weekly_Costs'].sum()

    df_breakeven['Variable_Costs'] = df_breakeven['itemPrice'] * 0.07

    # Number of units to break even = Fixed Costs / (Average Price – Variable Costs)
    df_breakeven['Breakeven_units'] = fixed_costs / (df_breakeven['itemPrice'].values - df_breakeven['Variable_Costs'].values)

    results = df_breakeven.to_dict(orient="records")

    return results


    


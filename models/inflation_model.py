import pandas as pd
import pymongo

def inflation_model(db: str):
    client = pymongo.MongoClient('mongodb+srv://DataLog:DataLog@cluster0.jzr1zc7.mongodb.net/')
    mydb = client[db]
    revenue_collection = mydb['revenue_forecast']

    Datalog_db = client['DataLog']
    cpi_col = Datalog_db['CPI']

    revenue_forecast = []
    cpi = []

    for x in revenue_collection.find():
        revenue_forecast .append(x)

    for x in cpi_col.find():
        cpi.append(x)

    revenue_df = pd.DataFrame(revenue_forecast)
    cpi_df = pd.DataFrame(cpi)

    revenue_df.rename(columns={'latest_date_in_model': 'Date'}, inplace=True)

    # Get new columns for Month and Year
    revenue_df['Year'] = revenue_df['Date'].str[0:4]
    revenue_df['Month'] = revenue_df['Date'].str[5:7]

    revenue_df['Previous_Month'] = revenue_df['Month'].values.astype(int) - 1
    revenue_df.drop(['Month'], axis=1, inplace=True)        

    revenue_df.rename(columns={'Previous_Month': 'Month'}, inplace=True)
    revenue_df['Month'] = revenue_df['Month'].astype(str)   

    df = pd.merge(revenue_df, cpi_df, on=['Month','Year'], how='left')

    df.rename(columns={'Total_CPI': 'Current_CPI'}, inplace=True)
    df.rename(columns={'Month': 'Month1'}, inplace=True)   

    df['Previous_Month'] = df['Month1'].values.astype(int) - 1

    df.rename(columns={'Previous_Month': 'Month'}, inplace=True)
    df['Month'] = df['Month'].astype(str).str.zfill(2)

    df2 = pd.merge(df, cpi_df, on=['Month','Year'], how='left')

    df2.rename(columns={'Total_CPI': 'Previous_CPI'}, inplace=True)
    df2.rename(columns={'Month': 'Month2'}, inplace=True)

    df_inflation = df2.copy()
    df_inflation['CY_predictedRevenue_Inflation'] = round(df_inflation['CY_predictedRevenue'] * (1 + ((df2['Current_CPI'] - df2['Previous_CPI']) / df2['Previous_CPI'])),2)
    df_inflation['OA_predictedRevenue_Inflation'] = round(df_inflation['OA_predictedRevenue'] * (1+ ((df2['Current_CPI'] - df2['Previous_CPI']) / df2['Previous_CPI'])),2)

    revenue_inflation = df_inflation[['date', 'CY_predictedRevenue_Inflation', 'OA_predictedRevenue_Inflation']].copy()

    results = revenue_inflation.to_dict("records")

    return results







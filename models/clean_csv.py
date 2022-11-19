import numpy as np
import pandas as pd
import pymongo

INVENTORY_LOG = 'inventorylog'
PAYMENT_TYPE = 'paymenttype'
ACTION_LOG = 'actionlog'

def cleancsv(db: str):
    client = pymongo.MongoClient(
        'mongodb+srv://DataLog:DataLog@cluster0.jzr1zc7.mongodb.net/')
    mydb = client[db]
    return mydb


def clean_inventorylog(id:str, db:str):
    mydb = cleancsv(db)

    url='https://drive.google.com/uc?id=' + id
    df = pd.read_csv(url)

    if 'Action' in df.columns:
        dummy = pd.get_dummies(df['Action'])
        df = df.merge(dummy, left_index=True, right_index=True)
        df.drop('Action', axis=1, inplace=True)
    
    if 'Cost' in df.columns:
        df.drop('Cost', axis=1, inplace=True)

    if 'SKU' in df.columns:
        df.drop('SKU', axis=1, inplace=True)

    if 'Invoice Number' in df.columns:
       df.drop('Invoice Number', axis=1, inplace=True)

    if 'PO Number' in df.columns:
        df.drop('PO Number', axis=1, inplace=True)

    if 'Transfer ID' in df.columns:
       df.drop('Transfer ID', axis=1, inplace=True)

    if 'Barcode' in df.columns:
      df.drop('Barcode', axis=1, inplace=True)

    if 'Unit' in df.columns:
        df.drop('Unit', axis=1, inplace=True)

    if 'Vendor' in df.columns:
        df.drop('Vendor', axis=1, inplace=True)

    if 'Inventory Qty' in df.columns:
        df.drop('Inventory Qty', axis=1, inplace=True)

    if 'Inv. Value After' in df.columns:
        df.drop('Inv. Value After', axis=1, inplace=True)

    if 'Inv. Value Before' in df.columns:
        df.drop('Inv. Value Before', axis=1, inplace=True)

    if 'Inv. Change Value' in df.columns:
        df.drop('Inv. Change Value', axis=1, inplace=True)

    if '%Quantity Change' in df.columns:
        df.drop('%Quantity Change', axis=1, inplace=True)

    if '%Value Change' in df.columns:
        df.drop('%Value Change', axis=1, inplace=True)

    if 'Cost After' in df.columns:
        df.drop('Cost After', axis=1, inplace=True)

    if 'Cost Before' in df.columns:
        df.drop('Cost Before', axis=1, inplace=True)

    if 'Quantity After' in df.columns:
       df.drop('Quantity After', axis=1, inplace=True)

    if 'Quantity Before' in df.columns:
        df.drop('Quantity Before', axis=1, inplace=True)

    if df['Establishment'].str.contains('Organic Acres').any():
        df['Establishment'].values[:] = 0

    if df['Establishment'].str.contains('Cypress').any():
        df['Establishment'].values[:] = 1

    sales = df[df['Sales'].isin({1})].reset_index(drop=True)

    return sales

# if 'Remarks' in sales.columns:
#     sales.drop('Remarks', axis=1, inplace=True)

# if 'Wastage' in sales.columns:
#     sales.drop('Wastage', axis=1, inplace=True)
    
# if 'Started' in sales.columns:
#     sales.drop('Started', axis=1, inplace=True)
    
# if 'Return' in sales.columns:
#     sales.drop('Return', axis=1, inplace=True)

# if 'Adjustment' in sales.columns:
#     sales.drop('Adjustment', axis=1, inplace=True)

# if 'Sales' in sales.columns:
#     sales.drop('Sales', axis=1, inplace=True)

# if 'Reset Cost' in sales.columns:
#     sales2021.drop('Reset Cost', axis=1, inplace=True)
    
# if 'Received' in sales.columns:
#     sales.drop('Received', axis=1, inplace=True)
    
# if 'Transfer In' in sales.columns:
#     sales.drop('Transfer In', axis=1, inplace=True)
    
# if 'Transfer Out' in sales.columns:
#     sales.drop('Transfer Out', axis=1, inplace=True)
    
# if 'Return to Vendor' in sales.columns:
#     sales.drop('Return to Vendor', axis=1, inplace=True)

# #creating new columns for order ID and sales
# sales[['orderID', 'itemPrice']] = sales['Order Details'].str.split(pat=',', expand=True)

# #dropping order details column
# sales.drop('Order Details', axis=1, inplace=True)

# #removing specific string in a column
# sales['orderID'] = sales['orderID'].str.replace('order_id:', '')
# sales['itemPrice'] = sales['itemPrice'].str.replace('net_sales_price:', '')

# #renaming user column to employee
# sales.rename(columns={'User': 'Employee'}, inplace=True)

# #removing returns from sales
# sales = sales[~sales['orderID'].str.contains('returns_id:') == True]

# sales['ymd'] = pd.to_datetime(sales['Date/Time']).dt.date
# sales['ymd'] = pd.to_datetime(sales['ymd'], format='%Y-%m-%d')

# sales['Quantity'] = sales['Quantity'].round().astype(int)

# sales = sales.astype({'orderID':int, 'itemPrice': float, 'Establishment':int})


# def clean_paymenttype(id:str):

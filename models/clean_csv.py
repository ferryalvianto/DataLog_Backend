import numpy as np
import pandas as pd
import pymongo

def cleancsv(db: str, id_inventory: str, id_payment: str, year: str, month: str, day: str):
    client = pymongo.MongoClient('mongodb+srv://DataLog:DataLog@cluster0.jzr1zc7.mongodb.net/')

    url = 'https://drive.google.com/uc?id=' + id_inventory
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

    if 'Product Class' in df.columns:
        df.drop('Product Class', axis=1, inplace=True)

    if df['Establishment'].str.contains('Organic Acres').any():
        df['Establishment'].values[:] = 0

    if df['Establishment'].str.contains('Cypress').any():
        df['Establishment'].values[:] = 1

    sales = df[df['Sales'].isin({1})].reset_index(drop=True)

    if 'Remarks' in sales.columns:
        sales.drop('Remarks', axis=1, inplace=True)

    if 'Wastage' in sales.columns:
        sales.drop('Wastage', axis=1, inplace=True)

    if 'Started' in sales.columns:
        sales.drop('Started', axis=1, inplace=True)

    if 'Return' in sales.columns:
        sales.drop('Return', axis=1, inplace=True)

    if 'Adjustment' in sales.columns:
        sales.drop('Adjustment', axis=1, inplace=True)

    if 'Sales' in sales.columns:
        sales.drop('Sales', axis=1, inplace=True)

    if 'Reset Cost' in sales.columns:
        sales.drop('Reset Cost', axis=1, inplace=True)

    if 'Received' in sales.columns:
        sales.drop('Received', axis=1, inplace=True)

    if 'Transfer In' in sales.columns:
        sales.drop('Transfer In', axis=1, inplace=True)

    if 'Transfer Out' in sales.columns:
        sales.drop('Transfer Out', axis=1, inplace=True)

    if 'Return to Vendor' in sales.columns:
        sales.drop('Return to Vendor', axis=1, inplace=True)

    # creating new columns for order ID and sales
    sales[['orderID', 'itemPrice']] = sales['Order Details'].str.split(
        pat=',', expand=True)

    # dropping order details column
    sales.drop('Order Details', axis=1, inplace=True)

    # removing specific string in a column
    sales['orderID'] = sales['orderID'].str.replace('order_id:', '')
    sales['itemPrice'] = sales['itemPrice'].str.replace('net_sales_price:', '')

    # renaming user column to employee
    sales.rename(columns={'User': 'Employee'}, inplace=True)

    # removing returns from sales
    sales = sales[~sales['orderID'].str.contains('returns_id:') == True]

    sales['ymd'] = pd.to_datetime(sales['Date/Time']).dt.date
    sales['ymd'] = pd.to_datetime(sales['ymd'], format='%Y-%m-%d')

    sales['Date'] = pd.to_datetime(sales['Date/Time']).dt.date
    sales['Time'] = pd.to_datetime(sales['Date/Time']).dt.time

    sales.drop('Date/Time', axis=1, inplace=True)

    # sales['Quantity'] = sales['Quantity'].round().astype(int)

    sales = sales.astype(
        {'orderID': int, 'itemPrice': float, 'Establishment': int})

    paymenturl = 'https://drive.google.com/uc?id=' + id_payment
    payment = pd.read_csv(paymenturl)

    if 'Transaction Status' in payment.columns:
        payment.drop('Transaction Status', axis=1, inplace=True)

    if 'Transaction Id' in payment.columns:
        payment.drop('Transaction Id', axis=1, inplace=True)

    if 'Last 4 CC Digits' in payment.columns:
        payment.drop('Last 4 CC Digits', axis=1, inplace=True)

    if 'Employee' in payment.columns:
        payment.drop('Employee', axis=1, inplace=True)

    if 'Raw Date' in payment.columns:
        payment.drop('Raw Date', axis=1, inplace=True)

    if 'Station' in payment.columns:
        payment.drop('Station', axis=1, inplace=True)

    if 'Rounding Delta' in payment.columns:
        payment.drop('Rounding Delta', axis=1, inplace=True)

    payment.rename(columns={'Order No': 'orderID'}, inplace=True)

    dummy = pd.get_dummies(payment['Card Type'])
    payment = payment.merge(dummy, left_index=True, right_index=True)

    dummy = pd.get_dummies(payment['Type'])
    payment = payment.merge(dummy, left_index=True, right_index=True)

    if 'Card Type' in payment.columns:
        payment.drop('Card Type', axis=1, inplace=True)

    if 'Type' in payment.columns:
        payment.drop('Type', axis=1, inplace=True)

    if 'Credit' in payment.columns:
        payment.drop('Credit', axis=1, inplace=True)

    if 'Adjustments' in payment.columns:
        payment.drop('Adjustments', axis=1, inplace=True)

    if 'Debit Card' in payment.columns:
        payment.drop('Debit Card', axis=1, inplace=True)

    if 'Other_x' in payment.columns:
        payment.drop('Other_x', axis=1, inplace=True)

    if 'Other_y' in payment.columns:
        payment.drop('Other_y', axis=1, inplace=True)

    if 'Customer Paid' in payment.columns:
        payment.drop('Customer Paid', axis=1, inplace=True)

    if 'Customer Change' in payment.columns:
        payment.drop('Customer Change', axis=1, inplace=True)

    payment['Date'] = pd.to_datetime(payment['Date'])

    payment['day'] = payment['Date'].dt.day
    payment['month'] = payment['Date'].dt.month
    payment['year'] = payment['Date'].dt.year
    payment['hour'] = payment['Date'].dt.hour

    payment['ymd'] = pd.to_datetime(payment[['year', 'month', 'day']])

    payment.drop('Date', axis=1, inplace=True)

    df_dailyRevenue = payment.groupby(
        ['ymd'])['Total'].sum().reset_index(name='dailyRevenue')

    payment['dailyRevenueByOrder'] = payment.groupby(['ymd'])['Total'].cumsum()

    payment = payment.merge(df_dailyRevenue, on='ymd', how='left')

    payment.drop('ymd', axis=1, inplace=True)

    df = sales.merge(payment, on='orderID', how='right')
    df = df.dropna()
    df = df.astype({'Establishment': int})

    base_url = "http://climate.weather.gc.ca/climate_data/bulk_data_e.html?"
    query_url = "format=csv&stationID={}&Year={}&Month={}&Day={}&timeframe=1".format('51442', year, month, day)
    api_endpoint = base_url + query_url
    df_temp = pd.read_csv(api_endpoint, skiprows=0)

    df_temp['Date'] = pd.to_datetime(df_temp['Date/Time (LST)']).dt.date

    df_temp['Date'] = df_temp['Date'].apply(lambda x: x.strftime('%Y-%m-%d'))

    df_temp.dropna()

    df_temp['Max_Temp_C_'] = df_temp['Temp (°C)'].max()
    df_temp['Min_Temp_C_'] = df_temp['Temp (°C)'].min()
    df_temp['Temperature'] = round(df_temp['Temp (°C)'].mean(), 2)

    df_temp = df_temp[['Date', 'Temperature', 'Min_Temp_C_', 'Max_Temp_C_']]

    df_temp = df_temp[df_temp['Date'].isin({year+'-'+month+'-'+day})].reset_index(drop=True)

    df_temp = df_temp.iloc[[0]]

    df['Date'] = df.Date.apply(lambda x: x.strftime('%Y-%m-%d'))
    df.drop("Time", axis=1, inplace=True)

    df = df.merge(df_temp, on='Date', how='left')

    if 'Gift Cards' not in df.columns:
        df['Gift Cards'] = 0

    if 'American Express' in df.columns:
        df.rename(columns={'American Express': 'Amex'}, inplace=True)
        df['Discover'] = 0
    elif 'Discover' in df.columns:
        df['Amex'] = 0
    else:
        df['Amex'] = 0
        df['Discover'] = 0

    df = df[['orderID', 'Establishment', 'Employee', 'Name', 'Category', 'Quantity', 'itemPrice', 'Total', 'Cash', 'Debit', 'Visa', 'Mastercard', 'Discover',
                'Amex', 'Gift Cards', 'hour', 'day', 'month', 'year', 'Date', 'dailyRevenueByOrder', 'dailyRevenue', 'Temperature', 'Min_Temp_C_', 'Max_Temp_C_']]

    df[['orderID', 'Establishment', 'Cash', 'Debit', 'Visa', 'Mastercard', 'Discover', 'Amex', 'Gift Cards', 'hour', 'day', 'month', 'year']] = df[[
            'orderID', 'Establishment', 'Cash', 'Debit', 'Visa', 'Mastercard', 'Discover', 'Amex', 'Gift Cards', 'hour', 'day', 'month', 'year']].astype(int)

    df.reset_index(inplace=True)
    df.drop("index", axis=1, inplace=True)

    cpi_url = "https://www.bankofcanada.ca/valet/observations/group/CPI_MONTHLY/csv"
    col_names = ["date", "V41690973"]
    df_cpi = pd.read_csv(cpi_url, sep='delimiter', header=None, engine='python')
    df_cpi = df_cpi.iloc[20:]
    df_cpi.reset_index(inplace=True)
    df_cpi.drop("index", axis=1, inplace=True)
    headers = df_cpi.iloc[0].values
    df_cpi.columns = headers
    df_cpi.drop(index=0, axis=0, inplace=True)
    df_cpi[['date', 'cpis']] = df_cpi['"date","V41690973","V41690914","STATIC_TOTALCPICHANGE","CPI_TRIM","CPI_MEDIAN","CPI_COMMON","ATOM_V41693242","STATIC_CPIXFET","CPIW"'].str.split(',', n=1, expand=True)
    df_cpi = df_cpi[['date','cpis']].copy()
    df_cpi[['cpi','cpis']] = df_cpi['cpis'].str.split(',', n=1, expand=True)
    df_cpi = df_cpi[['date','cpi']].copy()
    df_cpi['date'] = df_cpi['date'].str.replace('"', '')
    df_cpi['cpi'] = df_cpi['cpi'].str.replace('"', '')
    df_cpi['date'] = pd.to_datetime(df_cpi['date'])
    df_cpi['month'] = df_cpi['date'].dt.month
    df_cpi['year'] = df_cpi['date'].dt.year
    df_cpi = df_cpi[['year','month','cpi']].copy()
    df_cpi['cpi'] = df_cpi['cpi'].astype(float)

    df = df.merge(df_cpi, on=['year','month'], how='left')

    if df['cpi'].isnull().values.any():
        avg = df_cpi['cpi'][-3:].mean() + (df_cpi['cpi'][-1:] * 0.01)
        avg = round(avg.iloc[0], 2)
        df['cpi'] = df['cpi'].fillna(avg)

    #reading gas prices csv
    gas = pd.read_csv('https://www150.statcan.gc.ca/t1/tbl1/en/dtl!downloadDbLoadingData-nonTraduit.action?pid=1810000101&latestN=0&startDate=20220101&endDate=20221201&csvLocale=en&selectedMembers=%5B%5B%5D%2C%5B2%5D%5D&checkedLevels=0D1%2C0D2')
    gas = gas[gas['GEO'].str.contains('Vancouver')]
    gas = gas[['REF_DATE','VALUE']].copy()
    gas[['year', 'month']] = gas['REF_DATE'].str.split('-', 1, expand=True)
    gas[['year', 'month']] = gas[['year', 'month']].astype(int)
    gas = gas[['year','month','VALUE']].copy()
    gas.rename(columns={'VALUE': 'averageGasPrice'}, inplace=True)

    df = df.merge(gas, on=['month','year'], how='left')

    if df['averageGasPrice'].isnull().values.any():
        avg = gas['averageGasPrice'][-3:].mean() + (gas['averageGasPrice'][-1:] * 0.01)
        avg = round(avg.iloc[0], 2)
        df['averageGasPrice'] = df['averageGasPrice'].fillna(avg)

    steps_l = list(np.arange(0, len(df), 5000)) + [len(df)]

    ####-----______________HEATMAP-----------####

    def convert_HT_cat(category,product_name):
    
        #produce
        if(category=='Produce'): 
        
            if ('Avocado' in product_name):
                return 'Avocados'
            elif('Banana' in product_name):
                return 'Bananas'
            elif (('Peppers' in product_name)|('Tomatoes' in product_name) | ('Tomatillos' in product_name) ):
                return 'Peppers and Tomatoes'
            elif (('Potatoes' in product_name) | ('Yams' in product_name) | ('Onion' in product_name) ):
                return 'Tubers and Onions'
            elif (('Apples' in product_name) | ('Oranges' in product_name) | 
                                        ('Grapefruit' in product_name) | ('Lemons' in product_name) | 
                                        ('Tangelo' in product_name) |  ('Tangerine' in product_name)):
                return 'Apples and Citruses'
            elif (('Basil' in product_name) | ('Cilantro' in product_name) | 
                                        ('Dill' in product_name) | ('Mint' in product_name) | 
                                        ('Parsley' in product_name) |  ('Rosemary' in product_name) | 
                                        ('Sage' in product_name) | ('Thyme' in product_name) ):
                return 'Herbs'

            else:
                return 'Produce'
            
        #Coffee Bar
        elif(category=='Coffee Bar'):
            return 'Coffee Bar'
        
        #Deli
        elif(category=='Deli'):
            return 'Deli'
        
        #Dairy
        elif(category=='Dairy'):
            if('Ice Cream' in product_name):
                return 'Ice Cream'
            else:
                return 'Dairy'
        
        #Snacks
        elif(category=='Snacks'):
            return 'Chips'
        
        #Heat & Eat
        elif(category=='Heat & Eat'):
            if('Soup' in product_name):
                return 'Soups'
        
        #
        
        else:
            return 'Others'
        

    #assigning value to HT_Category

    df["HT_Category"] = df.apply(lambda x: convert_HT_cat(x.Category, x.Product_Name), axis=1)

    #creating a function to assign coordinates

    def assign_x_coordinate(ht_cat):
        if(ht_cat=='Coffee Bar'):
            return 541
        elif(ht_cat =='Seeds'):
            return 47
        elif(ht_cat=='Avocados'):
            return 69
        elif(ht_cat=='Cooler 1'):
            return 69
        elif(ht_cat=='Cooler 2'):
            return 69
        elif(ht_cat=='Bananas'):
            return 69
        elif(ht_cat=='Cards'):
            return 180
        elif(ht_cat=='Peppers and Tomatoes'):
            return 409
        elif(ht_cat=='Tubers and Onions'):
            return 409   
        elif(ht_cat=='Apples and Citruses'):
            return 409
        elif(ht_cat=='Isle 1'):
            return 1020
        elif(ht_cat=='Isle 2'):
            return 1020
        elif(ht_cat=='Isle 3'):
            return 1020
        elif(ht_cat=='Chips'):
            return 1525
        elif(ht_cat=='Herbs'):
            return 450
        elif(ht_cat=='Produce'):
            return 255
        elif(ht_cat=='Soups'):
            return 630
        elif(ht_cat=='Deli'):
            return 790
        elif(ht_cat=='Dairy'):
            return 905
        elif(ht_cat=='Vegan'):
            return 1060
        elif(ht_cat=='Freezer'):
            return 1250
        elif(ht_cat=='Ice Cream'):
            return 1440
        else:
            return 11
    
    def assign_y_coordinate(ht_cat):
        if(ht_cat=='Coffee Bar'):
            return 54
        elif(ht_cat =='Seeds'):
            return 348
        elif(ht_cat=='Avocados'):
            return 741
        elif(ht_cat=='Cooler 1'):
            return 875
        elif(ht_cat=='Cooler 2'):
            return 1025
        elif(ht_cat=='Bananas'):
            return 1200
        elif(ht_cat=='Cards'):
            return 480    
        elif(ht_cat=='Peppers and Tomatoes'):
            return 480
        elif(ht_cat=='Tubers and Onions'):
            return 740     
        elif(ht_cat=='Apples and Citruses'):
            return 1020
        elif(ht_cat=='Isle 1'):
            return 480
        elif(ht_cat=='Isle 2'):
            return 740
        elif(ht_cat=='Isle 3'):
            return 1020
        elif(ht_cat=='Chips'):
            return 520
        elif(ht_cat=='Herbs'):
            return 1220
        elif(ht_cat=='Produce'):
            return 1220
        elif(ht_cat=='Soups'):
            return 1220
        elif(ht_cat=='Deli'):
            return 1220
        elif(ht_cat=='Dairy'):
            return 1220
        elif(ht_cat=='Vegan'):
            return 1220
        elif(ht_cat=='Freezer'):
            return 1220
        elif(ht_cat=='Ice Cream'):
            return 1220
        else:
            return 11

    df['x_coor'] = df.HT_Category.apply(lambda x: assign_x_coordinate(x))
    df['y_coor'] = df.HT_Category.apply(lambda x: assign_y_coordinate(x))


    
    ################################################





    for start, end in zip(steps_l, steps_l[1:]):
        client[db]['df_sales'].insert_many(
            df.iloc[start:end].to_dict(orient="records"))

    df_return = df.head()
    df_return = df_return.to_dict(orient="records")

    return df_return

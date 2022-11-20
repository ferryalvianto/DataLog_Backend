import pymongo
import pandas as pd
import numpy as np
from sklearn import datasets
from sklearn.linear_model import LinearRegression
from sklearn.model_selection import train_test_split
import pickle
import time

def save_model_to_db():

    myclient = pymongo.MongoClient('mongodb+srv://DataLog:DataLog@cluster0.jzr1zc7.mongodb.net/DataLog')
    mydb = myclient["DataLog"]
    #collection for transaction
    mycol = mydb["Order_Item_Transaction"]

    transaction_list = []

    for x in mycol.find():
        transaction_list.append(x)
    
    df = pd.DataFrame(transaction_list)

   #only getting relevant Columns for Forecasting Quantity
    rel_col = ["Category","Quantity", "Date","Temperature", "Min_Temp_C_", "Max_Temp_C_", "Year"]
    df = df[rel_col]

    #creating column for daily quantity sold for a product
    df["daily_quantity_sold"] = df.groupby(['Category', 'Date'])['Quantity'].transform('sum')

    #droping quantity column as it is not needed anymore
    df.drop(["Quantity", "Date"], axis=1, inplace=True)

    #removing duplicates, as many product in same date have now sililar info
    df.drop_duplicates(inplace=True)

    df = pd.get_dummies(df, drop_first=True)

    df.columns = df.columns.str.replace('Category_', '')

    X = df.drop("daily_quantity_sold", axis=1)
    Y = df.daily_quantity_sold

    
    linear_model = LinearRegression()
    xtrain, xtest, ytrain, ytest = train_test_split(X, Y, test_size=0.20, random_state=0)
    linear_model.fit(xtrain,ytrain)


    model = linear_model
    model_name = "my_linear_model"
    
    #pickling the model
    pickled_model = pickle.dumps(model)
    

    #collection for regression model
    dbconnection='regression_models'

    #creating collection
    mycon = mydb[dbconnection]
    info = mycon.insert_one({ model_name: pickled_model, 
                             'name': model_name, 
                            'created_time': time.time()})
    
    
    details = {
        'model_name' : model_name,
        'created_time': time.time()
    }
    return details







def load_saved_model_from_db(weather_data):


    client= 'mongodb+srv://DataLog:DataLog@cluster0.jzr1zc7.mongodb.net/test'
    db = 'DataLog'
    dbconnection='regression_models'
    model_name = "my_linear_model"

    
    json_data = {}
    
    #saving model to mongoDB
    #creating connection
    myclient = pymongo.MongoClient(client)
    
    #fetch model in mongodb
    mydb = myclient[db]
    mycon = mydb[dbconnection]
    data = mycon.find({'name': model_name})
        
    for i in data:
        json_data = i

    pickled_model = json_data[model_name]   

    linear_fetch = pickle.loads(pickled_model)
   


    #fetch category mongodb

    dbconnection_Order_Item_Transaction = "Order_Item_Transaction"
    mycon = mydb[dbconnection_Order_Item_Transaction]
    Categories = mycon.distinct("Category")
    Categories.pop(0)

    df = pd.DataFrame()

  

    df["Temperature"] = ""
    df["Min_Temp_C_"] = ""
    df["Max_Temp_C_"] = ""
    df["Year"] = ""
    df["Category"] = ""

      #creating columns for Categories
    for column in Categories:
        df[column] = column

    for column in Categories:
        for i in range(1,len(weather_data)):
            df = df.append({column:1, 'Temperature': weather_data[i].temp_avg
            , 
            'Min_Temp_C_':weather_data[i].temp_min, 'Max_Temp_C_': weather_data[i].temp_max,
            'Year': weather_data[i].dt_txt, 'Category': column
            }, ignore_index=True)

    
    df.fillna(0, inplace=True)

    df_orig = df.copy()
    df.drop("Category", axis=1, inplace=True)

    df["Year"] = df["Year"].apply(lambda x: x[0:4])

    df= df.astype("int32")


    prediction = linear_fetch.predict(df)

    prediction = pd.DataFrame(prediction, columns=["predicted_quantity"])
    
    prediction['Category'] = df_orig.Category
    prediction['Date'] = df_orig.Year

        

    prediction = prediction.to_dict("records")

    return prediction

    
    

def load_saved_model_from_db_with_category(weather_data, category):


    client= 'mongodb+srv://DataLog:DataLog@cluster0.jzr1zc7.mongodb.net/test'
    db = 'DataLog'
    dbconnection='regression_models'
    model_name = "my_linear_model"

    
    json_data = {}
    
    #saving model to mongoDB
    #creating connection
    myclient = pymongo.MongoClient(client)
    
    #fetch model in mongodb
    mydb = myclient[db]
    mycon = mydb[dbconnection]
    data = mycon.find({'name': model_name})
        
    for i in data:
        json_data = i

    pickled_model = json_data[model_name]   

    linear_fetch = pickle.loads(pickled_model)
   


    #fetch category mongodb

    dbconnection_Order_Item_Transaction = "Order_Item_Transaction"
    mycon = mydb[dbconnection_Order_Item_Transaction]
    Categories = mycon.distinct("Category")
    Categories.pop(0)

    df = pd.DataFrame()

    df["Temperature"] = ""
    df["Min_Temp_C_"] = ""
    df["Max_Temp_C_"] = ""
    df["Year"] = ""
    df["Category"] = ""


    #creating columns for Categories
    for column in Categories:
        df[column] = column

   

    for column in Categories:
        for i in range(1,len(weather_data)):
            df = df.append({column:1, 'Temperature': weather_data[i].temp_avg
            , 
            'Min_Temp_C_':weather_data[i].temp_min, 'Max_Temp_C_': weather_data[i].temp_max,
            'Year': weather_data[i].dt_txt, 'Category': column
            }, ignore_index=True)

    
    df.fillna(0, inplace=True)

    df_orig = df.copy()
    df.drop("Category", axis=1, inplace=True)

    df["Year"] = df["Year"].apply(lambda x: x[0:4])

    df= df.astype("int32")


    prediction = linear_fetch.predict(df)

    prediction = pd.DataFrame(prediction, columns=["predicted_quantity"])
    
    prediction['Category'] = df_orig.Category
    prediction['Date'] = df_orig.Year
    prediction = prediction.loc[prediction.Category==category]
    prediction = prediction.to_dict("records")

    
    return prediction
    
    



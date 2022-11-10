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
    
    mydb = myclient[db]
    mycon = mydb[dbconnection]
    data = mycon.find({'name': model_name})
    
    for i in data:
        json_data = i
        
    
    pickled_model = json_data[model_name]

    linear_fetch = pickle.loads(pickled_model)
    weather_data_df = pd.DataFrame(weather_data, columns=["temp", "temp_min", "temp_max", "dt_txt"])


    weather_data_df.temp = weather_data_df.temp.apply(lambda x: x[1])
    weather_data_df.temp_min = weather_data_df.temp_min.apply(lambda x: x[1])
    weather_data_df.temp_max = weather_data_df.temp_max.apply(lambda x: x[1])
    weather_data_df.dt_txt = weather_data_df.dt_txt.apply(lambda x: x[1])

    weather_data_df_orig = weather_data_df.copy()

    weather_data_df.rename(columns={"temp_max": "Max_Temp_C_", "temp_min":"Min_Temp_C_", "temp": "Temperature", "dt_txt": "Year"}, inplace=True)
    weather_data_df["Category_BOH"] =0
    weather_data_df["Category_Bakery"] =0
    weather_data_df["Category_Be Fresh Meals"] =0
    weather_data_df["Category_Be Fresh Products"] =0
    weather_data_df["Category_Beverages"] =0
    weather_data_df["Category_Coffee Bar"] =0
    weather_data_df["Category_Dairy"] =1
    weather_data_df["Category_Dairy - do not use"] =0
    weather_data_df["Category_Deli"] =0
    weather_data_df["Category_Fresh Prep"] =0
    weather_data_df["Category_Grocery"] =0
    weather_data_df["Category_Health & Beauty"] =0
    weather_data_df["Category_Health & Home"] =0
    weather_data_df["Category_Heat & Eat"] =0
    weather_data_df["Category_Meat & Seafood"] =0
    weather_data_df["Category_Produce"] =0
    weather_data_df["Category_Snacks" ] =0
    weather_data_df["Category_Standard (Do Not Use)"] =0
    weather_data_df['Year'] = weather_data_df["Year"].apply(lambda x: x[0:4])
  
    weather_data_df= weather_data_df.astype("int32")

    prediction = linear_fetch.predict(weather_data_df)

    prediction = pd.DataFrame(prediction, columns=["predicted_quantity"])
    prediction['Category'] = "Category_Dairy"

    prediction['Date'] = weather_data_df_orig.dt_txt

    prediction = prediction.to_dict("records")

    return prediction
 






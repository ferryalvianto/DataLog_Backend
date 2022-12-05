import pymongo
import pandas as pd
from sklearn.linear_model import LinearRegression
from sklearn.model_selection import train_test_split
import pickle
import time
import numpy as np


def save_model_to_db(db: str, cy, yyyy, mm, dd):
    myclient = pymongo.MongoClient(
        'mongodb+srv://DataLog:DataLog@cluster0.jzr1zc7.mongodb.net/')
    mydb = myclient[db]

    if db == 'BeFresh':
        mycol = mydb["df_sales"]
    else:
        mycol = mydb["Order_Item_Transaction"]

    transaction_list = []

    for x in mycol.find():
        transaction_list.append(x)

    df = pd.DataFrame(transaction_list)

   # only getting relevant Columns for Forecasting Quantity
    if db == 'BeFresh':
        rel_col = ["Category", "Quantity", "Date",
                   "Temperature", "Min_Temp_C_", "Max_Temp_C_", "year"]
    else:
        rel_col = ["Category", "Quantity", "Date",
                   "Temperature", "Min_Temp_C_", "Max_Temp_C_", "Year"]

    cy = cy[["Category", "Quantity", "Date", "Temperature",
             "Min_Temp_C_", "Max_Temp_C_", "year"]].copy()
    df = df[rel_col]

    df = df.append(cy, ignore_index=True)

    df = df.sort_values(by='Category', ascending=True)

    Categories = df.copy()

    Categories = Categories.Category.unique()

    Categories_final = []

    for cat in Categories:
        Categories_final.append(cat)

    df.dropna(inplace=True)

    if 'year' in df.columns:
        df.rename(columns={'year': 'Year'}, inplace=True)

    # creating column for daily quantity sold for a product
    df["daily_quantity_sold"] = df.groupby(['Category', 'Date'])[
        'Quantity'].transform('sum')

    # droping quantity column as it is not needed anymore
    df.drop(["Quantity", "Date"], axis=1, inplace=True)

    # removing duplicates, as many product in same date have now sililar info
    df.drop_duplicates(inplace=True)

    df = pd.get_dummies(df, drop_first=True)

    df.columns = df.columns.str.replace('Category_', '')

    X = df.drop("daily_quantity_sold", axis=1)
    Y = df.daily_quantity_sold

    linear_model = LinearRegression()
    xtrain, xtest, ytrain, ytest = train_test_split(
        X, Y, test_size=0.20, random_state=0)
    linear_model.fit(xtrain, ytrain)

    model = linear_model
    model_name = "my_linear_model"

    # pickling the model
    pickled_model = pickle.dumps(model)

    # collection for regression model
    dbconnection = 'regression_models'

    # creating collection
    mycon = mydb[dbconnection]
    info = mycon.insert_one({model_name: pickled_model,
                            'name': model_name,
                             'created_time': time.time(),
                             'latest_date_in_model': yyyy+'-'+mm+'-'+dd,
                             'categories': Categories_final})

    details = {
        'model_name': model_name,
        'created_time': time.time()
    }

    return details


def load_saved_model_from_db(db, weather_data):
    dbconnection = 'regression_models'
    model_name = "my_linear_model"

    json_data = {}

    # fetch model in mongodb
    myclient = pymongo.MongoClient(
        'mongodb+srv://DataLog:DataLog@cluster0.jzr1zc7.mongodb.net/')
    mydb = myclient[db]
    mycon = mydb[dbconnection]
    data = mycon.find({'name': model_name})

    for i in data:
        json_data = i

    pickled_model = json_data[model_name]

    linear_fetch = pickle.loads(pickled_model)

    # fetch category mongodb
    # if db == 'BeFresh':
    #     dbconnection_Order_Item_Transaction = "df_sales"
    # else:
    #     dbconnection_Order_Item_Transaction = "Order_Item_Transaction"

    # mycon = mydb[dbconnection_Order_Item_Transaction]
    # Categories = mycon.distinct("Category")

    Categories = json_data["categories"]

    Categories.pop(0)

    df = pd.DataFrame()

    df["Temperature"] = ""
    df["Min_Temp_C_"] = ""
    df["Max_Temp_C_"] = ""
    df["Year"] = ""
    df["Category"] = ""

    # creating columns for Categories
    for column in Categories:
        df[column] = column

    for column in Categories:
        for i in range(len(weather_data)):
            df = df.append({column: 1, 'Temperature': weather_data[i].temp_avg,
                            'Min_Temp_C_': weather_data[i].temp_min, 'Max_Temp_C_': weather_data[i].temp_max,
                            'Year': weather_data[i].dt_txt, 'Category': column
                            }, ignore_index=True)

    df.fillna(0, inplace=True)

    df_orig = df.copy()
    df.drop("Category", axis=1, inplace=True)

    df["Year"] = df["Year"].apply(lambda x: x[0:4])

    df = df.astype("int32")

    prediction = linear_fetch.predict(df)

    prediction = pd.DataFrame(prediction, columns=["predicted_quantity"])

    prediction["predicted_quantity"] = prediction["predicted_quantity"].apply(
        lambda x: abs(x))

    prediction['Category'] = df_orig.Category
    prediction['Date'] = df_orig.Year

    prediction = prediction.to_dict("records")

    return prediction


def load_saved_model_from_db_with_category(db, weather_data, category):
    dbconnection = 'regression_models'
    model_name = "my_linear_model"

    json_data = {}

    # fetch model in mongodb
    myclient = pymongo.MongoClient(
        'mongodb+srv://DataLog:DataLog@cluster0.jzr1zc7.mongodb.net/')
    mydb = myclient[db]
    mycon = mydb[dbconnection]
    data = mycon.find({'name': model_name})

    for i in data:
        json_data = i

    pickled_model = json_data[model_name]

    linear_fetch = pickle.loads(pickled_model)

    # fetch category mongodb
    # if db == 'BeFresh':
    #     dbconnection_Order_Item_Transaction = "df_sales"
    # else:
    #     dbconnection_Order_Item_Transaction = "Order_Item_Transaction"

    # mycon = mydb[dbconnection_Order_Item_Transaction]
    # Categories = mycon.distinct("Category")

    Categories = json_data["categories"]

    Categories.pop(0)

    df = pd.DataFrame()

    df["Temperature"] = ""
    df["Min_Temp_C_"] = ""
    df["Max_Temp_C_"] = ""
    df["Year"] = ""
    df["Category"] = ""

    # creating columns for Categories
    for column in Categories:
        df[column] = column

    for column in Categories:
        for i in range(len(weather_data)):
            df = df.append({column: 1, 'Temperature': weather_data[i].temp_avg,
                            'Min_Temp_C_': weather_data[i].temp_min, 'Max_Temp_C_': weather_data[i].temp_max,
                            'Year': weather_data[i].dt_txt, 'Category': column
                            }, ignore_index=True)

    df.fillna(0, inplace=True)
    df_orig = df.copy()
    df.drop("Category", axis=1, inplace=True)
    df["Year"] = df["Year"].apply(lambda x: x[0:4])
    df = df.astype("int32")

    prediction = linear_fetch.predict(df)
    prediction = pd.DataFrame(prediction, columns=["predicted_quantity"])
    prediction["predicted_quantity"] = prediction["predicted_quantity"].apply(
        lambda x: abs(x))
    prediction['Category'] = df_orig.Category
    prediction['Date'] = df_orig.Year
    prediction = prediction.loc[prediction.Category == category]
    prediction = prediction.to_dict("records")

    return prediction


# table
def load_saved_model_from_db_quantity_forecast_table(db, weather_data):
    dbconnection = 'regression_models'
    model_name = "my_linear_model"

    json_data = {}

    # fetch model in mongodb
    myclient = pymongo.MongoClient(
        'mongodb+srv://DataLog:DataLog@cluster0.jzr1zc7.mongodb.net/')
    mydb = myclient[db]
    mycon = mydb[dbconnection]
    data = mycon.find({'name': model_name})

    for i in data:
        json_data = i

    pickled_model = json_data[model_name]

    linear_fetch = pickle.loads(pickled_model)

    # fetch category mongodb
    # if db == 'BeFresh':
    #     dbconnection_Order_Item_Transaction = "df_sales"
    # else:
    #     dbconnection_Order_Item_Transaction = "Order_Item_Transaction"

    # mycon = mydb[dbconnection_Order_Item_Transaction]
    # Categories = mycon.distinct("Category")

    Categories = json_data["categories"]

    Categories.pop(0)

    df = pd.DataFrame()

    df["Temperature"] = ""
    df["Min_Temp_C_"] = ""
    df["Max_Temp_C_"] = ""
    df["Year"] = ""
    df["Category"] = ""

    # creating columns for Categories
    for column in Categories:
        df[column] = column

    for column in Categories:
        for i in range(len(weather_data)):
            df = df.append({column: 1, 'Temperature': weather_data[i].temp_avg,
                            'Min_Temp_C_': weather_data[i].temp_min, 'Max_Temp_C_': weather_data[i].temp_max,
                            'Year': weather_data[i].dt_txt, 'Category': column
                            }, ignore_index=True)

    df.fillna(0, inplace=True)

    df_orig = df.copy()
    df.drop("Category", axis=1, inplace=True)

    df["Year"] = df["Year"].apply(lambda x: x[0:4])

    df = df.astype("int32")

    prediction = linear_fetch.predict(df)

    prediction = pd.DataFrame(prediction, columns=["predicted_quantity"])

    prediction["predicted_quantity"] = prediction["predicted_quantity"].apply(
        lambda x: abs(x))

    prediction['Category'] = df_orig.Category
    prediction['Date'] = df_orig.Year

    Dates = prediction.Date.unique()

    prediction_test_transpose = prediction.predicted_quantity

    prediction_excel = pd.DataFrame(
        prediction_test_transpose.values.reshape(-1, 5), columns=Dates)

    category_list = np.array(Categories)

    prediction_excel.insert(0, 'Categories', category_list)

    prediction_excel = prediction_excel.to_dict("records")

    return prediction_excel

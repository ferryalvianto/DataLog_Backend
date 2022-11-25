from pydantic import BaseModel
from prophet import Prophet
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib as mpl
import matplotlib.patches as mpatches
import numpy as np
from datetime import datetime, timedelta
import seaborn as sns
import warnings
warnings.filterwarnings('ignore')
import holidays
from sklearn.metrics import mean_absolute_error, mean_absolute_percentage_error
import pymongo

plt.style.use('seaborn')
mpl.rcParams['axes.labelsize'] = 10
mpl.rcParams['axes.linewidth'] = 1
mpl.rcParams['lines.linewidth'] = 1
mpl.rcParams['lines.markersize'] = 4
mpl.rcParams['xtick.labelsize'] = 12
mpl.rcParams['xtick.minor.bottom'] = False
mpl.rcParams['ytick.labelsize'] = 12
mpl.rcParams['text.color'] = 'k'

# Color dictionary that we will use later
dayofweek_colors = {0: 'tab:blue', 1: 'tab:orange',
                    2: 'tab:green', 3: 'tab:red', 
                    4: 'tab:purple', 5: 'tab:brown', 
                    6: 'tab:pink'}

pd.set_option('display.max_columns', None)


client = pymongo.MongoClient('mongodb+srv://DataLog:DataLog@cluster0.jzr1zc7.mongodb.net/')


async def save_timeseries_to_db(db: str):
    mydb = client[db]
    mycol = mydb["df_sales"]
    
    transaction_list = []

    for x in mycol.find():
        transaction_list.append(x)

    df = pd.DataFrame(transaction_list)

    df = df[['Date','dailyRevenue']].copy()

    df = df.drop_duplicates(subset=['Date'], keep='last').reset_index(drop=True)
    df['Date'] = pd.to_datetime(df['Date'])

    df.rename(columns={'Date':'ds', 'dailyRevenue':'y'}, inplace=True)

    holidays.country_holidays('CA', prov='BC')
    df['holiday'] = pd.Series(df.ds).apply(lambda x: holidays.country_holidays('CA', prov='BC').get(x)).values

    events = df[['ds','holiday']].copy()
    events = events.dropna()

    # Data start date
    start_date = '2021-01-02'
    # Data end date
    end_date = '2021-12-31' 
    # Date for splitting training and testing dataset
    train_end_date = '2021-12-15'

    # Train test split
    train = df[df['ds'] <= train_end_date]
    test = df[df['ds'] > train_end_date]
    # Check the shape of the dataset
    print(train.shape)
    print(test.shape)

    # Add holidays
    model_holiday = Prophet(seasonality_prior_scale=1, seasonality_mode='additive',changepoint_prior_scale= 5, holidays_prior_scale =0.3, yearly_seasonality=True, weekly_seasonality=True, holidays=events)
    # Fit the model on the training dataset
    model_holiday.fit(train)
    # All the holidays and events
    model_holiday.train_holiday_names

    # Create the time range for the forecast
    future_holiday = model_holiday.make_future_dataframe(periods=16)
    # Fill the missing values with the previous value
    future_holiday = future_holiday.fillna(method='ffill')
    # Make prediction
    forecast_holiday = model_holiday.predict(future_holiday)

    # Visualize the forecast
    # model_holiday.plot(forecast_holiday)

    # Visualize the forecast components
    # model_holiday.plot_components(forecast_holiday)

    # Merge actual and predicted values
    performance_holiday = pd.merge(test, forecast_holiday[['ds', 'yhat', 'yhat_lower', 'yhat_upper']][-16:], on='ds')
    # Check MAE value
    performance_holiday_MAE = mean_absolute_error(performance_holiday['y'], performance_holiday['yhat'])
    # print(f'The MAE for the holiday/event model is {performance_holiday_MAE}')
    # Check MAPE value
    performance_holiday_MAPE = mean_absolute_percentage_error(performance_holiday['y'], performance_holiday['yhat'])
    # print(f'The MAPE for the holiday/event model is {performance_holiday_MAPE}')

    result = performance_holiday[['ds','y','yhat']]
    result.rename(columns={'ds':'date', 'y':'actualRevenue', 'yhat':'predictedRevenue'}, inplace=True)
    result

    return result
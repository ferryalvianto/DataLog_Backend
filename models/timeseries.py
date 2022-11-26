from prophet import Prophet
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib as mpl
import matplotlib.patches as mpatches
import numpy as np
from datetime import datetime, timedelta
import seaborn as sns
import holidays
from sklearn.metrics import mean_absolute_error, mean_absolute_percentage_error
import pymongo
import pickle
import time

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

client = pymongo.MongoClient('mongodb+srv://DataLog:DataLog@cluster0.jzr1zc7.mongodb.net/')

def save_timeseries_to_db(db: str):
    oa19 = pd.read_csv('https://drive.google.com/uc?id=1pdMJvWVmV8KLwpIkfBmD0yLZd1Zq9Hou')
    oa20 = pd.read_csv('https://drive.google.com/uc?id=1sd7UEHk8fhHk52W61aupnnxRBnP2cTxg')
    oa21 = pd.read_csv('https://drive.google.com/uc?id=11XGlbZknAqXxhB3D-ZDGC7kBWIYG-wLv')

    cy19 = pd.read_csv('https://drive.google.com/uc?id=1loqBDfWZQ96Z3-KoJSSzNb0mngGo-YQf')
    cy20 = pd.read_csv('https://drive.google.com/uc?id=1FMg0L0ia5pRqQzctqVQDJFxFQyXTPf6y')
    cy21 = pd.read_csv('https://drive.google.com/uc?id=1o0cvR0ZrhyA3Ekeoy9HF2J8IRamqeRb-')

    oa = pd.concat([oa19,oa20])
    oa = pd.concat([oa,oa21])

    cy = pd.concat([cy19,cy20])
    cy = pd.concat([cy,cy21])

    oa = oa[['Date','dailyRevenue']].copy()
    cy = cy[['Date','dailyRevenue']].copy()

    oa = oa.drop_duplicates(subset=['Date'], keep='last').reset_index(drop=True)
    cy = cy.drop_duplicates(subset=['Date'], keep='last').reset_index(drop=True)

    oa['Date'] = pd.to_datetime(oa['Date'])
    cy['Date'] = pd.to_datetime(cy['Date'])
    cy.rename(columns={'dailyRevenue':'CY_dailyRevenue'}, inplace=True)
    df = oa.merge(cy, on='Date', how='left')
    df.rename(columns={'Date':'ds', 'dailyRevenue':'y'}, inplace=True)
    df.groupby(df['ds'].dt.dayofweek).count()[['y']].rename(columns={'y': 'count'})

    # Data start date
    start_date = '2019-01-02'
    # Data end date
    end_date = '2021-12-31' 
    # Date for splitting training and testing dataset
    train_end_date = '2021-12-15'

    # Train test split
    train = df[df['ds'] <= train_end_date]
    test = df[df['ds'] > train_end_date]
    # Check the shape of the dataset
    print(train.shape)

    holidays.country_holidays('CA', prov='BC')

    df['holiday'] = pd.Series(df.ds).apply(lambda x: holidays.country_holidays('CA', prov='BC').get(x)).values
    events = df[['ds','holiday']].copy()
    events = events.dropna()

    # Add seasonality 
    model_holiday1 = Prophet(seasonality_prior_scale=0.03, seasonality_mode='additive',changepoint_prior_scale= 0.0015, holidays_prior_scale = 7.96, yearly_seasonality=True, weekly_seasonality=True, holidays=events)
    # Add regressor
    # Fit the model on the training dataset
    model_holiday1.fit(train)

    # Create the time range for the forecast
    future_holiday1 = model_holiday1.make_future_dataframe(periods=16)
    # Append the regressor values
    future_holiday1 = pd.merge(future_holiday1, df[['ds', 'CY_dailyRevenue']], on='ds', how='inner')

    # Make prediction
    forecast_holiday1 = model_holiday1.predict(future_holiday1)
    # Visualize the forecast
    # model_holiday1.plot(forecast_holiday1); 

    # Visualize the forecast components
    # model_holiday1.plot_components(forecast_holiday1);

    # Merge actual and predicted values
    performance_holiday1 = pd.merge(test, forecast_holiday1[['ds', 'yhat', 'yhat_lower', 'yhat_upper']][-16:], on='ds')
    # Check MAE value
    performance_holiday1_MAE = mean_absolute_error(performance_holiday1['y'], performance_holiday1['yhat'])
    print(f'The MAE for the multivariate model is {performance_holiday1_MAE}')
    # Check MAPE value
    performance_holiday1_MAPE = mean_absolute_percentage_error(performance_holiday1['y'], performance_holiday1['yhat'])
    print(f'The MAPE for the multivariate model is {performance_holiday1_MAPE}')

    result = performance_holiday1[['ds','y','yhat']]
    result.rename(columns={'ds':'date', 'y':'actualRevenue', 'yhat':'predictedRevenue'}, inplace=True)
    result

    pickled_model = pickle.dumps(performance_holiday1)

    #creating collection
    mydb = client[db]
    mycon = mydb['timeseries_models']
    info = mycon.insert_one({ 'timeseries_model': pickled_model, 
                             'name': 'timeseries_model', 
                            'created_time': time.time()})

    return 'test'
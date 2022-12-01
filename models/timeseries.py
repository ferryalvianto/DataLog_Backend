from prophet import Prophet
import pandas as pd
import holidays
from sklearn.metrics import mean_absolute_error, mean_absolute_percentage_error
import pymongo
import pickle
from datetime import datetime, timedelta

def save_timeseries_to_db(db: str, cy, oa, yyyy, mm, dd):
    client = pymongo.MongoClient('mongodb+srv://DataLog:DataLog@cluster0.jzr1zc7.mongodb.net/')
    mydb = client[db]
    col = mydb['df_sales']
    results_oa = col.find({"Establishment":0},{"_id": 0,"Date":1, "dailyRevenue":1})
    results_cy = col.find({"Establishment":1},{"_id": 0,"Date":1, "dailyRevenue":1})

    transaction_list_oa = []
    transaction_list_cy = []

    for x in results_oa:
        transaction_list_oa.append(x)
        
    for x in results_cy:
        transaction_list_cy.append(x)
        
    df_oa = pd.DataFrame(transaction_list_oa)
    df_cy = pd.DataFrame(transaction_list_cy)

    df_oa = df_oa.drop_duplicates(subset=['Date'], keep='last').reset_index(drop=True)
    df_cy = df_cy.drop_duplicates(subset=['Date'], keep='last').reset_index(drop=True)

    df_oa['Date'] = pd.to_datetime(df_oa['Date'])
    df_cy['Date'] = pd.to_datetime(df_cy['Date'])

    oa = oa[['Date','dailyRevenue']].copy()
    cy = cy[['Date','dailyRevenue']].copy()

    oa = oa.drop_duplicates(subset=['Date'], keep='last').reset_index(drop=True)
    cy = cy.drop_duplicates(subset=['Date'], keep='last').reset_index(drop=True)

    oa['Date'] = pd.to_datetime(oa['Date'])
    cy['Date'] = pd.to_datetime(cy['Date'])

    oa = oa.append(df_oa, ignore_index = True)
    cy = cy.append(df_cy, ignore_index = True)

    cy.rename(columns={'dailyRevenue':'CY_dailyRevenue'}, inplace=True)
    df = oa.merge(cy, on='Date', how='left')
    df.rename(columns={'Date':'ds', 'dailyRevenue':'y'}, inplace=True)
    df.groupby(df['ds'].dt.dayofweek).count()[['y']].rename(columns={'y': 'count'})

    start_date = '2019-01-02'
    end_date = yyyy+'-'+mm+'-'+dd 
    end_date = datetime.strptime(end_date, '%Y-%m-%d')
    train_end_date = end_date - timedelta(days=7)

    train = df[df['ds'] <= train_end_date]
    test = df[df['ds'] > train_end_date]

    holidays.country_holidays('CA', prov='BC')

    df['holiday'] = pd.Series(df.ds).apply(lambda x: holidays.country_holidays('CA', prov='BC').get(x)).values
    events = df[['ds','holiday']].copy()
    events = events.dropna()

    # OA TIMESERIES
    model_holiday1 = Prophet(daily_seasonality=False, seasonality_prior_scale=0.01, seasonality_mode='multiplicative',changepoint_prior_scale= 5, holidays_prior_scale = 1, holidays=events)
    model_holiday1.add_seasonality(name='monthly', period=30.5, fourier_order=10)
    model_holiday1.add_seasonality(name='weekly', period=7, fourier_order=15)
    model_holiday1.add_seasonality(name='yearly', period=365, fourier_order=25)
    model_holiday1.fit(df)

    future_holiday1 = model_holiday1.make_future_dataframe(periods=7)
    future_holiday1 = pd.merge(future_holiday1, df[['ds', 'CY_dailyRevenue']], on='ds', how='inner')
    forecast_holiday1 = model_holiday1.predict(future_holiday1)

    performance_holiday1 = pd.merge(test, forecast_holiday1[['ds', 'yhat', 'yhat_lower', 'yhat_upper']][-7:], on='ds')
    performance_holiday1_MAE = mean_absolute_error(performance_holiday1['y'], performance_holiday1['yhat'])
    performance_holiday1_MAPE = mean_absolute_percentage_error(performance_holiday1['y'], performance_holiday1['yhat'])

    result = performance_holiday1[['ds','y','yhat']]
    result.rename(columns={'ds':'date', 'y':'OA_actualRevenue', 'yhat':'OA_predictedRevenue'}, inplace=True)

    pickled_model = pickle.dumps(performance_holiday1)

    mydb = client[db]
    mycon = mydb['timeseries_models']
    mycon.insert_one({ 'timeseries_model': pickled_model, 
                            'name' : 'OA_timeseries', 
                            'MAPE' : performance_holiday1_MAPE,
                            'created_date': datetime.today().strftime('%Y-%m-%d'),
                            'latest_date_in_model': yyyy+'-'+mm+'-'+dd })

    # CY TIMESERIES
    df1 = cy.merge(oa, on='Date', how='left')
    df1.rename(columns={'Date':'ds', 'CY_dailyRevenue':'y', 'dailyRevenue':'OA_dailyRevenue'}, inplace=True)
    df1.groupby(df1['ds'].dt.dayofweek).count()[['y']].rename(columns={'y': 'count'})

    train = df1[df1['ds'] <= train_end_date]
    test = df1[df1['ds'] > train_end_date]

    df1['holiday'] = pd.Series(df1.ds).apply(lambda x: holidays.country_holidays('CA', prov='BC').get(x)).values
    events = df1[['ds','holiday']].copy()
    events = events.dropna()

    model_holiday = Prophet(daily_seasonality=False, seasonality_prior_scale=0.135, seasonality_mode='multiplicative',changepoint_prior_scale= 0.039, holidays_prior_scale = 0.011, holidays=events)
    model_holiday.add_seasonality(name='monthly', period=30.5, fourier_order=10)
    model_holiday.add_seasonality(name='weekly', period=7, fourier_order=1)
    model_holiday.add_seasonality(name='yearly', period=365, fourier_order=3)
    model_holiday.fit(df1)

    future_holiday = model_holiday.make_future_dataframe(periods=7)
    future_holiday = pd.merge(future_holiday, df1[['ds', 'OA_dailyRevenue']], on='ds', how='inner')

    forecast_holiday = model_holiday.predict(future_holiday)

    performance_holiday = pd.merge(test, forecast_holiday[['ds', 'yhat', 'yhat_lower', 'yhat_upper']][-7:], on='ds')
    performance_holiday_MAE = mean_absolute_error(performance_holiday['y'], performance_holiday['yhat'])
    performance_holiday_MAPE = mean_absolute_percentage_error(performance_holiday['y'], performance_holiday['yhat'])

    result1 = performance_holiday[['ds','y','yhat']]
    result1.rename(columns={'ds':'date', 'y':'CY_actualRevenue', 'yhat':'CY_predictedRevenue'}, inplace=True)

    pickled_model1 = pickle.dumps(performance_holiday)

    mydb = client[db]
    mycon = mydb['timeseries_models']
    mycon.insert_one({ 'timeseries_model': pickled_model1, 
                            'name' : 'CY_timeseries',
                            'MAPE' : performance_holiday_MAPE,
                            'created_date': datetime.today().strftime('%Y-%m-%d'),
                            'latest_date_in_model': yyyy+'-'+mm+'-'+dd })

    results = result.merge(result1, on='date', how='left')

    # PREDICTIONS
    future_OA = model_holiday1.make_future_dataframe(periods=10)
    forecast_OA = model_holiday1.predict(future_OA)
    forecast_OA = forecast_OA[['ds', 'yhat']][-10:].copy()
    forecast_OA.rename(columns={'ds':'date', 'yhat':'OA_predictedRevenue'}, inplace=True)

    future_CY = model_holiday.make_future_dataframe(periods=10)
    forecast_CY = model_holiday.predict(future_CY)
    forecast_CY = forecast_CY[['ds', 'yhat']][-10:].copy()
    forecast_CY.rename(columns={'ds':'date', 'yhat':'CY_predictedRevenue'}, inplace=True)

    preds = forecast_CY.merge(forecast_OA, on='date', how='left')
    preds['latest_date_in_model'] = yyyy+'-'+mm+'-'+dd
    preds = preds.to_dict("records")
    coll = mydb['revenue_forecast']
    coll.insert_many(preds)
    
    return 'OA Timeseries MAE is ±$' + str(round(performance_holiday1_MAE, 2)) + ' (±' + '{:.2%}'.format(performance_holiday1_MAPE) + ').\nCY Timeseries MAE is ±$' + str(round(performance_holiday_MAE, 2)) + ' (±' + '{:.2%}'.format(performance_holiday_MAPE) + ').'


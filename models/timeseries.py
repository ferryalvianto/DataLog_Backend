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


# async def save_timeseries_to_db(db: str):
#     mydb = client[db]

 #only getting relevant Columns for Forecasting Quantity

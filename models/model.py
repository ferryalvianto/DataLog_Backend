# Pydantic allows auto creation of JSON Schemas from models
from sqlite3 import Date
from pydantic import BaseModel
from typing import Union

class Token(BaseModel):
    access_token: str
    token_type: str
    user: dict

class TokenData(BaseModel):
    username: Union[str, None] = None
    db: Union[str, None] = None

class User(BaseModel):
    username: str
    password: str
    firstname: Union[str, None] = None
    lastname: Union[str, None] = None
    email: Union[str, None] = None
    newuser: Union[bool, None] = None
    disabled: Union[bool, None] = None
    db: Union[str, None] = None


class UserInDB(User):
    password: str

class DailyRevenueForecast(BaseModel):
    Date: str
    PredictedRevenue: int
    Flag_latest: str

#didn't really use, for noww
class Wastage(BaseModel):
    Category: str
    Product_Name: str
    Quantity: int
    Date: str

class Sentiments(BaseModel):
    Classification: str
    Rating: str
    Comment: str
    Date: str

class Revenue(BaseModel):
    ymd: str
    dailyRevenue: int

class ProductQuantityForecast(BaseModel):
    Date: str
    Product_Name: str
    PredictedQuantity: int
    Temperature: int
    Day: int
    Month: int
    Year: int

class WeatherForecast(BaseModel):
    temp_avg: int
    temp_min: int
    temp_max: int
    dt_txt: str 


class ML_Model(BaseModel):
    myxgb: object
    name: str


class RevenueMaxDate(BaseModel):
    ymd: str
    revenue: int

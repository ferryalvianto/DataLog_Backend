import pymongo
from fastapi import Depends, HTTPException, status
from fastapi.security import OAuth2PasswordBearer
from jose import jwt, JWTError
from passlib.context import CryptContext
from datetime import datetime, timedelta
from typing import Union
from models.model import TokenData, UserInDB, User

def get_auth_list():
    client = pymongo.MongoClient('mongodb+srv://DataLog:DataLog@cluster0.jzr1zc7.mongodb.net/')
    auth_db = client['authentication']
    auth_col = auth_db['authentication']
    auth_cursor = auth_col.find({}, {'_id': 0})
    auth_lists = list(auth_cursor)
    auth_lists = auth_lists[0]
    return auth_lists

def get_secret_key():
    auth_lists = get_auth_list()
    return auth_lists.get('SECRET_KEY')

def get_algorithm():
    auth_lists = get_auth_list()
    return auth_lists.get('ALGORITHM')

def get_access_token():
    auth_lists = get_auth_list()
    return auth_lists.get('ACCESS_TOKEN_EXPIRE_MINUTES')

def get_db_names():
    client = pymongo.MongoClient('mongodb+srv://DataLog:DataLog@cluster0.jzr1zc7.mongodb.net/')
    lists = client.list_database_names()
    lists.remove('admin')
    lists.remove('authentication')
    lists.remove('local')
    return {'names':lists}

def create_access_token(data: dict, expires_delta: Union[timedelta, None] = None):
    SECRET_KEY = get_secret_key()
    ALGORITHM = get_algorithm()
    to_encode = data.copy()
    if expires_delta:
        expire = datetime.utcnow() + expires_delta
    else:
        expire = datetime.utcnow() + timedelta(minutes=15)
    to_encode.update({"exp": expire})
    encoded_jwt = jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)
    return encoded_jwt

async def get_current_user(token: str = Depends(OAuth2PasswordBearer(tokenUrl="token"))):
    SECRET_KEY = get_secret_key()
    ALGORITHM = get_algorithm()
    credentials_exception = HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Could not validate credentials",
        headers={"WWW-Authenticate": "Bearer"},
    )
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        username: str = payload.get('username')
        db: str = payload.get('db')
        if username is None:
            raise credentials_exception
        token_data = TokenData(username=username, db=db)
    except JWTError:
        raise credentials_exception
    
    client = pymongo.MongoClient('mongodb+srv://DataLog:DataLog@cluster0.jzr1zc7.mongodb.net/')
    mydb = client[token_data.db]
    mycol = mydb["users"]
    cursor = mycol.find({}, {'_id': 0})
    users = list(cursor)

    user_found = next((u for u in users if u['username'] == token_data.username), {})

    if token_data.username in user_found.get('username'):
        user_dict = user_found
        user = UserInDB(**user_dict)
    else:
        raise credentials_exception
    return user

async def get_current_active_user(current_user: User = Depends(get_current_user)):
    if current_user.disabled:
        raise HTTPException(status_code=400, detail="Inactive user")
    return current_user

async def update_user_db(username: str, db:str, data: User):
    client = pymongo.MongoClient('mongodb+srv://DataLog:DataLog@cluster0.jzr1zc7.mongodb.net/')
    mydb = client[db]
    mycol = mydb["users"]
    student = mycol.find_one({"username": username})
    pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")
    if student:
        data['password'] = pwd_context.hash(data['password'])
        updated_student = mycol.update_one(
            {"username": username}, {"$set": data}
        )
        if updated_student:
            return True
        return False


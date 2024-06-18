from pymongo import MongoClient
from datetime import datetime
import pandas as pd
from datetime import date

def get_triple_witching_days(start_year, end_year):
    triple_witching_days = []
    for year in range(start_year, end_year + 1):
        for month in [3, 6, 9, 12]:
            # Obtain the date of every month
            month_dates = pd.date_range(start=f'{year}-{month}-01', end=f'{year}-{month}-28')
            # Find the third Friday
            third_friday = month_dates[month_dates.weekday == 4][2]
            triple_witching_days.append(third_friday)
    return triple_witching_days


def add_triple_witching(db_name, 
                       collection_name, 
                       mongo_uri):
    
    today = date.today()
    start_year = today.year - 4
    end_year = today.year + 4

    triple_witching_days = get_triple_witching_days(start_year, end_year)

    triple_witching_days_dicts = [
    {"date": day.to_pydatetime(), "description": "triple witching day"} 
    for day in triple_witching_days
    ]


    client = MongoClient(mongo_uri)
    db = client[db_name]   
    collection = db[collection_name]
    collection.delete_many({})

    collection.insert_many(triple_witching_days_dicts)

    print('Triple Witching dates successfully added')


if __name__ == '__main__':
    today = date.today()
    print(today)

    add_triple_witching('intern_tmp', 
                   'Triple_witching', 
                   mongo_uri="mongodb://intern:password0711@161.117.176.172:27018/intern_tmp")
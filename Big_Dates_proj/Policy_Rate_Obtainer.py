from pymongo import MongoClient
from datetime import datetime
import pandas as pd
from Big_Date_API import *

def future_data():
    future_dates = obtain_all_future_output()
    valid_data = []

    for ticker, entry in future_dates.items():
        if None in entry.values():
            print(f"Invalid entry for {ticker}: {entry}")
        else:
            valid_data.append({
                "ticker": ticker,
                "date": entry["date"],
                "country": entry["country"],
                "description": entry["description"],
                "timezone": entry["timezone"]
            })

    df = pd.DataFrame(valid_data)

    return df

def future_data_insert(db_name, 
                       collection_name, 
                       mongo_uri):
    future_dates = future_data()
    
    df = future_dates

    required_columns = ['date', 'country', 'description', 'timezone']
    for col in required_columns:
        if col not in df.columns:
            raise ValueError(f"DataFrame Loss the Column: {col}")
    
    df['date'] = df['date'].apply(lambda x: datetime.datetime.combine(x, datetime.datetime.min.time()) if isinstance(x, datetime.date) else x)
    
    client = MongoClient(mongo_uri)
    db = client[db_name]
    collection = db[collection_name]
    

    # Delete records where date is after today
    today = datetime.datetime.combine(datetime.date.today(), datetime.datetime.min.time())
    delete_result = collection.delete_many({'date': {'$gt': today}})
    print(f"Deleted {delete_result.deleted_count} records")

    # Insert new records
    inserted_count = 0
    for record in df.to_dict(orient='records'):
        if record['date'] >= today:
            collection.insert_one(record)
            inserted_count += 1
    
    print(f"Successfully inserted {inserted_count} records")
    
if __name__ == '__main__':
    today = datetime.date.today()
    print(today)

    future_data_insert('intern_tmp', 
                   'rate_release_dates', 
                   mongo_uri="mongodb://intern:password0711@161.117.176.172:27018/intern_tmp")
    
    
    
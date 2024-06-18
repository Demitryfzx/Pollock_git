## Big_Date_API

This project is designed to retrieve policy rates from various central banks around the world using specified economic data sources. The project is divided into two main parts: future release dates and historical release dates. By using parallel processing, it efficiently fetches policy rate release information for multiple central banks.

It is designed to obtain policy rates information from 27 countries, although only 15 countries turns out to have valid information

### Fetching Future Policy Rate Release Dates

- To fetch a single future policy rate release dates (The index name can be "FDTR Index", etc):

```
retrive_single_futureversion(index_name)
```

- To fetch all future policy rate release dates:

```
obtain_all_future_output()
```

### Fetching Historical Policy Rate Release Dates

- To fetch a single historical policy rate release dates (The index name can be "FDTR Index", etc):

```
retrive_single_historicalversion(index_name)
```

- To fetch all historical policy rate release dates:

```
obtain_all_historical_output()
```

### Example Output

After running the script, you will see output similar to the following:

The output below mentions what ticker doesn't provide the exact rate release date
```
This Ticker doesn't provide meeting dates
This Ticker doesn't provide meeting dates
CHLR12M generated an exception: 'NoneType' object is not iterable
DEBRDISC generated an exception: 'NoneType' object is not iterable
This Ticker doesn't provide meeting dates
SRREPO generated an exception: 'NoneType' object is not iterable
This Ticker doesn't provide meeting dates
This Ticker doesn't provide meeting dates
This Ticker doesn't provide meeting dates
HKBASE generated an exception: 'NoneType' object is not iterable
ICBRANN generated an exception: 'NoneType' object is not iterable
QAIRONLR generated an exception: 'NoneType' object is not iterable
This Ticker doesn't provide meeting dates
This Ticker doesn't provide meeting dates
This Ticker doesn't provide meeting dates
VNREFINC generated an exception: 'NoneType' object is not iterable
EGBRDR generated an exception: 'NoneType' object is not iterable
CUAEBASE generated an exception: 'NoneType' object is not iterable
```

The output below mentions that the requires information is already updated to MongoDB
```
Successful Insertion
```

### Code Structure

- `TICKERS`: List of all tickers to be fetched.
- `COUNTRY_TO_CENTRALBANK`: Maps country names to their respective central banks.
- `TICKER_TO_COUNTRY`: Maps tickers to country names.
- `ticker_to_index(ticker)`: Converts a ticker to an index name.
- `country_name_standardization(name)`: Standardizes country names to fit pytz package.
- `obtain_timezone(index_name)`: Obtains the timezone of the given index.
- `retrive_single_futureversion(index_name, checker=False)`: Retrieves future release dates for a single ticker.
- `obtain_single_future_output(ticker)`: Fetches future release output for a single ticker.
- `obtain_all_future_output()`: Fetches future release output for all tickers.
- `retrive_single_historicalversion(index_name, checker=False)`: Retrieves historical release dates for a single ticker.
- `obtain_single_historical_output(ticker)`: Fetches historical release output for a single ticker.
- `obtain_all_historical_output()`: Fetches historical release output for all tickers.

### MongoDB data format (example)
data = {
    "date": datetime(2024, 6, 14),
    "instrument_code": "BOJDTR Index",
    "description": "BOJ rate release",
    "timezone": "Japan"
}


## Policy_Rate_Obtainer
This script consists of two main functions that work together to fetch future policy rate release data and store it in a MongoDB collection. The script is designed to run daily. It is based on the Big_Date_API mentioned above

#### `future_data()`

- **Purpose**: Fetches future policy rate release dates from the `Big_Date_API` and verifies the validity of the data.
- **Returns**: A Pandas DataFrame containing valid future policy rate release dates with columns: `ticker`, `date`, `country`, `description`, `timezone`.

#### `future_data_insert(db_name, collection_name, mongo_uri)`

- **Purpose**: Transforms the `date` field from `datetime.date` to `datetime.datetime`, deletes existing records in MongoDB where the date is after today, and inserts new valid records.
- **Parameters**:
  - `db_name` (str): The name of the MongoDB database.
  - `collection_name` (str): The name of the MongoDB collection.
  - `mongo_uri` (str): The MongoDB connection URI.

## 




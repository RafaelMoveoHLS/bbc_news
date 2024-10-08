import pandas as pd
from typing import Any, Dict
from pymongo import MongoClient
from fastapi import FastAPI, HTTPException
from contextlib import asynccontextmanager
from validate import QueryModel

app = FastAPI()

# Lifespan event handler
# @asynccontextmanager
# async def lifespan(app: FastAPI):
#     # Startup actions
#     load_data()
#     yield
#     # Add any shutdown actions here if needed

try:
    # MongoDB setup
    client = MongoClient("mongodb://localhost:27017/")
    db = client["task1"]
    collection = db["bbc_news"]
    print("\033[95m Connected to MongoDB \033[0m")
except Exception as e:
    raise RuntimeError(f"Error connecting to MongoDB: {str(e)}")


def load_bbc_news_data(file_path: str) -> pd.DataFrame:
    """
    Load BBC news data from a CSV file into a pandas DataFrame.

    Args:
        file_path (str): The file path to the CSV file.

    Returns:
        pd.DataFrame: The loaded DataFrame with the news data.
    """
    try:
        df = pd.read_csv(file_path)
        return df
    except Exception as e:
        raise RuntimeError(f"Error loading the data: {str(e)}")


def filter_news_first_half_2024(df: pd.DataFrame) -> pd.DataFrame:
    """
    Filter the news data for the first half of 2024.

    Args:
        df (pd.DataFrame): The DataFrame containing the news data.

    Returns:
        pd.DataFrame: The filtered DataFrame with news from the first half of 2024.
    """
    try:
        # Convert 'date' column to datetime if not already
        df['pubDate'] = pd.to_datetime(df['pubDate'])

        # Filter news for the first half of 2024
        start_date = pd.to_datetime('2024-01-01')
        end_date = pd.to_datetime('2024-06-30')
        filtered_df = df[(df['pubDate'] >= start_date)
                         & (df['pubDate'] < end_date)]
        return filtered_df

    except Exception as e:
        raise RuntimeError(f"Error filtering the data: {str(e)}")


def get_mongo_client(uri: str = "mongodb://localhost:27017/") -> MongoClient:
    """
    Establish a connection to a MongoDB client.

    Args:
        uri (str, optional): MongoDB connection string. Defaults to "mongodb://localhost:27017/".

    Returns:
        MongoClient: The MongoDB client.
    """
    try:
        client = MongoClient(uri)
        return client
    except Exception as e:
        raise RuntimeError(f"Error connecting to MongoDB: {str(e)}")


def insert_news_to_mongo(collection: any, data: pd.DataFrame) -> None:
    """
    Inserts the filtered news data into the MongoDB collection.

    Args:
        collection (MongoClient.database.collection): The collection name.
        data (pd.DataFrame): The filtered news DataFrame.
    """
    try:
        news_dict = data.to_dict(orient='records')

        # Insert data into collection
        if news_dict:
            collection.insert_many(news_dict)
            print(
                f"\033[92m Data loaded successfully into MongoDB.\033[0m [{len(news_dict)} documents]")
        else:
            raise ValueError("No data to insert.")
    except Exception as e:
        raise RuntimeError(f"Error inserting data into MongoDB: {str(e)}")


@app.post("/count")
async def count_rows(query: QueryModel):
    """
    API route that receives a JSON payload with key-value pairs 
    and returns the count of rows in MongoDB that match the query.

    Args:
        query (QueryModel): The query parameters in the request body.

    Returns:
        dict: A dictionary containing the count of matching rows.
    """
    try:
        # Convert the query model to a dictionary, excluding None values
        query_dict = query.model_dump(exclude_none=True)

        # Modify query to use regular expressions for partial matches
        regex_query = {}
        for key, value in query_dict.items():
            # Use MongoDB regex to search for values like SQL's LIKE '%value%'
            regex_query[key] = {"$regex": value, "$options": "i"} # "i" for case-insensitive

        # Perform the MongoDB query
        count = collection.count_documents(regex_query)

        return {"count": count}

    except Exception as e:
        raise HTTPException(
            status_code=500, detail=f"Error querying database: {str(e)}")


@app.on_event("startup")
def load_data():
    """
    Load BBC news data into MongoDB if the collection is empty.
    """
    # Check if collection is empty
    if collection.count_documents({}) == 0:
        print("\033[93m Loading data from CSV to MongoDB... \033[0m")
        df = load_bbc_news_data('bbc_news_data/bbc_news.csv')
        filtered_df = filter_news_first_half_2024(df)
        insert_news_to_mongo(collection, data=filtered_df)
    else:
        print("\033[96m No data to load. MongoDB collection is not empty. \033[0m")

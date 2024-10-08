import pandas as pd
from typing import Any, Dict
from pymongo import MongoClient
from fastapi import FastAPI, HTTPException
from contextlib import asynccontextmanager
from validate import QueryModel
from services.logger import get_logger

# Create a logger instance
logger = get_logger()

# Lifespan event handler
@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup actions
    load_data()
    yield
    # Add any shutdown actions here if needed

app = FastAPI(lifespan=lifespan)

try:
    # MongoDB setup
    client = MongoClient("mongodb://localhost:27017/")
    db = client["task1"]
    collection = db["bbc_news"]
    logger.info("Connected to MongoDB")
except Exception as e:
    logger.critical(f"Error connecting to MongoDB: {str(e)}")
    raise RuntimeError(f"Error connecting to MongoDB: {str(e)}")

def load_data()->None:
    """
    Load BBC news data into MongoDB if the collection is empty.
    """
    # Check if collection is empty
    if collection.count_documents({}) == 0:
        logger.info("Loading data from CSV to MongoDB...")
        df = load_bbc_news_data('bbc_news_data/bbc_news.csv')
        filtered_df = filter_news_first_half_2024(df)
        insert_news_to_mongo(collection, data=filtered_df)
    else:
        logger.info("No data to load. MongoDB collection is not empty.")

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
        logger.critical(f"Error loading the data: {str(e)}")
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
        logger.critical(f"Error filtering the data: {str(e)}")
        raise RuntimeError(f"Error filtering the data: {str(e)}")


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
            logger.info(
                f"Data loaded successfully into MongoDB-> [{len(news_dict)} documents]")
        else:
            raise ValueError("No data to insert.")
    except Exception as e:
        logger.critical(f"Error inserting data into MongoDB: {str(e)}")
        raise RuntimeError(f"Error inserting data into MongoDB: {str(e)}")


@app.post("/count")
async def count_rows(query: QueryModel)-> Dict[str, int]:
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
        logger.error(status_code=500, detail=f"Error querying database: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Error querying database: {str(e)}")
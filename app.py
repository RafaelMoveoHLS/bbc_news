import pandas as pd
from typing import Any
from pymongo import MongoClient

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
        filtered_df = df[(df['pubDate'] >= start_date) & (df['pubDate'] < end_date)]
        return filtered_df
    
    except Exception as e:
        raise RuntimeError(f"Error filtering the data: {str(e)}")


def get_mongo_client(uri: str="mongodb://localhost:27017/" ) -> MongoClient:
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


def insert_news_to_mongo(db_name: str, collection_name: str, data: pd.DataFrame, client: MongoClient) -> None:
    """
    Inserts the filtered news data into the MongoDB collection.
    
    Args:
        db_name (str): The database name.
        collection_name (str): The collection name.
        data (pd.DataFrame): The filtered news DataFrame.
        client (MongoClient): The MongoDB client.
    """
    try:
        db = client[db_name]
        collection = db[collection_name]
        
        # Convert DataFrame to dictionary
        news_dict = data.to_dict(orient='records')
        
        # Insert data into collection
        if news_dict:
            collection.insert_many(news_dict)
        else:
            raise ValueError("No data to insert.")
    except Exception as e:
        raise RuntimeError(f"Error inserting data into MongoDB: {str(e)}")


if __name__ == "__main__":
    # Load the data
    df = load_bbc_news_data('bbc_news_data/bbc_news.csv')

    # Filter the news
    filtered_df = filter_news_first_half_2024(df)

    # Connect to MongoDB
    client = get_mongo_client()

    # Insert the filtered data into MongoDB
    insert_news_to_mongo(db_name="task1", collection_name="bbc_news", data=filtered_df, client=client)

    print("News data inserted successfully into MongoDB.")
from services.logger import get_logger
import pandas as pd
from pymongo.collection import Collection
from services.mongo_operations import insert_news_to_mongo


logger = get_logger()

def load_data(collection: Collection)->None:
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
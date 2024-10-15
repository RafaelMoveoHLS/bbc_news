from managers.news_manager import NewsManager
from services.exeptions import NewsLoadingError
from services.logger import get_logger
import pandas as pd


logger = get_logger()

news_manager = NewsManager()

def load_data()->None:
    """
    Load BBC news data into MongoDB if the collection is empty.
    """
    try:
        # Check if collection is not empty
        if news_manager.collection.count_documents({}) != 0:
            logger.info("No data to load. MongoDB collection is not empty.")
        else:
            # Load news from CSV to MongoDB
            logger.info("Loading data from CSV to MongoDB...")
            df = pd.read_csv('bbc_news_data/bbc_news.csv')
            
            # Filter the news data for the first half of 2024.
            df['pubDate'] = pd.to_datetime(df['pubDate'])
            start_date = pd.to_datetime('2024-01-01')
            end_date = pd.to_datetime('2024-06-30')
            filtered_df = df[(df['pubDate'] >= start_date)
                            & (df['pubDate'] < end_date)]

            # Drop duplicates based on 'title' or 'description'
            filtered_df = filtered_df.drop_duplicates(subset=['title'], keep='first')
            filtered_df = filtered_df.drop_duplicates(subset=['description'], keep='first')

            # Insert filtered news data into MongoDB
            news_dict = filtered_df.to_dict(orient='records')
            news_manager.insert_many(news_dict)
            logger.info(f"Data loaded successfully into MongoDB-> [{len(news_dict)} documents]")
    except Exception as e:
        logger.critical(f"Error loading data: {str(e)}")
        raise NewsLoadingError(f"Error loading data: {str(e)}")
    
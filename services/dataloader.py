from managers.news_manager import NewsManager
from services.exeptions import NewsLoadingError
from services.logger import get_logger
from services.openai_service import embed_with_openai_batched
import pandas as pd

logger = get_logger()
news_manager = NewsManager()

def load_data()->None:
    """
    Load BBC news data from a CSV file into MongoDB if the collection is empty. 
    The news data is filtered to include only the first half of 2024 and enriched with OpenAI embeddings.

    Raises:
        NewsLoadingError: If there is an issue during data loading.
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
            filtered_df = df[(df['pubDate'] >= pd.to_datetime('2024-01-01'))
                            & (df['pubDate'] < pd.to_datetime('2024-06-30'))]

            # Drop duplicates based on 'title' or 'description'
            filtered_df = filtered_df.drop_duplicates(subset=['title'], keep='first')
            filtered_df = filtered_df.drop_duplicates(subset=['description'], keep='first')

            # Add embeddings to the news data using OpenAI API
            filtered_df = add_embeddings(filtered_df)

            # Insert filtered news data into MongoDB
            news_dict = filtered_df.to_dict(orient='records')
            news_manager.insert_many(news_dict)
            logger.info(f"Data loaded successfully into MongoDB-> [{len(news_dict)} documents]")
    except Exception as e:
        logger.critical(f"Error loading data: {str(e)}")
        raise NewsLoadingError(f"Error loading data: {str(e)}")

def add_embeddings(df:pd.DataFrame)->pd.DataFrame:
    """
    Add OpenAI embeddings to the news data.

    Args:
        df (pd.DataFrame): DataFrame containing news articles.

    Returns:
        pd.DataFrame: DataFrame with an additional 'openai_embedding' column.
    """
    df['content'] = df['title'].fillna('') + ' ' + df['description'].fillna('')
    logger.info("Turning for OpenAI API for the embeddings")
    df['openai_embedding'] = embed_with_openai_batched(df['content'].tolist(), 1000)
    logger.info("Successfully retrieved embeddings from OpenAI API.")
    return df
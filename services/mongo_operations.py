from fastapi import HTTPException
from services.logger import get_logger
import pandas as pd
from pymongo.collection import Collection
from services.validator import QueryModel


logger = get_logger()

def insert_news_to_mongo(collection: Collection, data: pd.DataFrame) -> None:
    """
    Inserts the filtered news data into the MongoDB collection.

    Args:
        collection (pymon.collection): The collection name.
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
    
def count_documents_in_mongo(collection: Collection, query: QueryModel) -> int:
    """
    Count documents in MongoDB that match the given query.
    
    Args:
        query (QueryModel): The query parameters in the request body.
        collection (pymon.collection): The collection name.

    Returns:
        int: Count of matching rows
    """
    try:
        query_dict = query.model_dump(exclude_none=True)
        regex_query = {key: {"$regex": value, "$options": "i"} for key, value in query_dict.items()}
        return collection.count_documents(regex_query)
    except Exception as e:
        logger.error(status_code=500, detail=f"Error querying database: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Error querying database: {str(e)}")
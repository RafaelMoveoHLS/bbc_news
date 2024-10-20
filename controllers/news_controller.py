from typing import Any, Dict
from fastapi import HTTPException
from handlers.news_handler import NewsHandler
from services.logger import get_logger
from validators.news_validator import NewsQueryModel

logger = get_logger()

class NewsController:
    """ The NewsController class manages the business logic for news-related operations. """
    def __init__(self):
        self.handler: NewsHandler = NewsHandler()

    def count_matching_news(self, query: NewsQueryModel) -> int:
        """
        Count news that match the given query.
        
        Args:
            query (NewsQueryModel): The query parameters in the request body.

        Returns:
            int: Count of matching news
        """
        try:
            return self.handler.count_matching_news(query)
        except Exception as e:
            logger.error(msg=f"Status code:{500}. Error querying database: {str(e)}")
            raise HTTPException(status_code=500, detail=f"Error querying database: {str(e)}")
    
    def semantic_news_search(self, query: str) -> Dict[str, Any]:
        """
        Retrieve related news articles based on semantic similarity.
        
        Args:
            query (str): The search query passed as a query parameter.

        Returns:
            Dict[str, Any]: List of related news articles
        """
        try:
            return self.handler.semantic_news_search(query)
        except Exception as e:
            logger.error(msg=f"Status code:{500}. Error while searching the news: {str(e)}")
            raise HTTPException(status_code=500, detail=f"Error while searching the news: {str(e)}")
    
from fastapi import HTTPException
from handlers.news_handler import NewsHandler
from services.logger import get_logger
from validators.news_validator import NewsQueryModel

logger = get_logger()

class NewsController:
    """ The NewsController class manages the business logic for news-related operations. """
    def __init__(self):
        self.handler = NewsHandler()

    def count_matching_news(self, query: NewsQueryModel) -> int:
        """
        Count news that match the given query.
        
        Args:
            query (NewsQueryModel): The query parameters in the request body.

        Returns:
            int: Count of matching news
        """
        try:
            query_dict = query.model_dump(exclude_none=True)
            count = self.handler.count_matching_news(query_dict)
            return {"count": count}

        except Exception as e:
            logger.error(msg=f"Status code:{500}. Error querying database: {str(e)}")
            raise HTTPException(status_code=500, detail=f"Error querying database: {str(e)}")
    
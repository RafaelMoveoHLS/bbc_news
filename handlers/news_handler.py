from handlers.main_handler import Handler
from managers.news_manager import NewsManager
from validators.news_validator import NewsQueryModel


class NewsHandler(Handler):
    """
    A class to handle news data and perform various operations.
    """
    manager = NewsManager()

    def __init__(self):
        super().__init__()

    def count_matching_news(self, query: NewsQueryModel) -> object:
        """
        Count news that match the given query.
        
        Args:
            query (NewsQueryModel): The query parameters in the request body.

        Returns:
            object: The response body object containing the count of matching news.
        """
        query_dict = query.model_dump(exclude_none=True)
        return {"count": self.manager.count_matching_rows(query_dict)}




from handlers.main_handler import Handler
from managers.news_manager import NewsManager


class NewsHandler(Handler):
    """
    A class to handle news data and perform various operations.
    """
    manager = NewsManager()

    def __init__(self):
        super().__init__()

    def count_matching_news(self, query_dict:object) -> int:
        """
        Count news that match the given query.
        
        Args:
            query (object): The query dictionary that was in the request body.

        Returns:
            int: Count of matching news
        """
        return self.manager.count_matching_rows(query_dict)




from managers.main_manager import Manager
from pymongo.collection import Collection
from pymongo.database import Database

class NewsManager(Manager):
    """
    A class to manage news data and perform various operations.
    """
    db_name = "task1"
    collection_name = "bbc_news"
    
    def __init__(self):
        super().__init__()
        



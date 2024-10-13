from abc import abstractmethod
from pymongo import MongoClient
from pymongo.collection import Collection
from pymongo.database import Database

class Manager:
    """
    The main handler class
    """
    
    client: MongoClient = None

    @property
    @abstractmethod
    def db(self) -> Database:
        pass
    
    @abstractmethod
    def collection(self) -> Collection:
        pass

    def __init__(self):
        # TODO: Change the connect string to be taken from the env files.
        self.client = MongoClient("mongodb://localhost:27017/")
    
    def count_matching_rows(self, query_dict: object) -> int:
        """
        Count rows in the collection that match the given query.
        
        Args:
            query_dict (object): The query dictionary that was in the request body.

        Returns:
            int: Count of matching rows
        """
        regex_query = {key: {"$regex": value, "$options": "i"} for key, value in query_dict.items()}
        # print(regex_query)
        return self.collection.count_documents(regex_query)
    
    def insert_many(self, data_dict: list[dict]) -> None:
        """
        Insert multiple documents into the collection.
        
        Args:
            data_dict (list[dict]): The list of dictionaries to be inserted.
        """
        self.collection.insert_many(data_dict)
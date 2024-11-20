import time
from typing import Any, Dict, List
from managers.main_manager import Manager
from services.exeptions import MongoDBVectorSearchError
from services.logger import get_logger

logger = get_logger()


class NewsManager(Manager):
    """
    A class to manage news data and perform various operations.
    """

    def __init__(self):
        super().__init__(db_name="task1", collection_name="bbc_news")

    def get_top_n_related_rows(self, query_embedding: List[float], top_n: int) -> List[Dict[str, Any]]:
        """
        Perform vector search to retrieve the top N related news articles from the MongoDB database.

        Args:
            query_embedding (List[float]): The embedding vector of the query.
            top_n (int): The number of top-related news articles to retrieve.

        Returns:
            List[Dict[str, Any]]: A list of dictionaries containing related news articles with their similarity scores.
        """
        # MongoDB vector search query
        pipeline = [
            {
                '$vectorSearch': {
                    'index': 'vector_index',
                    'path': 'e5_embedding',  # Field in the documents containing the vector
                    'queryVector': query_embedding,  # Query vector
                    'numCandidates': 10000,  # Adjust for performance if needed
                    'limit': top_n  # Limit results to top N
                }
            },
            {
                '$project': {
                    "title": 1,
                    "description": 1,
                    "link": 1,
                    "pubDate": 1,
                    # "guid": 1,
                    "content": 1,
                    "e5_embedding": 1,
                    "cosine_similarity": {"$meta": "vectorSearchScore"} # MongoDB's similarity score
                }
            }
        ]

        try:
            # Retrieve all news articles from the database
            logger.info("Starting to retrieve all news articles...")
            start_time = time.time()
            result = list(self.collection.aggregate(pipeline))
            # Log the completion of news retrieval and calculate elapsed time
            end_time = time.time()
            logger.info(f"Finished retrieving news articles. Total time taken: {(end_time - start_time):.0f} seconds.")
            return result
            
        except Exception as e:
            raise MongoDBVectorSearchError(e)

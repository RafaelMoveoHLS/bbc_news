from typing import Any, Dict
from handlers.main_handler import Handler
from managers.news_manager import NewsManager
from services.openai_service import embed_with_openai_batched
from validators.news_validator import NewsQueryModel
from sklearn.metrics.pairwise import cosine_similarity



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
    
    def semantic_news_search(self, query: str) -> Dict[str, Any]:
        """
        Retrieve related news articles based on semantic similarity.

        Args:
            query (str): The search query passed as a query parameter.

        Returns:
            Dict[str, Any]: The response body object containing the related news articles.
        """
        # Retrieve all news articles from the database
        all_news = self.manager.get_all_rows()

        # Embed the query using OpenAI
        query_embedding = embed_with_openai_batched([query])[0]

        # List to store relevant news with their similarity scores
        relevant_news = []

        # Iterate over each news article and calculate cosine similarity
        for news in all_news:
            news_embedding = news.get('openai_embedding', [])
            if news_embedding:
                # Calculate cosine similarity between query and news embedding
                similarity = cosine_similarity(
                    [query_embedding], [news_embedding]
                )[0][0]

                # Add to relevant news if similarity exceeds threshold
                if similarity >= 0.45:
                    relevant_news.append({
                        "news": news,
                        "similarity": round(similarity, 4) 
                    })

        if len(relevant_news) > 0:
            # Sort relevant news by similarity in descending order
            sorted_news = sorted(relevant_news, key=lambda x: x["similarity"], reverse=True)

            # Prepare the response object with related news
            return {
                "related_news": [
                    {
                        "title": item["news"]["title"],
                        "description": item["news"]["description"],
                        "link": item["news"]["link"],
                        "published_date": item["news"]["pubDate"].strftime("%Y-%m-%d"),
                        "guid": item["news"]["guid"],
                        "similarity": item["similarity"]
                    }
                    for item in sorted_news
                ]
            }
        else:
            return {"related_news": "No relevant news found."}
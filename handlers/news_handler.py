from typing import Any, Dict
from handlers.main_handler import Handler
from managers.news_manager import NewsManager
from services.openai_service import ask_chatgpt_4o_mini, embed_with_openai_batched
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
                    [query_embedding], [news_embedding])[0][0]

                # Add to relevant news if similarity exceeds threshold
                if similarity >= 0.41:
                    relevant_news.append({
                        "title": news["title"],
                        "description": news["description"],
                        "link": news["link"],
                        "published_date": news["pubDate"].strftime("%Y-%m-%d"),
                        "guid": news["guid"],
                        "content": news["content"],
                        "cosine_similarity": round(similarity, 4)
                    })

        if len(relevant_news) > 0:
            # Sort relevant news by similarity in descending order
            sorted_news = sorted(
                relevant_news, key=lambda x: x["cosine_similarity"], reverse=True)
            return {"related_news": sorted_news}
        else:
            return {"related_news": "No relevant news found."}

    def question_the_news(self, question: str) -> Dict[str, Any]:
        """
        Answer the user's question based on the relevant news.

        Args:
            question (str): The user's question to answer according the news.


        Returns:
            Dict[str, Any]: The response body object containing the relevant answer.
        """
        # Retrieve all relevant news
        relevant_news = self.semantic_news_search(question)["related_news"]
        if relevant_news == "No relevant news found.":
            return {"answer": "No news found to answer this question."}

        # Prepare the prompt
        # Combine relevant news content into a single context passage
        context = "".join(
            [f"<p>{news['content']}</p>" for news in relevant_news])

        # Limit context length if necessary (for efficient processing)
        max_context_length = 5000
        if len(context) > max_context_length:
            context = context[:max_context_length]

        prompt = f"""Context: {context}
        Question: {question}
        Answer accurately and concisely based only on the provided context, focusing on the most relevant details:"""

        answer = ask_chatgpt_4o_mini(prompt)
        return {"answer": answer}

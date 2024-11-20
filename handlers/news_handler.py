import json
from typing import Any, Dict
from config import STEP_FUNCTION_ARN
from services.exeptions import OpenAIChatError
from services.logger import get_logger
from handlers.main_handler import Handler
from managers.news_manager import NewsManager
from services.e5_service import embed_with_e5
from services.openai_service import embed_with_openai_batched
from validators.news_validator import NewsQueryModel
from sklearn.metrics.pairwise import cosine_similarity
import boto3


logger = get_logger()

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

    def semantic_news_search(self, query: str, model_name: str = "E5", top_n: int = 20) -> Dict[str, Any]:
        """
        Retrieve related news articles based on semantic similarity using MongoDB Vector Search.

        Args:
            query (str): The search query passed as a query parameter.
            model_name (str): The embedding model to use for query embedding ("E5" or "OpenAI"). Defaults to "E5".
            top_n (int): The number of top-related news articles to retrieve. Defaults to 10.

        Returns:
            Dict[str, Any]: The response body object containing the related news articles.
        """
        # Embed the query based on the specified model
        if model_name == "OpenAI":
            query_embedding = embed_with_openai_batched([query])[0]
        elif model_name == "E5":
            query_embedding = embed_with_e5([query])[0]
        else:
            raise ValueError(f"Unsupported model name: {model_name}")

        # Retrieve top N related rows using the vector search
        top_n_news = self.manager.get_top_n_related_rows(query_embedding, top_n)

        relevant_news = []

        if top_n_news:
            for news in top_n_news:
                # Add to relevant news if similarity exceeds threshold
                similarity = cosine_similarity([query_embedding], [news['e5_embedding']])[0][0]
                if similarity >= 0.91:
                    relevant_news.append({
                        "title": news["title"],
                        "description": news["description"],
                        "link": news["link"],
                        "published_date": news["pubDate"].strftime("%Y-%m-%d"),
                        "calculated_cosine_similarity": round(similarity,4),
                        # "guid": news["guid"],
                        # "content": news["content"],
                        # "cosine_similarity": round(news['cosine_similarity'],4)
                    })
            # Format the result and return sorted news by similarity
            sorted_news = sorted(relevant_news, key=lambda x: x["calculated_cosine_similarity"], reverse=True)
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
        try:
            # Initialize Step Functions client
            client = boto3.client("stepfunctions", region_name="eu-north-1")
            
            # Synchronous execution of step functions
            response = client.start_sync_execution(
                stateMachineArn=STEP_FUNCTION_ARN,
                input=json.dumps({
                    "relevant_news": relevant_news,
                    "question": question
                })
            )

            # Extract and parse the output
            if response['status'] == 'SUCCEEDED':
                output = response['output']
                parsed_output = json.loads(output)
                chatgpt_answer = parsed_output.get('final_answer', 'No answer returned from ChatGPT')
                return {"answer": chatgpt_answer}
            else:
                return {"Execution failed": response}

            # Standard lambda function use:
            """
            # Start the Step Function execution
            start_response = client.start_execution(
                stateMachineArn="arn:aws:states:eu-north-1:779000067130:stateMachine:SkillsBBCNewsStateMachine",
                input=json.dumps({
                    "relevant_news": relevant_news,
                    "question": question
                })
            )
            execution_arn = start_response['executionArn']
            # print("Step Function Execution ARN:", execution_arn)

            # Wait for the Step Function to complete
            def wait_for_execution(execution_arn):
                seconds_waited = 0
                while True:
                    response = client.describe_execution(executionArn=execution_arn)
                    status = response['status']
                    if status in ['SUCCEEDED', 'FAILED', 'TIMED_OUT', 'ABORTED']:
                        return response
                    print(f"Waiting for execution to complete, waited {seconds_waited} seconds")
                    seconds_waited += 1
                    time.sleep(1)

            execution_response = wait_for_execution(execution_arn)

            # Extract and parse the output
            if execution_response['status'] == 'SUCCEEDED':
                output = execution_response['output']
                parsed_output = json.loads(output)
                chatgpt_answer = parsed_output.get('final_answer', 'No answer returned')
                return {"answer": chatgpt_answer}
            else:
                return {"Execution failed": execution_response}
            """

            # Local lambda code use:
            """
            prompt = create_prompt(relevant_news, question)
            prompt = "Context: <p>Israel Supreme Court strikes down judicial reforms. The controversial plans triggered nationwide protests last year against Benjamin Netanyahu's government.</p>\n    Question: Why the judicial reforms fails?\n    Answer accurately and concisely based only on the provided context, focusing on the most relevant details:"
            raw_answer = ask_chatgpt_4o_mini(prompt)
            answer = process_chatgpt_raw_answer(raw_answer['raw_response'])
            """
        except Exception as e:
            logger.error(f"Failed to generate answer: {str(e)}")
            raise OpenAIChatError(e)
        

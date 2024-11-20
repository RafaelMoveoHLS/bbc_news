import json
import time
from typing import Any, Dict
from config import STEP_FUNCTION_ARN
from lambda_functions.ask_chatgpt_4o_mini import ask_chatgpt_4o_mini
from lambda_functions.process_raw_answer import process_chatgpt_raw_answer
from services.exeptions import OpenAIChatError
from services.logger import get_logger
from handlers.main_handler import Handler
from lambda_functions.create_prompt import create_prompt
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

    def semantic_news_search(self, query: str, model_name:str = 'E5') -> Dict[str, Any]:
        """
        Retrieve related news articles based on semantic similarity.

        Args:
            query (str): The search query passed as a query parameter.

        Returns:
            Dict[str, Any]: The response body object containing the related news articles.
        """
        # Retrieve all news articles from the database
        logger.info("Starting to retrieve all news articles...")
        start_time = time.time()
        all_news = self.manager.get_all_rows()
        # Log the completion of news retrieval and calculate elapsed time
        end_time = time.time()
        logger.info(f"Finished retrieving news articles. Total time taken: {(end_time - start_time):.0f} seconds.")

        if model_name == "OpenAI":
            # Embed the query using OpenAI
            query_embedding = embed_with_openai_batched([query])[0]
            similarity_threshold = 0.41
        elif model_name == "E5":
            # Embed the query using E5
            query_embedding = embed_with_e5([query])[0]
            similarity_threshold = 0.91

        # List to store relevant news with their similarity scores
        relevant_news = []

        # Iterate over each news article and calculate cosine similarity
        for news in all_news:
            if model_name == "OpenAI":
                news_embedding = news.get('openai_embedding', [])
            elif model_name == "E5":
                news_embedding = news.get('e5_embedding', [])

            if news_embedding:
                # Calculate cosine similarity between query and news embedding
                similarity = cosine_similarity(
                    [query_embedding], [news_embedding])[0][0]

                # Add to relevant news if similarity exceeds threshold
                if similarity >= similarity_threshold:
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
            sorted_news = sorted(relevant_news, key=lambda x: x["cosine_similarity"], reverse=True)
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
        

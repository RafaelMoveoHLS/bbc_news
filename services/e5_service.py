import time
from services.exeptions import SageMakerError
from services.logger import get_logger
import boto3
import json

logger = get_logger()

def embed_with_e5(texts: list[str]) -> list[list[float]]:
    """
    Embed a list of texts using fine-tuned E5 model from SageMaker.

    Args:
        texts (list[str]): List of text inputs to be embedded.

    Returns:
        list[list[float]]: List of embeddings for the input texts. 
    """
    emb_list = []
    init_start_time = time.time()
    start_time = time.time()  # Start time for tracking elapsed time
    logger.info("Start embedding with E5 on SageMaker...")

    try:
        # Initialize the SageMaker runtime client
        sagemaker_runtime = boto3.client(
            "sagemaker-runtime",
            region_name="eu-north-1"
        )

        for i, text in enumerate(texts):
            # Payload
            payload = {"inputs": text}
            # Send the request to the SageMaker endpoint
            response = sagemaker_runtime.invoke_endpoint(
                EndpointName="skill-bcc-news-finetuned-e5-embedding-endpoint",
                ContentType="application/json",
                Body=json.dumps(payload)
            )
            # Parse the response
            response_body = json.loads(response["Body"].read().decode("utf-8"))
            emb_list.append(response_body[0][0])

            # Print progress every 100 texts
            if (i + 1) % 100 == 0:
                percent_complete = ((i + 1) / len(texts)) * 100
                elapsed_time = time.time() - start_time  # Calculate time taken
                logger.info(f"Processed {percent_complete:.0f}% of news -> {i + 1} out of {len(texts)} texts. "
                            f"Time taken for last 100 texts: {elapsed_time:.2f} seconds.")
                start_time = time.time()  # Reset the start time for the next batch
        
        logger.info(f"Time taken to embed {len(texts)} texts: {(time.time() - init_start_time):.2f} seconds.")         
        return emb_list
    except Exception as e:
            logger.error(f"Failed to embed the query with SageMaker: {str(e)}")
            raise SageMakerError(e)
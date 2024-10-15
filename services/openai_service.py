from openai import OpenAI
from services.logger import get_logger

logger = get_logger()

def embed_with_openai_batched(texts: list[str], batch_size: int = 1000)-> list[list[float]]:
    """
    Embed a list of texts using the OpenAI API in batches.

    Args:
        texts (list[str]): List of text inputs to be embedded.
        batch_size (int): Number of texts to process in each batch.

    Returns:
        list[list[float]]: List of embeddings for the input texts.
    """    
    client = OpenAI()
    emb_list = []
    # Process texts in batches
    for batch_texts in batch(texts, batch_size):
        logger.info(f"Send {len(batch_texts)} news to OpenAI API. Got already {len(emb_list)} embeddings.")
        try:
            response = client.embeddings.create(
                model="text-embedding-3-small",
                input=batch_texts
            )
            # Collect embeddings from the response
            for emb in response.data:
                emb_list.append(emb.embedding)
        except Exception as e:
            logger.error(f"Failed to retrieve embeddings: {str(e)}")
            raise

    return emb_list

def batch(iterable: list, batch_size: int):
    """
    Split a list into smaller batches.

    Args:
        iterable (list): List to be split into batches.
        batch_size (int): Size of each batch.

    Yields:
        list: A batch of elements from the original list.
    """
    for i in range(0, len(iterable), batch_size):
        yield iterable[i:i + batch_size]
from openai import OpenAI
from services.exeptions import OpenAIChatError, OpenAIEmbeddingError
from services.logger import get_logger
from config import OPENAI_API_KEY

logger = get_logger()

client = OpenAI(api_key=OPENAI_API_KEY)

def embed_with_openai_batched(texts: list[str], batch_size: int = 1000)-> list[list[float]]:
    """
    Embed a list of texts using the OpenAI API in batches.

    Args:
        texts (list[str]): List of text inputs to be embedded.
        batch_size (int): Number of texts to process in each batch.

    Returns:
        list[list[float]]: List of embeddings for the input texts.
    """    
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
            raise OpenAIEmbeddingError(e)

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


def ask_chatgpt_4o_mini(prompt: str)-> str:
    """
    Use the OpenAI ChatGPT-4o-mini to generate an answer based on the given prompt.

    Args:
        prompt (str): The prompt for the chatbot.

    Returns:
        str: The generated answer.
    """
    try:
        completion = client.chat.completions.create(
        model = "gpt-4o-mini",
        messages =[
            {"role": "system", "content": "You are a question answering system, you answer questions based only on given context."},
            {"role": "user", "content": prompt}
            ]
        )
    
        return completion.choices[0].message.content.strip()
    except Exception as e:
        logger.error(f"Failed to generate answer: {str(e)}")
        raise OpenAIChatError(e)
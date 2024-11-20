import json

def process_chatgpt_raw_answer(raw_answer: str)-> str:
    """
    Process the raw ChatGPT answer to extract the final answer.

    Args:
        raw_answer (str): The raw ChatGPT answer in json format.

    Returns:
        str: The final answer extracted from the raw ChatGPT answer.
    """
    raw_answer = json.loads(raw_answer)
    return raw_answer['choices'][0]['message']['content'].strip()
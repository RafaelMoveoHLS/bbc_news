from openai import OpenAI
from config import OPENAI_API_KEY

client = OpenAI(api_key=OPENAI_API_KEY)

def ask_chatgpt_4o_mini(prompt: str) -> str:
    """
    Use the OpenAI ChatGPT-4o-mini to generate an answer based on the given prompt.

    Args:
        prompt (str): The prompt for the chatbot.

    Returns:
        str: The generated answer.
    """
    completion = client.chat.completions.create(
        model="gpt-4o-mini",
        messages=[
            {"role": "system", "content": "You are a question answering system, you answer questions based only on given context."},
            {"role": "user", "content": prompt}
        ]
    )
    return {"raw_response": completion.model_dump_json()}
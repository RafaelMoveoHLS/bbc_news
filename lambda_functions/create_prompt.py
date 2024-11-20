from typing import Any, Dict, List


def create_prompt(relevant_news: List[Dict[str, Any]], question: str)-> str:
    """
    Creates a prompt for the LLM to generate an answer based on the relevant news articles.

    Args:
        relevant_news (pd.DataFrame): DataFrame containing relevant news articles.
        question (str): The user's question to answer according the news.
        
    Returns:
        str: The prompt for the LLM.
    """
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
    return prompt
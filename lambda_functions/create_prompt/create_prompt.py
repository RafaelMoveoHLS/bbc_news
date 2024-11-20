import json

def handler(event, context):
    """
    AWS Lambda handler for creating the prompt.
    """
    relevant_news = event.get("relevant_news", [])
    question = event.get("question", "")
    
    # Combine relevant news into a single context
    context = "".join([f"<p>{news['content']}</p>" for news in relevant_news])
    context = context[:5000]  # Limit context length to 5000 characters

    prompt = f"""Context: {context}
    Question: {question}
    Answer accurately and concisely based only on the provided context, focusing on the most relevant details:"""

    return {"prompt": prompt}
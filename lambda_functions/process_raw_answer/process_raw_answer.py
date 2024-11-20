import json

def handler(event, context):
    """
    AWS Lambda handler for processing raw ChatGPT responses.
    """
    raw_answer = json.loads(event.get("raw_response", {}))
    final_answer = raw_answer["choices"][0]["message"]["content"].strip()
    return {"final_answer": final_answer}
from typing import Any, Dict
import boto3
import json
from services.logger import get_logger

logger = get_logger()


def get_secret(secret_name: str, region_name: str = "eu-north-1") -> Dict[str, Any]:
    """
    Retrieve a secret from AWS Secrets Manager.

    :param secret_name: Name of the secret in AWS Secrets Manager.
    :param region_name: AWS region where the secret is stored.
    :return: A dictionary containing the secret values.
    """
    client = boto3.client("secretsmanager", region_name=region_name)

    try:
        response = client.get_secret_value(SecretId=secret_name)
        secret_dict = json.loads(response["SecretString"])
        return secret_dict
    except Exception as e:
        logger.error(f"Error retrieving secret {secret_name}: {e}")
        raise RuntimeError(f"Error retrieving secret {secret_name}: {str(e)}")


# Fetch the secrets
secret_name = "dev/bbc_news_skill/secrets"
region_name = "eu-north-1"

secrets = get_secret(secret_name, region_name)

# Access the secrets
OPENAI_API_KEY = secrets["OPENAI_API_KEY"]
MONGO_DB_ATLAS_URI = secrets["MONGO_DB_URI_ATLAS"]

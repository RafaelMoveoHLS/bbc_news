import time
import torch
from transformers import AutoModel, AutoTokenizer
from peft import PeftModel, PeftConfig
import os
from services.logger import get_logger

logger = get_logger()
logger.info("Trying to load E5...")

# Paths to the base model and adapter
base_model_directory = os.path.abspath("models/fine_tuned_e5_large_v2_lora/base_model")
adapter_directory = os.path.abspath("models/fine_tuned_e5_large_v2_lora/adapter")
tokenizer_directory = os.path.abspath("models/fine_tuned_e5_large_v2_lora")

# Verify that required files are present in the base model directory
assert "config.json" in os.listdir(base_model_directory), "config.json is missing in the base model directory."
assert "model.safetensors" in os.listdir(base_model_directory), "model.safetensors is missing in the base model directory."

# Load the base model, ensuring it looks only in the local directory
model = AutoModel.from_pretrained(base_model_directory, local_files_only=True)

# Load the LoRA adapter configuration and apply it
adapter_config = PeftConfig.from_pretrained(adapter_directory, local_files_only=True)
model = PeftModel.from_pretrained(model, adapter_directory, local_files_only=True)

# Load the tokenizer
tokenizer = AutoTokenizer.from_pretrained(tokenizer_directory, local_files_only=True)

logger.info("E5 model and adapter loaded successfully.")

def embed_with_e5(texts: list[str]) -> list[list[float]]:
    """
    Embed a list of texts using fine-tuned E5 model.

    Args:
        texts (list[str]): List of text inputs to be embedded.

    Returns:
        list[list[float]]: List of embeddings for the input texts. 
    """
    emb_list = []
    start_time = time.time()  # Start time for tracking elapsed time

    for i, text in enumerate(texts):
        inputs = tokenizer(text, return_tensors="pt",padding=True, truncation=True)
        with torch.no_grad():
            embeddings = model(**inputs).last_hidden_state.mean(dim=1).cpu().numpy()
        emb_list.append(embeddings.tolist()[0])

        # Print progress every 100 texts
        if (i + 1) % 100 == 0:
            percent_complete = ((i + 1) / len(texts)) * 100
            elapsed_time = time.time() - start_time  # Calculate time taken
            logger.info(f"Processed {percent_complete:.0f}% of news -> {i + 1} out of {len(texts)} texts. "
                        f"Time taken for last 100 texts: {elapsed_time:.2f} seconds.")
            start_time = time.time()  # Reset the start time for the next batch
    return emb_list
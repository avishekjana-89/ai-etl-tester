from abc import ABC, abstractmethod
import logging
from typing import List, Dict, Any, Optional

import litellm
from app.config import AI_PROVIDER, AI_MODEL, AI_API_KEY, AI_BASE_URL

logger = logging.getLogger("etl_ai")

# Configure litellm global settings if needed
import os
os.environ["LITELLM_HTTP_HANDLER"] = "httpx"

litellm.telemetry = False  # Disable telemetry for privacy
litellm.disable_cost_updated = True  # Disable remote cost map fetching (fixes "Network unreachable" errors)

class BaseAIProvider(ABC):
    """
    Interface for AI providers.
    """
    @abstractmethod
    async def chat(self, messages: List[Dict[str, str]], temperature: float = 0.1) -> str:
        """
        Send a chat completion request.
        """
        ...

class LiteLLMProvider(BaseAIProvider):
    """
    Unified provider implementation using LiteLLM.
    Supports OpenAI, Anthropic, Bedrock, Vertex, Ollama, etc.
    """
    def __init__(self, api_key: str, model: str, base_url: Optional[str] = None, provider_name: Optional[str] = None):
        self.api_key = api_key
        self.base_url = base_url
        
        # LiteLLM format: 'provider/model' or just 'model' for OpenAI
        # If provider_name is explicitly passed but not in model string, we can prepend it
        if provider_name and "/" not in model and provider_name.lower() != "openai":
             self.model = f"{provider_name.lower()}/{model}"
        else:
            self.model = model
            
        logger.info(f"Initialized LiteLLMProvider with model: {self.model}")

    async def chat(self, messages: List[Dict[str, str]], temperature: float = 0.1) -> str:
        logger.info(f"AI Request (LiteLLM: {self.model}) - {len(messages)} messages")
        
        try:
            # Handle LiteLLM's local Mac URL routing bug for Ollama:
            # Treat Ollama as an OpenAI compatible endpoint to bypass its custom HTTP formatter
            is_ollama = "ollama" in self.model.lower()
            model_name = self.model.replace("ollama/", "openai/") if is_ollama else self.model
            
            # Ollama's OpenAI compatible API is hosted under /v1
            api_base = self.base_url
            if is_ollama and api_base and not api_base.endswith("/v1"):
                api_base = f"{api_base.rstrip('/')}/v1"
                
            response = await litellm.acompletion(
                model=model_name,
                messages=messages,
                api_key=self.api_key if self.api_key else "ollama", # OpenAI wrapper requires some string
                base_url=api_base,
                temperature=temperature,
                response_format={"type": "json_object"} if "json" in str(messages).lower() else None
            )
            return response.choices[0].message.content
        except Exception as e:
            logger.error(f"LiteLLM Error: {str(e)}")
            raise

def get_ai_provider() -> BaseAIProvider:
    """
    Factory to get the unified LiteLLM provider.
    """
    return LiteLLMProvider(
        api_key=AI_API_KEY,
        model=AI_MODEL,
        base_url=AI_BASE_URL,
        provider_name=AI_PROVIDER
    )

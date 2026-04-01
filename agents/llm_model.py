__author__='chinonsocynthiaosuji'

"""
Author: Chinonso Cynthia Osuji
Date: 10/07/2025
Description:
    LLM model interface for various providers (Ollama, OpenAI, Anthropic, Groq, aiXplain, HuggingFace).
"""

import os, getpass
import re
from dotenv import load_dotenv, find_dotenv
from typing import Any, Dict, Optional, Text, Union
from langchain_core.prompts import ChatPromptTemplate

_ = load_dotenv(find_dotenv())  # Load environment variables

# === Base Interface ===
class ModelBase:
    def model_(self, agent_prompts: Optional[Text]) -> Dict:
        raise NotImplementedError

    def raw_model(self):
        raise NotImplementedError


# === Ollama Model ===
class OllamaModel(ModelBase):
    def __init__(self, model_name: str = "llama3.2", temperature: float = 0.0):
        from langchain_ollama import ChatOllama
        self.llm = ChatOllama(model=model_name, temperature=temperature)

    def model_(self, agent_prompts: Optional[Text]) -> Dict:
        prompt = ChatPromptTemplate.from_messages([
            ("system", agent_prompts), ("human", "{input}")
        ])
        return prompt | self.llm

    def raw_model(self):
        return self.llm


# # === OpenAI Model ===
# class OpenAIModel(ModelBase):
#     def __init__(self, model_name: str = "gpt-4", temperature: float = 0.0, api_key: Optional[str] = None):
#         from langchain_openai import ChatOpenAI
#         openai_key = os.getenv("OPENAI_API_KEY") or api_key
#         self.llm = ChatOpenAI(model=model_name, temperature=temperature, api_key=openai_key)

#     def model_(self, agent_prompts: Optional[Text]) -> Dict:
#         prompt = ChatPromptTemplate.from_messages([
#             ("system", agent_prompts), ("human", "{input}")
#         ])
#         return prompt | self.llm

#     def raw_model(self):
#         return self.llm

    
# # === OpenAI Model ===
from langchain_openai import ChatOpenAI


_CONTROL_CHAR_RE = re.compile(r"[\x00-\x08\x0b\x0c\x0e-\x1f]")
VALID_REASONING_EFFORTS = {"none", "low", "medium", "high"}


def _sanitize_text(value: str) -> str:
    if not value:
        return value
    cleaned = value.replace("\x00", " ")
    cleaned = _CONTROL_CHAR_RE.sub(" ", cleaned)
    return cleaned.encode("utf-8", "replace").decode("utf-8")


def _sanitize_payload(value):
    if isinstance(value, str):
        return _sanitize_text(value)
    if isinstance(value, list):
        return [_sanitize_payload(item) for item in value]
    if isinstance(value, tuple):
        return tuple(_sanitize_payload(item) for item in value)
    if isinstance(value, dict):
        return {key: _sanitize_payload(item) for key, item in value.items()}
    return value


def _sanitize_messages(messages):
    sanitized = []
    for message in messages:
        content = getattr(message, "content", None)
        additional_kwargs = getattr(message, "additional_kwargs", None)

        if content is None and additional_kwargs is None:
            sanitized.append(message)
            continue

        update = {"content": _sanitize_payload(content)}
        if isinstance(additional_kwargs, dict):
            update["additional_kwargs"] = _sanitize_payload(additional_kwargs)

        if hasattr(message, "model_copy"):
            sanitized.append(message.model_copy(update=update))
        elif hasattr(message, "copy"):
            sanitized.append(message.copy(update=update))
        else:
            try:
                message.content = update["content"]
                if "additional_kwargs" in update:
                    message.additional_kwargs = update["additional_kwargs"]
            except Exception:
                pass
            sanitized.append(message)

    return sanitized


def normalize_reasoning_effort(reasoning_effort: Optional[str]) -> Optional[str]:
    if reasoning_effort is None:
        return None
    normalized = str(reasoning_effort).strip().lower()
    if not normalized:
        return None
    if normalized not in VALID_REASONING_EFFORTS:
        raise ValueError(
            f"Unsupported reasoning effort {reasoning_effort!r}. "
            f"Expected one of {sorted(VALID_REASONING_EFFORTS)}."
        )
    return normalized

class ChatOpenAINoStop(ChatOpenAI):
    """
    Wrapper around ChatOpenAI that ignores any `stop` parameter coming from
    LangChain Agents or Runnables, so that models which do not support `stop`
    (like gpt-5.1) can be used safely.
    """

    # Newer LangChain code paths call _generate with `stop`
    def _generate(self, messages, stop=None, run_manager=None, **kwargs):
        # Drop the stop argument completely
        return super()._generate(_sanitize_messages(messages), run_manager=run_manager, **kwargs)

    # Some older paths call generate(...) directly
    def generate(self, messages, stop=None, **kwargs):
        # Drop stop here as well, just in case
        return super().generate(_sanitize_messages(messages), **kwargs)

    # Streaming agent paths call _stream with `stop`
    def _stream(self, messages, stop=None, run_manager=None, **kwargs):
        return super()._stream(_sanitize_messages(messages), run_manager=run_manager, **kwargs)

    # Async non-streaming paths may also pass `stop`
    async def _agenerate(self, messages, stop=None, run_manager=None, **kwargs):
        return await super()._agenerate(_sanitize_messages(messages), run_manager=run_manager, **kwargs)

    # Async streaming paths may also pass `stop`
    async def _astream(self, messages, stop=None, run_manager=None, **kwargs):
        async for chunk in super()._astream(_sanitize_messages(messages), run_manager=run_manager, **kwargs):
            yield chunk


class OpenAIModel(ModelBase):
    def __init__(
        self,
        model_name: str = "gpt-4",
        temperature: float = 0.0,
        api_key: Optional[str] = None,
        reasoning_effort: Optional[str] = None,
        **kwargs: Any,
    ):
        openai_key = os.getenv("OPENAI_API_KEY") or api_key
        if not openai_key:
            raise ValueError("OPENAI_API_KEY not found. Set it in .env or pass `api_key`.")

        normalized_reasoning_effort = normalize_reasoning_effort(reasoning_effort)
        if normalized_reasoning_effort is not None:
            kwargs.pop("reasoning_effort", None)
            kwargs.setdefault("reasoning", {"effort": normalized_reasoning_effort})

        # Use the no stop wrapper here
        self.llm = ChatOpenAINoStop(
            model=model_name,
            temperature=temperature,
            api_key=openai_key,
            **kwargs,
            # optional, but recommended for new models:
            # max_completion_tokens=800,
        )

    def model_(self, agent_prompts: Optional[Text]) -> Dict:
        prompt = ChatPromptTemplate.from_messages([
            ("system", agent_prompts),
            ("human", "{input}")
        ])
        return prompt | self.llm

    def raw_model(self):
        return self.llm



# === Anthropic Model ===
class AnthropicModel(ModelBase):
    def __init__(self, model_name: str = "claude-sonnet-4-5", temperature: float = 0.0, api_key: Optional[str] = None):
        from langchain_anthropic import ChatAnthropic
        claude_key = os.environ.get("ANTHROPIC_API_KEY") or api_key
        self.llm = ChatAnthropic(model=model_name, temperature=temperature, api_key=claude_key)

    def model_(self, agent_prompts: Optional[Text]) -> Dict:
        prompt = ChatPromptTemplate.from_messages([
            ("system", agent_prompts), ("human", "{input}")
        ])
        return prompt | self.llm

    def raw_model(self):
        return self.llm
    

# === Groq Model ===
class GroqModel(ModelBase):
    def __init__(self, model_name: str = "llama-3.3-70b-versatile", temperature: float = 0.0, api_key: Optional[str] = None):
        from langchain_groq import ChatGroq
        groq_key = os.getenv("GROQ_API_KEY") or api_key
        os.environ["GROQ_API_KEY"] = groq_key
        self.llm = ChatGroq(model=model_name, temperature=temperature, api_key=groq_key)

    def model_(self, agent_prompts: Optional[Text]) -> Dict:
        prompt = ChatPromptTemplate.from_messages([
            ("system", agent_prompts), ("human", "{input}")
        ])
        return prompt | self.llm

    def raw_model(self):
        return self.llm

# === aiXplain Model ===
class AiXplainModel(ModelBase):
    def __init__(self, model_id: str = "640b517694bf816d35a59125", temperature: float = 0.0, api_key: Optional[str] = None):
        from aixplain.factories import ModelFactory
        os.environ["TEAM_API_KEY"] = os.getenv("TEAM_API_KEY") or api_key
        self.llm = ModelFactory.get(model_id)
        self.temperature = temperature  # store if you need to use it in prompts

    def model_(self, agent_prompts: Optional[Text]) -> Dict:
        prompt = ChatPromptTemplate.from_messages([
            ("system", agent_prompts), ("human", "{input}")
        ])
        return prompt | self.llm

    def raw_model(self):
        return self.llm


# === HuggingFace Model ===
class HFModel(ModelBase):
    def __init__(self, model_name: str = "HuggingFaceH4/zephyr-7b-beta", temperature: float = 0.0, api_key: Optional[str] = None):
        from langchain_huggingface import ChatHuggingFace
        hf_token = os.getenv("HF_TOKEN") or api_key
        self.llm = ChatHuggingFace(
            model=model_name,
            temperature=temperature,
            huggingfacehub_api_token=hf_token
        )

    def model_(self, agent_prompts: Optional[Text]) -> Dict:
        prompt = ChatPromptTemplate.from_messages([
            ("system", agent_prompts), ("human", "{input}")
        ])
        return prompt | self.llm

    def raw_model(self):
        return self.llm


# === Unified Factory ===
class UnifiedModel:
    def __init__(self, provider: str, **kwargs):
        provider = provider.lower()

        if provider == "ollama":
            self.model = OllamaModel(**kwargs)

        elif provider == "openai":
            kwargs.setdefault("api_key", os.getenv("OPENAI_API_KEY"))
            if not kwargs["api_key"]:
                raise ValueError("OPENAI_API_KEY not found. Set it in .env or pass `api_key`.")
            self.model = OpenAIModel(**kwargs)
            
        elif provider == "anthropic":
            kwargs.setdefault("api_key", os.getenv("ANTHROPIC_API_KEY"))
            if not kwargs["api_key"]:
                raise ValueError("ANTHROPIC_API_KEY not found. Set it in .env or pass `api_key`.")
            self.model = AnthropicModel(**kwargs)

        elif provider == "groq":
            kwargs.setdefault("api_key", os.getenv("GROQ_API_KEY"))
            if not kwargs["api_key"]:
                raise ValueError("GROQ_API_KEY not found. Set it in .env or pass `api_key`.")
            self.model = GroqModel(**kwargs)

        elif provider in ["hf", "huggingface"]:
            kwargs.setdefault("hf_token", os.getenv("HF_TOKEN"))
            self.model = HFModel(**kwargs)

        elif provider == "aixplain":
            kwargs.setdefault("model_id", "640b517694bf816d35a59125")
            self.model = AiXplainModel(**kwargs)

        else:
            raise ValueError(f"Unsupported provider: {provider}")

    def model_(self, agent_prompts: Optional[Text]):
        return self.model.model_(agent_prompts)

    def raw_model(self):
        return self.model.raw_model()


model_name = {
    "ollama": {"model_name": "llama3.2", "temperature": 0.0},
    "openai": {"model_name": "gpt-4.1", "temperature": 0.0},
    "anthropic": {"model_name": "claude-sonnet-4-5", "temperature": 0.0},
    "groq": {"model_name": "deepseek-r1-distill-llama-70b", "temperature": 0.0},
    "hf": {"model_name": "HuggingFaceH4/zephyr-7b-beta", "temperature": 0.0},
    "aixplain": {"model_id": "640b517694bf816d35a59125", "temperature": 0.0},
}#.get(provider.lower())


def resolve_model_config(
    provider: str,
    model_override: Optional[str] = None,
    temperature: Optional[float] = None,
    reasoning_effort: Optional[str] = None,
) -> Dict[str, Any]:
    provider_key = provider.lower()
    conf = model_name.get(provider_key, {}).copy()
    if model_override:
        key = "model_id" if provider_key == "aixplain" else "model_name"
        conf[key] = model_override
    if temperature is not None:
        conf["temperature"] = temperature
    normalized_reasoning_effort = normalize_reasoning_effort(reasoning_effort)
    if provider_key == "openai" and normalized_reasoning_effort is not None:
        conf.pop("reasoning_effort", None)
        conf["reasoning"] = {"effort": normalized_reasoning_effort}
    return conf

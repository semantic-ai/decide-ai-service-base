"""Helpers for recording AI model calls via Task.record_ai_call().

Shared across services (pdf-content, geocoding, ...) so AI-call logging and
cost attribution behave identically everywhere. Consolidated from the previous
per-service copies.
"""
import re
from typing import Any

_AIRO_PREFIX = "http://data.lblod.info/ontology/airo#"


def model_name_to_uri(model_name: str) -> str:
    """Convert a model name/identifier to an airo: model URI.

    Examples:
        "mistral-medium-3.5"        -> "http://data.lblod.info/ontology/airo#mistral-medium-3-5"
        "Helsinki-NLP/opus-mt-nl-en" -> "http://data.lblod.info/ontology/airo#helsinki-nlp-opus-mt-nl-en"
    """
    safe = re.sub(r'[^A-Za-z0-9]+', '-', model_name).strip('-').lower()
    return f"{_AIRO_PREFIX}{safe}"


def _resolve_model_uri(model_uri: str) -> str:
    """Resolve a model identifier to a proper URI.

    If *model_uri* is already an absolute URI (starts with ``http``) it is
    returned unchanged; otherwise it is treated as a model name and passed
    through :func:`model_name_to_uri`.
    """
    if model_uri.startswith("http"):
        return model_uri
    return model_name_to_uri(model_uri)


def extract_tokens_from_response(response: Any) -> tuple[int, int]:
    """Extract input/output token counts from a LangChain chat response.

    Returns (tokens_in, tokens_out). Falls back to (0, 0) if unavailable.
    """
    usage = getattr(response, "usage_metadata", None)
    if isinstance(usage, dict):
        return (
            usage.get("input_tokens", 0) or 0,
            usage.get("output_tokens", 0) or 0,
        )
    return (0, 0)


def record_llm_call(task, endpoint: str, model_uri: str, response, duration: float):
    """Record an LLM API call: token usage + cost, attributed to the real model.

    Args:
        task: A Task instance with record_ai_call().
        endpoint: Base URL or provider identifier.
        model_uri: Absolute airo URI or model name/identifier. Resolved via
            :func:`_resolve_model_uri`; cost is looked up against the resolved URI.
        response: LangChain chat response (for token extraction).
        duration: Elapsed seconds for the call.
    """
    from .pricing import calculate_cost

    tokens_in, tokens_out = extract_tokens_from_response(response)
    resolved_uri = _resolve_model_uri(model_uri)
    cost = calculate_cost(resolved_uri, tokens_in, tokens_out)
    task.record_ai_call(
        endpoint=endpoint,
        model_uri=resolved_uri,
        tokens_in=tokens_in,
        tokens_out=tokens_out,
        duration=duration,
        cost=cost,
    )


def record_ml_call(task, endpoint: str, model_uri: str, duration: float):
    """Record a classic ML inference on *task* (local/free: tokens=0, no cost).

    Args:
        task: A Task instance with record_ai_call().
        endpoint: Base URL or "local".
        model_uri: Absolute airo URI or model name/identifier, resolved via
            :func:`_resolve_model_uri`.
        duration: Elapsed seconds for the call.
    """
    task.record_ai_call(
        endpoint=endpoint,
        model_uri=_resolve_model_uri(model_uri),
        tokens_in=0,
        tokens_out=0,
        duration=duration,
    )

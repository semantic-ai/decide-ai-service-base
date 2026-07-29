import time
import logging
import requests
from string import Template

from escape_helpers import sparql_escape_uri
from helpers import query

from .sparql_config import get_prefixes_for_query, GRAPHS

logger = logging.getLogger(__name__)

# ==============================================================================
# Pricing cache (time-based TTL)
# ==============================================================================

_PRICING_CACHE: dict | None = None
_PRICING_CACHE_TIME: float = 0
_PRICING_CACHE_TTL: int = 3600  # 1 hour
_OPENROUTER_MODELS_URL = "https://openrouter.ai/api/v1/models"


def _cache_expired() -> bool:
    return time.monotonic() - _PRICING_CACHE_TIME > _PRICING_CACHE_TTL


def get_openrouter_id(model_uri: str) -> str | None:
    """
    Retrieve the ext:openrouterId for a given model URI from the triplestore.

    Args:
        model_uri: The URI of the airo:AIModel resource.

    Returns:
        The OpenRouter model ID string, or None if not found.
    """
    q = Template(
        get_prefixes_for_query("ext") +
        """
        SELECT ?openrouterId WHERE {
            GRAPH $graph {
                VALUES ?model { $model_uri }
                ?model ext:openrouterId ?openrouterId .
            }
        }
        LIMIT 1
        """
    ).substitute(
        graph=sparql_escape_uri(GRAPHS["jobs"]),
        model_uri=sparql_escape_uri(model_uri),
    )

    try:
        result = query(q, sudo=True)
        bindings = result.get("results", {}).get("bindings", [])
        if bindings and "openrouterId" in bindings[0]:
            return bindings[0]["openrouterId"]["value"]
    except Exception as e:
        logger.warning(f"Failed to query OpenRouter ID for model '{model_uri}': {e}")

    return None


def get_llm_pricing() -> dict:
    """
    Fetch model pricing from OpenRouter API.

    Returns a dict mapping OpenRouter model IDs to their per-1M-token rates:
    {
        "openai/gpt-4o": {
            "input_per_1m": 2.50,
            "output_per_1m": 10.00
        },
        ...
    }

    Cached for PRICING_CACHE_TTL seconds. Returns empty dict on any error.
    """
    global _PRICING_CACHE, _PRICING_CACHE_TIME

    if _PRICING_CACHE is not None and not _cache_expired():
        return _PRICING_CACHE

    try:
        response = requests.get(_OPENROUTER_MODELS_URL, timeout=10)
        response.raise_for_status()
        data = response.json()

        pricing_map: dict = {}
        for model in data.get("data", []):
            pricing = model.get("pricing", {})
            model_id = model.get("id")
            if not model_id:
                continue
            pricing_map[model_id] = {
                "input_per_1m": float(pricing.get("prompt", 0)) * 1_000_000,
                "output_per_1m": float(pricing.get("completion", 0)) * 1_000_000,
            }

        _PRICING_CACHE = pricing_map
        _PRICING_CACHE_TIME = time.monotonic()
        logger.info(f"Fetched pricing for {len(pricing_map)} models from OpenRouter")
        return pricing_map

    except Exception as e:
        logger.warning(f"Failed to fetch OpenRouter pricing: {e}")
        return {}


def calculate_cost(model_uri: str, tokens_in: int, tokens_out: int) -> float:
    """
    Calculate the cost of an AI model call.

    Args:
        model_uri: The URI of the model used (must have ext:openrouterId in the triplestore).
        tokens_in: Number of input/prompt tokens.
        tokens_out: Number of output/completion tokens.

    Returns:
        Cost in USD. Returns 0.0 for local models, unknown models, or any error.
    """
    openrouter_id = get_openrouter_id(model_uri)

    if openrouter_id is None:
        return 0.0

    pricing_db = get_llm_pricing()
    rates = pricing_db.get(openrouter_id)

    if rates is None:
        logger.warning(
            f"No pricing found for OpenRouter model '{openrouter_id}' "
            f"(resolved from model URI '{model_uri}'). Cost set to 0."
        )
        return 0.0

    try:
        input_cost = (tokens_in / 1_000_000) * rates["input_per_1m"]
        output_cost = (tokens_out / 1_000_000) * rates["output_per_1m"]
        return input_cost + output_cost
    except Exception as e:
        logger.error(f"Error calculating cost for model '{model_uri}': {e}")
        return 0.0

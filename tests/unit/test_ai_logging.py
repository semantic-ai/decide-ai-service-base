"""Tests for the shared AI-usage logging helpers.

These live in the base service because ``ai_logging`` (and cost attribution) is
now shared by all services. ``record_llm_call`` lazily imports
``decide_ai_service_base.pricing.calculate_cost``; we stub that module so the
tests are deterministic and never import the mu-python ``helpers`` runtime or
reach openrouter.ai.
"""
import sys
import types
from unittest.mock import MagicMock

import pytest

from decide_ai_service_base.ai_logging import (
    model_name_to_uri,
    _resolve_model_uri,
    extract_tokens_from_response,
    record_llm_call,
    record_ml_call,
)

AIRO = "http://data.lblod.info/ontology/airo#"


@pytest.fixture(autouse=True)
def fake_calculate_cost(monkeypatch):
    """Stub the lazily-imported pricing.calculate_cost with a fixed value."""
    fake = types.ModuleType("decide_ai_service_base.pricing")
    fake.calculate_cost = MagicMock(return_value=0.0123)
    monkeypatch.setitem(sys.modules, "decide_ai_service_base.pricing", fake)
    return fake.calculate_cost


# --- model URI resolution -------------------------------------------------

def test_model_name_to_uri_sanitizes():
    assert model_name_to_uri("mistral-medium-3.5") == AIRO + "mistral-medium-3-5"
    assert model_name_to_uri("Helsinki-NLP/opus-mt-nl-en") == AIRO + "helsinki-nlp-opus-mt-nl-en"


def test_resolve_model_uri_passthrough_and_convert():
    absolute = AIRO + "already-a-uri"
    assert _resolve_model_uri(absolute) == absolute            # http URI kept as-is
    assert _resolve_model_uri("spacy-nl") == AIRO + "spacy-nl"  # name -> airo URI


# --- token extraction -----------------------------------------------------

def test_extract_tokens_from_usage_metadata():
    resp = MagicMock()
    resp.usage_metadata = {"input_tokens": 100, "output_tokens": 50}
    assert extract_tokens_from_response(resp) == (100, 50)


def test_extract_tokens_without_metadata():
    resp = MagicMock(spec=[])  # no usage_metadata attribute
    assert extract_tokens_from_response(resp) == (0, 0)


def test_extract_tokens_none_values_default_to_zero():
    resp = MagicMock()
    resp.usage_metadata = {"input_tokens": None, "output_tokens": None}
    assert extract_tokens_from_response(resp) == (0, 0)


# --- record_llm_call: resolves model, computes + forwards cost ------------

def test_record_llm_call_records_cost_and_resolved_model(fake_calculate_cost):
    task = MagicMock()
    resp = MagicMock()
    resp.usage_metadata = {"input_tokens": 100, "output_tokens": 50}

    record_llm_call(task, "https://api.mistral.ai/v1", "mistral-medium-3.5", resp, 2.0)

    resolved = AIRO + "mistral-medium-3-5"
    fake_calculate_cost.assert_called_once_with(resolved, 100, 50)
    task.record_ai_call.assert_called_once_with(
        endpoint="https://api.mistral.ai/v1",
        model_uri=resolved,
        tokens_in=100,
        tokens_out=50,
        duration=2.0,
        cost=0.0123,
    )


def test_record_llm_call_keeps_absolute_model_uri(fake_calculate_cost):
    task = MagicMock()
    resp = MagicMock()
    resp.usage_metadata = {"input_tokens": 1, "output_tokens": 2}
    model_uri = AIRO + "mistral-medium-3-5"

    record_llm_call(task, "https://api.mistral.ai/v1", model_uri, resp, 1.0)

    fake_calculate_cost.assert_called_once_with(model_uri, 1, 2)
    assert task.record_ai_call.call_args.kwargs["model_uri"] == model_uri


# --- record_ml_call: local/free, no cost ----------------------------------

def test_record_ml_call_records_no_cost(fake_calculate_cost):
    task = MagicMock()

    record_ml_call(task, "local", "spacy-nl", 0.5)

    fake_calculate_cost.assert_not_called()  # ML path does not price
    kwargs = task.record_ai_call.call_args.kwargs
    assert kwargs == {
        "endpoint": "local",
        "model_uri": AIRO + "spacy-nl",
        "tokens_in": 0,
        "tokens_out": 0,
        "duration": 0.5,
    }
    assert "cost" not in kwargs

import time

import pytest
from unittest.mock import patch, MagicMock

from decide_ai_service_base.pricing import (
    calculate_cost,
    get_llm_pricing,
    get_openrouter_id,
    _cache_expired,
    _PRICING_CACHE_TTL,
)


class TestCacheExpired:
    def test_cache_fresh_not_expired(self):
        import decide_ai_service_base.pricing as mod
        mod._PRICING_CACHE = {"dummy": True}
        mod._PRICING_CACHE_TIME = time.monotonic()
        assert not _cache_expired()

    def test_cache_old_is_expired(self):
        import decide_ai_service_base.pricing as mod
        mod._PRICING_CACHE = {"dummy": True}
        mod._PRICING_CACHE_TIME = time.monotonic() - _PRICING_CACHE_TTL - 1
        assert _cache_expired()

    def test_none_cache_is_expired(self):
        import decide_ai_service_base.pricing as mod
        mod._PRICING_CACHE = None
        mod._PRICING_CACHE_TIME = 0
        assert _cache_expired()


class TestGetLlmPricing:
    @patch("decide_ai_service_base.pricing.requests.get")
    def test_successful_fetch(self, mock_get):
        import decide_ai_service_base.pricing as mod
        mod._PRICING_CACHE = None

        mock_response = MagicMock()
        mock_response.json.return_value = {
            "data": [
                {
                    "id": "openai/gpt-4o",
                    "pricing": {"prompt": 0.0000025, "completion": 0.00001}
                }
            ]
        }
        mock_response.raise_for_status.return_value = None
        mock_get.return_value = mock_response

        result = get_llm_pricing()
        assert "openai/gpt-4o" in result
        assert result["openai/gpt-4o"]["input_per_1m"] == pytest.approx(2.5)
        assert result["openai/gpt-4o"]["output_per_1m"] == pytest.approx(10.0)

    @patch("decide_ai_service_base.pricing.requests.get")
    def test_request_failure_returns_empty(self, mock_get):
        import decide_ai_service_base.pricing as mod
        mod._PRICING_CACHE = None
        mock_get.side_effect = Exception("Network error")
        result = get_llm_pricing()
        assert result == {}

    def test_cached_result_returned(self):
        import decide_ai_service_base.pricing as mod
        mod._PRICING_CACHE = {"cached-model": {"input_per_1m": 1.0, "output_per_1m": 2.0}}
        mod._PRICING_CACHE_TIME = time.monotonic()
        result = get_llm_pricing()
        assert result == {"cached-model": {"input_per_1m": 1.0, "output_per_1m": 2.0}}


class TestCalculateCost:
    def test_zero_cost_when_no_openrouter_id(self):
        with patch("decide_ai_service_base.pricing.get_openrouter_id", return_value=None):
            cost = calculate_cost("http://example.org/model/1", 100, 50)
            assert cost == 0.0

    @patch("decide_ai_service_base.pricing.get_openrouter_id", return_value="openai/gpt-4o")
    @patch("decide_ai_service_base.pricing.get_llm_pricing")
    def test_cost_calculation(self, mock_pricing, mock_roid):
        mock_pricing.return_value = {
            "openai/gpt-4o": {"input_per_1m": 2.5, "output_per_1m": 10.0}
        }
        cost = calculate_cost("http://example.org/model/1", 100_000, 50_000)
        expected = (100_000 / 1_000_000) * 2.5 + (50_000 / 1_000_000) * 10.0
        assert cost == pytest.approx(expected)

    @patch("decide_ai_service_base.pricing.get_openrouter_id", return_value="unknown-model")
    @patch("decide_ai_service_base.pricing.get_llm_pricing")
    def test_unknown_model_returns_zero(self, mock_pricing, mock_roid):
        mock_pricing.return_value = {}
        cost = calculate_cost("http://example.org/model/1", 100, 50)
        assert cost == 0.0

    @patch("decide_ai_service_base.pricing.get_openrouter_id", return_value="openai/gpt-4o")
    @patch("decide_ai_service_base.pricing.get_llm_pricing")
    def test_zero_tokens_returns_zero(self, mock_pricing, mock_roid):
        mock_pricing.return_value = {
            "openai/gpt-4o": {"input_per_1m": 2.5, "output_per_1m": 10.0}
        }
        cost = calculate_cost("http://example.org/model/1", 0, 0)
        assert cost == 0.0

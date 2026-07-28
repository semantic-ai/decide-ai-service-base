import pytest

from helpers import query, update
from escape_helpers import sparql_escape_uri, sparql_escape_string
from decide_ai_service_base.sparql_config import get_prefixes_for_query, GRAPHS


@pytest.fixture(autouse=True)
def cleanup_pricing_data():
    """Clean up test model data after each test."""
    yield
    try:
        update(f"""
            DELETE WHERE {{
                GRAPH {sparql_escape_uri(GRAPHS["jobs"])} {{
                    ?s <http://mu.semte.ch/vocabularies/ext/testPricingCleanup> "true" .
                    ?s ?p ?o .
                }}
            }}
        """)
    except Exception:
        pass


class TestGetOpenrouterId:
    def test_returns_openrouter_id_when_exists(self):
        from decide_ai_service_base.pricing import get_openrouter_id

        model_uri = "http://test.example/model/gpt4o"
        test_flag = "http://mu.semte.ch/vocabularies/ext/testPricingCleanup"
        openrouter_id = "openai/gpt-4o"

        update(f"""
            INSERT DATA {{
                GRAPH {sparql_escape_uri(GRAPHS["jobs"])} {{
                    {sparql_escape_uri(model_uri)} a <https://w3id.org/airo#AIModel> ;
                        <http://mu.semte.ch/vocabularies/ext/openrouterId> {sparql_escape_string(openrouter_id)} ;
                        <{test_flag}> "true" .
                }}
            }}
        """)

        try:
            result = get_openrouter_id(model_uri)
            assert result == openrouter_id
        finally:
            update(f"""
                DELETE WHERE {{
                    GRAPH {sparql_escape_uri(GRAPHS["jobs"])} {{
                        <{model_uri}> <{test_flag}> ?v .
                        <{model_uri}> ?p ?o .
                    }}
                }}
            """)

    def test_returns_none_when_model_not_found(self):
        from decide_ai_service_base.pricing import get_openrouter_id
        result = get_openrouter_id("http://test.example/model/nonexistent")
        assert result is None

    def test_returns_none_when_no_openrouter_id(self):
        from decide_ai_service_base.pricing import get_openrouter_id

        model_uri = "http://test.example/model/local-model"
        test_flag = "http://mu.semte.ch/vocabularies/ext/testPricingCleanup"

        update(f"""
            INSERT DATA {{
                GRAPH {sparql_escape_uri(GRAPHS["jobs"])} {{
                    {sparql_escape_uri(model_uri)} a <https://w3id.org/airo#AIModel> ;
                        <{test_flag}> "true" .
                }}
            }}
        """)

        try:
            result = get_openrouter_id(model_uri)
            assert result is None
        finally:
            update(f"""
                DELETE WHERE {{
                    GRAPH {sparql_escape_uri(GRAPHS["jobs"])} {{
                        <{model_uri}> <{test_flag}> ?v .
                        <{model_uri}> ?p ?o .
                    }}
                }}
            """)

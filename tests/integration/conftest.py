import pytest


@pytest.fixture
def virtuoso_endpoint():
    """Return the Virtuoso SPARQL endpoint URL."""
    import os
    return os.environ.get("MU_SPARQL_ENDPOINT", "http://localhost:8890/sparql")


@pytest.fixture
def virtuoso_updatepoint():
    """Return the Virtuoso SPARQL update endpoint URL."""
    import os
    return os.environ.get("MU_SPARQL_UPDATEPOINT", "http://localhost:8890/sparql")


@pytest.fixture
def virtuoso_available(virtuoso_endpoint):
    """Check if Virtuoso is reachable. Skip tests if not."""
    from SPARQLWrapper import SPARQLWrapper, JSON
    wrapper = SPARQLWrapper(virtuoso_endpoint, returnFormat=JSON)
    wrapper.setTimeout(5)
    wrapper.setQuery("ASK WHERE { ?s ?p ?o }")
    try:
        wrapper.query().convert()
        return True
    except Exception:
        pytest.skip("Virtuoso triplestore not available at localhost")

import pytest

from helpers import query, update


@pytest.fixture(autouse=True)
def cleanup_test_data():
    """Clean up test data after each test."""
    yield
    # Clean up any test data inserted during the test
    try:
        update("""
            DELETE { GRAPH <http://mu.semte.ch/graphs/jobs> { ?s ?p ?o } }
            WHERE { GRAPH <http://mu.semte.ch/graphs/jobs> { ?s <http://mu.semte.ch/vocabularies/ext/testResource> ?o } }
        """)
    except Exception:
        pass
    try:
        update("""
            DELETE { GRAPH <http://mu.semte.ch/graphs/ai> { ?s ?p ?o } }
            WHERE { GRAPH <http://mu.semte.ch/graphs/ai> { ?s <http://mu.semte.ch/vocabularies/ext/testResource> ?o } }
        """)
    except Exception:
        pass


class TestBasicQuery:
    def test_select_returns_results(self):
        result = query("SELECT ?s WHERE { ?s ?p ?o } LIMIT 5")
        assert "results" in result
        assert "bindings" in result["results"]

    def test_ask_returns_boolean(self):
        result = query("ASK WHERE { ?s ?p ?o }")
        assert "boolean" in result
        assert result["boolean"] is True

    def test_empty_result(self):
        result = query("SELECT ?s WHERE { <http://test.nonexistent/xyz> ?p ?o }")
        bindings = result["results"]["bindings"]
        assert bindings == []

    def test_query_with_sudo(self):
        result = query("SELECT ?s WHERE { ?s ?p ?o } LIMIT 1", sudo=True)
        assert "results" in result


class TestBasicUpdate:
    def test_insert_and_delete(self):
        update("""
            INSERT DATA {
                GRAPH <http://mu.semte.ch/graphs/jobs> {
                    <http://test.example/test/1> <http://mu.semte.ch/vocabularies/ext/testResource> "test1" .
                }
            }
        """)
        result = query("""
            SELECT ?o WHERE {
                GRAPH <http://mu.semte.ch/graphs/jobs> {
                    <http://test.example/test/1> <http://mu.semte.ch/vocabularies/ext/testResource> ?o .
                }
            }
        """)
        bindings = result["results"]["bindings"]
        assert len(bindings) == 1
        assert bindings[0]["o"]["value"] == "test1"

        update("""
            DELETE {
                GRAPH <http://mu.semte.ch/graphs/jobs> {
                    <http://test.example/test/1> <http://mu.semte.ch/vocabularies/ext/testResource> ?o .
                }
            } WHERE {
                GRAPH <http://mu.semte.ch/graphs/jobs> {
                    <http://test.example/test/1> <http://mu.semte.ch/vocabularies/ext/testResource> ?o .
                }
            }
        """)
        result = query("""
            SELECT ?o WHERE {
                GRAPH <http://mu.semte.ch/graphs/jobs> {
                    <http://test.example/test/1> <http://mu.semte.ch/vocabularies/ext/testResource> ?o .
                }
            }
        """)
        assert result["results"]["bindings"] == []

    def test_update_with_sudo(self):
        update("""
            INSERT DATA {
                GRAPH <http://mu.semte.ch/graphs/jobs> {
                    <http://test.example/test/2> <http://mu.semte.ch/vocabularies/ext/testResource> "test2" .
                }
            }
        """, sudo=True)
        result = query("""
            SELECT ?o WHERE {
                GRAPH <http://mu.semte.ch/graphs/jobs> {
                    <http://test.example/test/2> <http://mu.semte.ch/vocabularies/ext/testResource> ?o .
                }
            }
        """, sudo=True)
        assert len(result["results"]["bindings"]) == 1

    def test_invalid_update_does_not_crash(self):
        update("this is not valid SPARQL")


class TestHeaderForwarding:
    def test_query_clears_headers_when_no_request(self):
        from helpers import sparqlQuery, MU_HEADERS
        for header in MU_HEADERS:
            if header in sparqlQuery.customHttpHeaders:
                del sparqlQuery.customHttpHeaders[header]
        query("SELECT ?s WHERE { ?s ?p ?o } LIMIT 1")
        for header in MU_HEADERS:
            assert header not in sparqlQuery.customHttpHeaders

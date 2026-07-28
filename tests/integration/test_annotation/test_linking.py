import pytest

from helpers import query, update
from escape_helpers import sparql_escape_uri
from decide_ai_service_base.sparql_config import get_prefixes_for_query, GRAPHS


@pytest.fixture(autouse=True)
def cleanup_linking_data():
    yield
    try:
        update(f"""
            DELETE WHERE {{
                GRAPH {sparql_escape_uri(GRAPHS["ai"])} {{
                    ?s <http://mu.semte.ch/vocabularies/ext/testLinkingCleanup> "true" .
                    ?s ?p ?o .
                }}
            }}
        """)
    except Exception:
        pass


class TestLinkingAnnotation:
    def test_add_to_triplestore(self):
        from decide_ai_service_base.annotation import LinkingAnnotation

        activity_id = "http://test.example/activity/link-1"
        source_uri = "http://test.example/expression/link-1"
        class_uri = "http://dbpedia.org/ontology/Place"
        agent = "http://test.example/agent/1"
        test_flag = "http://mu.semte.ch/vocabularies/ext/testLinkingCleanup"

        update(f"""
            INSERT DATA {{
                GRAPH {sparql_escape_uri(GRAPHS["ai"])} {{
                    {sparql_escape_uri(activity_id)} <{test_flag}> "true" .
                }}
            }}
        """)

        ann = LinkingAnnotation(
            activity_id=activity_id,
            source_uri=source_uri,
            class_uri=class_uri,
            agent=agent,
            agent_type="http://test.example/agentType",
        )

        annotation_uri = ann.add_to_triplestore_if_not_exists()
        assert annotation_uri.startswith("http://data.lblod.info/id/annotations/")

        result = query(f"""
            SELECT ?ann WHERE {{
                GRAPH {sparql_escape_uri(GRAPHS["ai"])} {{
                    ?ann a <http://www.w3.org/ns/oa#Annotation> ;
                         <http://www.w3.org/ns/oa#hasBody> {sparql_escape_uri(class_uri)} ;
                         <http://www.w3.org/ns/oa#motivatedBy> <http://www.w3.org/ns/oa#classifying> .
                }}
            }}
        """)
        bindings = result["results"]["bindings"]
        assert len(bindings) >= 1

    def test_second_call_does_not_duplicate(self):
        from decide_ai_service_base.annotation import LinkingAnnotation

        activity_id = "http://test.example/activity/link-dedup"
        source_uri = "http://test.example/expression/link-dedup"
        class_uri = "http://dbpedia.org/ontology/Place"
        agent = "http://test.example/agent/1"
        test_flag = "http://mu.semte.ch/vocabularies/ext/testLinkingCleanup"

        update(f"""
            INSERT DATA {{
                GRAPH {sparql_escape_uri(GRAPHS["ai"])} {{
                    {sparql_escape_uri(activity_id)} <{test_flag}> "true" .
                }}
            }}
        """)

        ann = LinkingAnnotation(
            activity_id=activity_id,
            source_uri=source_uri,
            class_uri=class_uri,
            agent=agent,
            agent_type="http://test.example/agentType",
        )

        ann.add_to_triplestore_if_not_exists()
        ann.add_to_triplestore_if_not_exists()

        count_result = query(f"""
            SELECT (COUNT(?ann) AS ?count) WHERE {{
                GRAPH {sparql_escape_uri(GRAPHS["ai"])} {{
                    ?ann a <http://www.w3.org/ns/oa#Annotation> ;
                         <http://www.w3.org/ns/oa#hasBody> {sparql_escape_uri(class_uri)} ;
                         <http://www.w3.org/ns/oa#motivatedBy> <http://www.w3.org/ns/oa#classifying> .
                }}
            }}
        """)
        count = int(count_result["results"]["bindings"][0]["count"]["value"])
        assert count >= 1

    def test_init_sets_class_uri(self):
        from decide_ai_service_base.annotation import LinkingAnnotation

        ann = LinkingAnnotation(
            activity_id="http://test.example/activity/1",
            source_uri="http://test.example/source/1",
            class_uri="http://example.org/SomeClass",
            agent="http://test.example/agent/1",
            agent_type="http://test.example/agentType",
        )
        assert ann.class_uri == "http://example.org/SomeClass"
        assert ann.source_uri == "http://test.example/source/1"

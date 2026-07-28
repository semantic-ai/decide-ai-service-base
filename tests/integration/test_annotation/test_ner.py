import pytest

from helpers import query, update
from escape_helpers import sparql_escape_uri
from decide_ai_service_base.sparql_config import get_prefixes_for_query, GRAPHS


@pytest.fixture(autouse=True)
def cleanup_ner_data():
    yield
    try:
        update(f"""
            DELETE WHERE {{
                GRAPH {sparql_escape_uri(GRAPHS["ai"])} {{
                    ?s <http://mu.semte.ch/vocabularies/ext/testNerCleanup> "true" .
                    ?s ?p ?o .
                }}
            }}
        """)
    except Exception:
        pass


class TestNERAnnotation:
    def test_add_to_triplestore_with_positions(self):
        from decide_ai_service_base.annotation import NERAnnotation

        activity_id = "http://test.example/activity/ner-1"
        source_uri = "http://test.example/expression/ner-1"
        class_uri = "http://dbpedia.org/ontology/City"
        agent = "http://test.example/agent/1"
        test_flag = "http://mu.semte.ch/vocabularies/ext/testNerCleanup"

        update(f"""
            INSERT DATA {{
                GRAPH {sparql_escape_uri(GRAPHS["ai"])} {{
                    {sparql_escape_uri(activity_id)} <{test_flag}> "true" .
                }}
            }}
        """)

        ann = NERAnnotation(
            activity_id=activity_id,
            source_uri=source_uri,
            class_uri=class_uri,
            start=0,
            end=10,
            agent=agent,
            agent_type="http://test.example/agentType",
            confidence=0.95,
        )

        annotation_uri = ann.add_to_triplestore_if_not_exists()
        assert annotation_uri.startswith("http://data.lblod.info/id/annotations/")

        result = query(f"""
            SELECT ?ann WHERE {{
                GRAPH {sparql_escape_uri(GRAPHS["ai"])} {{
                    ?ann a <http://www.w3.org/ns/oa#Annotation> ;
                         <http://www.w3.org/ns/oa#hasBody> {sparql_escape_uri(class_uri)} ;
                         <http://www.w3.org/ns/oa#motivatedBy> <http://www.w3.org/ns/oa#tagging> .
                }}
            }}
        """)
        bindings = result["results"]["bindings"]
        assert len(bindings) >= 1

    def test_add_to_triplestore_without_positions(self):
        from decide_ai_service_base.annotation import NERAnnotation

        activity_id = "http://test.example/activity/ner-2"
        source_uri = "http://test.example/expression/ner-2"
        class_uri = "http://dbpedia.org/ontology/Person"
        agent = "http://test.example/agent/1"
        test_flag = "http://mu.semte.ch/vocabularies/ext/testNerCleanup"

        update(f"""
            INSERT DATA {{
                GRAPH {sparql_escape_uri(GRAPHS["ai"])} {{
                    {sparql_escape_uri(activity_id)} <{test_flag}> "true" .
                }}
            }}
        """)

        ann = NERAnnotation(
            activity_id=activity_id,
            source_uri=source_uri,
            class_uri=class_uri,
            start=None,
            end=None,
            agent=agent,
            agent_type="http://test.example/agentType",
        )

        annotation_uri = ann.add_to_triplestore_if_not_exists()
        assert annotation_uri.startswith("http://data.lblod.info/id/annotations/")

    def test_second_call_does_not_duplicate(self):
        from decide_ai_service_base.annotation import NERAnnotation

        activity_id = "http://test.example/activity/ner-dedup"
        source_uri = "http://test.example/expression/ner-dedup"
        class_uri = "http://dbpedia.org/ontology/City"
        agent = "http://test.example/agent/1"
        test_flag = "http://mu.semte.ch/vocabularies/ext/testNerCleanup"

        update(f"""
            INSERT DATA {{
                GRAPH {sparql_escape_uri(GRAPHS["ai"])} {{
                    {sparql_escape_uri(activity_id)} <{test_flag}> "true" .
                }}
            }}
        """)

        ann = NERAnnotation(
            activity_id=activity_id,
            source_uri=source_uri,
            class_uri=class_uri,
            start=0,
            end=5,
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
                         <http://www.w3.org/ns/oa#motivatedBy> <http://www.w3.org/ns/oa#tagging> .
                }}
            }}
        """)
        count = int(count_result["results"]["bindings"][0]["count"]["value"])
        assert count >= 1

    def test_build_selector_parts_with_positions(self):
        from decide_ai_service_base.annotation import NERAnnotation

        ann = NERAnnotation(
            activity_id="http://test.example/activity/1",
            source_uri="http://test.example/source/1",
            class_uri="http://test.example/class/1",
            start=10,
            end=20,
            agent="http://test.example/agent/1",
            agent_type="http://test.example/agentType",
        )

        selector_part, selector_filter = ann._build_selector_parts(
            sparql_escape_uri("http://test.example/sr/1"),
            sparql_escape_uri("http://test.example/source/1"),
        )
        assert "oa:start 10" in selector_part
        assert "oa:end 20" in selector_part

    def test_build_selector_parts_without_positions(self):
        from decide_ai_service_base.annotation import NERAnnotation

        ann = NERAnnotation(
            activity_id="http://test.example/activity/1",
            source_uri="http://test.example/source/1",
            class_uri="http://test.example/class/1",
            start=None,
            end=None,
            agent="http://test.example/agent/1",
            agent_type="http://test.example/agentType",
        )

        selector_part, selector_filter = ann._build_selector_parts(
            sparql_escape_uri("http://test.example/sr/1"),
            sparql_escape_uri("http://test.example/source/1"),
        )
        assert "oa:start" not in selector_part
        assert "oa:end" not in selector_part

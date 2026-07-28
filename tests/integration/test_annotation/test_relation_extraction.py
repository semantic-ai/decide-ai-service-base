import pytest

from helpers import query, update
from escape_helpers import sparql_escape_uri
from decide_ai_service_base.sparql_config import GRAPHS


@pytest.fixture(autouse=True)
def cleanup_relation_data():
    yield
    try:
        update(f"""
            DELETE WHERE {{
                GRAPH {sparql_escape_uri(GRAPHS["ai"])} {{
                    ?s <http://mu.semte.ch/vocabularies/ext/testRelationCleanup> "true" .
                    ?s ?p ?o .
                }}
            }}
        """)
    except Exception:
        pass


class TestRelationExtractionAnnotation:
    def test_add_to_triplestore(self):
        from decide_ai_service_base.annotation import RelationExtractionAnnotation

        activity_id = "http://test.example/activity/rel-1"
        source_uri = "http://test.example/expression/rel-1"
        agent = "http://test.example/agent/1"
        test_flag = "http://mu.semte.ch/vocabularies/ext/testRelationCleanup"

        update(f"""
            INSERT DATA {{
                GRAPH {sparql_escape_uri(GRAPHS["ai"])} {{
                    {sparql_escape_uri(activity_id)} <{test_flag}> "true" .
                }}
            }}
        """)

        ann = RelationExtractionAnnotation(
            subject="http://dbpedia.org/resource/Brussels",
            predicate="<http://www.w3.org/ns/locn#geometry>",
            obj=sparql_escape_uri("http://data.lblod.info/id/geometries/1"),
            activity_id=activity_id,
            source_uri=source_uri,
            start=0,
            end=20,
            agent=agent,
            agent_type="http://test.example/agentType",
            confidence=0.9,
        )

        annotation_uri = ann.add_to_triplestore_if_not_exists()
        assert annotation_uri.startswith("http://data.lblod.info/id/annotations/")

        result = query(f"""
            SELECT ?ann WHERE {{
                GRAPH {sparql_escape_uri(GRAPHS["ai"])} {{
                    ?ann a <http://www.w3.org/ns/oa#Annotation> ;
                         <http://www.w3.org/ns/oa#motivatedBy> <http://www.w3.org/ns/oa#linking> .
                }}
            }}
        """)
        bindings = result["results"]["bindings"]
        assert len(bindings) >= 1

    def test_init_sets_attributes(self):
        from decide_ai_service_base.annotation import RelationExtractionAnnotation

        ann = RelationExtractionAnnotation(
            subject="http://example.org/subject",
            predicate="<http://example.org/predicate>",
            obj='"""object value"""',
            activity_id="http://test.example/activity/1",
            source_uri="http://test.example/source/1",
            start=5,
            end=15,
            agent="http://test.example/agent/1",
            agent_type="http://test.example/agentType",
            confidence=0.85,
        )

        assert ann.subject == "http://example.org/subject"
        assert ann.predicate == "<http://example.org/predicate>"
        assert ann.object == '"""object value"""'
        assert ann.confidence == 0.85

    def test_build_statement_parts(self):
        from decide_ai_service_base.annotation import RelationExtractionAnnotation

        ann = RelationExtractionAnnotation(
            subject="http://example.org/subject",
            predicate="<http://example.org/predicate>",
            obj='"""object value"""',
            activity_id="http://test.example/activity/1",
            source_uri="http://test.example/source/1",
            start=5,
            end=15,
            agent="http://test.example/agent/1",
            agent_type="http://test.example/agentType",
        )

        stmt_parts, stmt_filter = ann._build_statement_parts(
            sparql_escape_uri("http://test.example/statement/1"),
            sparql_escape_uri("http://example.org/subject"),
            "<http://example.org/predicate>",
            '"""object value"""',
        )

        assert "rdf:Statement" in stmt_parts
        assert "rdf:subject" in stmt_parts
        assert "rdf:predicate" in stmt_parts
        assert "rdf:object" in stmt_parts

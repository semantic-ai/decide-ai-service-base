from decide_ai_service_base.sparql_config import (
    get_prefix_section,
    get_prefixes_for_query,
    SPARQL_PREFIXES,
    GRAPHS,
    JOB_STATUSES,
    TASK_OPERATIONS,
    LANGUAGE_CODE_TO_URI,
    LANGUAGE_URI_TO_CODE,
    ONTOLOGY_CLASSES,
    AGENT_TYPES,
)


class TestGetPrefixSection:
    def test_returns_string(self):
        result = get_prefix_section()
        assert isinstance(result, str)

    def test_contains_all_prefixes(self):
        result = get_prefix_section()
        for prefix in SPARQL_PREFIXES:
            assert f"PREFIX {prefix}:" in result

    def test_contains_all_uris(self):
        result = get_prefix_section()
        for uri in SPARQL_PREFIXES.values():
            assert uri in result

    def test_ends_with_newline(self):
        result = get_prefix_section()
        assert result.endswith("\n")

    def test_format_is_correct(self):
        result = get_prefix_section()
        for prefix, uri in SPARQL_PREFIXES.items():
            assert f"PREFIX {prefix}: <{uri}>" in result


class TestGetPrefixesForQuery:
    def test_single_prefix(self):
        result = get_prefixes_for_query("mu")
        assert "PREFIX mu: <http://mu.semte.ch/vocabularies/core/>" in result

    def test_multiple_prefixes(self):
        result = get_prefixes_for_query("mu", "oa", "prov")
        assert "PREFIX mu:" in result
        assert "PREFIX oa:" in result
        assert "PREFIX prov:" in result

    def test_unknown_prefix_only_raises(self):
        import pytest
        with pytest.raises(ValueError, match="No valid prefixes found"):
            get_prefixes_for_query("unknown_prefix")

    def test_unknown_prefix_raises(self):
        import pytest
        with pytest.raises(ValueError, match="No valid prefixes found"):
            get_prefixes_for_query("nonexistent")

    def test_mixed_known_and_unknown(self):
        result = get_prefixes_for_query("mu", "nonexistent", "oa")
        assert "PREFIX mu:" in result
        assert "PREFIX oa:" in result
        assert "PREFIX nonexistent:" not in result

    def test_empty_result_raises(self):
        import pytest
        with pytest.raises(ValueError):
            get_prefixes_for_query()

    def test_ends_with_newline(self):
        result = get_prefixes_for_query("mu")
        assert result.endswith("\n")


class TestConstants:
    def test_graphs_has_required_keys(self):
        required = ["public", "jobs", "data_containers", "ai"]
        for key in required:
            assert key in GRAPHS

    def test_job_statuses_has_all_states(self):
        required = ["scheduled", "busy", "success", "failed"]
        for state in required:
            assert state in JOB_STATUSES
            assert JOB_STATUSES[state].endswith(state)

    def test_task_operations_has_expected_keys(self):
        assert "entity_extraction" in TASK_OPERATIONS
        assert "translation" in TASK_OPERATIONS
        assert "geo_extraction" in TASK_OPERATIONS

    def test_language_code_to_uri(self):
        assert LANGUAGE_CODE_TO_URI["nl"].endswith("NLD")
        assert LANGUAGE_CODE_TO_URI["en"].endswith("ENG")
        assert LANGUAGE_CODE_TO_URI["de"].endswith("DEU")

    def test_language_uri_to_code_is_inverse(self):
        for code, uri in LANGUAGE_CODE_TO_URI.items():
            assert LANGUAGE_URI_TO_CODE[uri] == code

    def test_ontology_classes_has_expected_keys(self):
        assert "location" in ONTOLOGY_CLASSES
        assert "annotation" in ONTOLOGY_CLASSES
        assert "ai_component" in ONTOLOGY_CLASSES

    def test_agent_types(self):
        assert "person" in AGENT_TYPES
        assert "ai_component" in AGENT_TYPES

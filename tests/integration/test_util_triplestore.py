import pytest

from helpers import query, update
from escape_helpers import sparql_escape_uri, sparql_escape_string
from decide_ai_service_base.sparql_config import (
    get_prefixes_for_query,
    GRAPHS,
    JOB_STATUSES,
)
from decide_ai_service_base.task import Task


PREFIXES = get_prefixes_for_query("task", "adms", "dct")
JOBS_GRAPH = sparql_escape_uri(GRAPHS["jobs"])
DATA_GRAPH = sparql_escape_uri(GRAPHS["data_containers"])


@pytest.fixture(autouse=True)
def cleanup_util_data():
    """Clean up test data after each test."""
    yield
    try:
        update(f"""
            DELETE WHERE {{
                GRAPH {JOBS_GRAPH} {{
                    ?s <http://mu.semte.ch/vocabularies/ext/testUtilCleanup> "true" .
                    ?s ?p ?o .
                }}
            }}
        """)
    except Exception:
        pass
    try:
        update(f"""
            DELETE WHERE {{
                GRAPH {DATA_GRAPH} {{
                    ?s <http://mu.semte.ch/vocabularies/ext/testUtilCleanup> "true" .
                    ?s ?p ?o .
                }}
            }}
        """)
    except Exception:
        pass


@pytest.fixture
def registered_task_class():
    """Register a concrete task class for testing."""
    from unittest.mock import patch

    class TestTask(Task):
        __task_type__ = "http://test.example/operation/test-task"
        def process(self):
            pass

    with patch.object(Task, "supported_operations", return_value=[TestTask]):
        yield TestTask


class TestGetOneOpenTask:
    def setup_method(self):
        """Clean up before each test."""
        try:
            update(f"""
                DELETE WHERE {{
                GRAPH {JOBS_GRAPH} {{
                    ?s <http://mu.semte.ch/vocabularies/ext/testUtilCleanup> "true" .
                    ?s ?p ?o .
                }}
            }}
            """)
        except Exception:
            pass

    def test_returns_scheduled_task(self, registered_task_class):
        from decide_ai_service_base.util import get_one_open_task

        task_uri = "http://test.example/util-task/1"
        test_flag = "http://mu.semte.ch/vocabularies/ext/testUtilCleanup"

        update(f"""
            {PREFIXES}
            INSERT DATA {{
                GRAPH {JOBS_GRAPH} {{
                    {sparql_escape_uri(task_uri)} a task:Task ;
                        adms:status {sparql_escape_uri(JOB_STATUSES["scheduled"])} ;
                        task:operation <{registered_task_class.__task_type__}> ;
                        <{test_flag}> "true" .
                }}
            }}
        """)

        try:
            result = get_one_open_task()
            assert result == task_uri
        finally:
            update(f"""
                DELETE WHERE {{
                    GRAPH {JOBS_GRAPH} {{
                        <{task_uri}> <{test_flag}> ?v .
                        <{task_uri}> ?p ?o .
                    }}
                }}
            """)

    def test_returns_none_when_no_open_tasks(self, registered_task_class):
        from decide_ai_service_base.util import get_one_open_task
        result = get_one_open_task()
        assert result is None


class TestFailBusyAndScheduledTasks:
    def test_fails_busy_tasks(self, registered_task_class):
        from decide_ai_service_base.util import fail_busy_and_scheduled_tasks

        task_uri = "http://test.example/util-task/fail-1"
        test_flag = "http://mu.semte.ch/vocabularies/ext/testUtilCleanup"

        update(f"""
            {PREFIXES}
            INSERT DATA {{
                GRAPH {JOBS_GRAPH} {{
                    {sparql_escape_uri(task_uri)} a task:Task ;
                        adms:status {sparql_escape_uri(JOB_STATUSES["busy"])} ;
                        task:operation <{registered_task_class.__task_type__}> ;
                        dct:isPartOf <http://test.example/job/1> ;
                        <{test_flag}> "true" .
                }}
            }}
        """)

        try:
            fail_busy_and_scheduled_tasks()

            result = query(f"""
                {PREFIXES}
                SELECT ?status WHERE {{
                    GRAPH {JOBS_GRAPH} {{
                        {sparql_escape_uri(task_uri)} adms:status ?status .
                    }}
                }}
            """)
            bindings = result["results"]["bindings"]
            assert len(bindings) == 1
            assert bindings[0]["status"]["value"] == JOB_STATUSES["failed"]
        finally:
            update(f"""
                DELETE WHERE {{
                    GRAPH {JOBS_GRAPH} {{
                        <{task_uri}> <{test_flag}> ?v .
                        <{task_uri}> ?p ?o .
                    }}
                }}
            """)


class TestWriteErrorLog:
    def test_write_error_log_creates_container(self):
        from decide_ai_service_base.util import write_error_log

        task_uri = "http://test.example/util-task/error-1"
        test_flag = "http://mu.semte.ch/vocabularies/ext/testUtilCleanup"

        update(f"""
            {PREFIXES}
            INSERT DATA {{
                GRAPH {JOBS_GRAPH} {{
                    {sparql_escape_uri(task_uri)} a task:Task ;
                        <{test_flag}> "true" .
                }}
            }}
        """)

        try:
            container_uri = write_error_log(task_uri, "Test error message")
            assert container_uri.startswith("http://data.lblod.info/id/data-container/")

            result = query(f"""
                {PREFIXES}
                SELECT ?container WHERE {{
                    GRAPH {DATA_GRAPH} {{
                        {sparql_escape_uri(task_uri)} task:resultsContainer ?container .
                    }}
                }}
            """)
            bindings = result["results"]["bindings"]
            assert len(bindings) >= 1
            assert any(b["container"]["value"] == container_uri for b in bindings)
        finally:
            update(f"""
                DELETE WHERE {{
                    GRAPH {JOBS_GRAPH} {{
                        <{task_uri}> <{test_flag}> ?v .
                        <{task_uri}> ?p ?o .
                    }}
                }}
            """)
            update(f"""
                DELETE WHERE {{
                    GRAPH {DATA_GRAPH} {{
                        <{container_uri}> ?p ?o .
                    }}
                }}
            """)

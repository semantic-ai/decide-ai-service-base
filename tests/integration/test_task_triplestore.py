import datetime
from unittest.mock import patch

import pytest

from helpers import query, update
from escape_helpers import sparql_escape_uri, sparql_escape_datetime
from decide_ai_service_base.sparql_config import (
    get_prefixes_for_query,
    GRAPHS,
    JOB_STATUSES,
)


PREFIXES = get_prefixes_for_query("task", "adms")
JOBS_GRAPH = sparql_escape_uri(GRAPHS["jobs"])


@pytest.fixture(autouse=True)
def cleanup_task_data():
    """Clean up test task data after each test."""
    yield
    try:
        update(f"""
            DELETE WHERE {{
                GRAPH {JOBS_GRAPH} {{
                    ?s <http://mu.semte.ch/vocabularies/ext/testCleanup> "true" .
                    ?s ?p ?o .
                }}
            }}
        """)
    except Exception:
        pass


class TestTaskFromUri:
    @patch("decide_ai_service_base.task.Task.lookup")
    @patch("decide_ai_service_base.task.Task.supported_operations")
    def test_from_uri_finds_task(self, mock_ops, mock_lookup):
        from decide_ai_service_base.task import Task

        task_uri = "http://test.example/task/from-uri-1"
        test_flag = "http://mu.semte.ch/vocabularies/ext/testCleanup"

        class MockTask(Task):
            __task_type__ = "http://test.example/operation/mock"
            def process(self):
                pass

        mock_ops.return_value = [MockTask]
        mock_lookup.return_value = MockTask

        update(f"""
            {PREFIXES}
            INSERT DATA {{
                GRAPH {JOBS_GRAPH} {{
                    {sparql_escape_uri(task_uri)} a task:Task ;
                        task:operation <{MockTask.__task_type__}> ;
                        <{test_flag}> "true" .
                }}
            }}
        """)

        try:
            instance = Task.from_uri(task_uri)
            assert isinstance(instance, MockTask)
            assert instance.task_uri == task_uri
        finally:
            update(f"""
                DELETE WHERE {{
                    GRAPH {JOBS_GRAPH} {{
                        <{task_uri}> <{test_flag}> ?v .
                        <{task_uri}> ?p ?o .
                    }}
                }}
            """)

    @patch("decide_ai_service_base.task.Task.supported_operations")
    def test_from_uri_not_found(self, mock_ops):
        from decide_ai_service_base.task import Task

        mock_ops.return_value = []
        with pytest.raises(RuntimeError, match="not found"):
            Task.from_uri("http://test.example/task/nonexistent")


class TestTaskChangeState:
    @patch("decide_ai_service_base.task.Task.supported_operations")
    def test_change_state_transitions(self, mock_ops):
        from decide_ai_service_base.task import Task

        task_uri = "http://test.example/task/change-state-1"
        test_flag = "http://mu.semte.ch/vocabularies/ext/testCleanup"

        class ConcreteTask(Task):
            __task_type__ = "http://test.example/operation/change-state"
            def process(self):
                pass

        mock_ops.return_value = [ConcreteTask]

        update(f"""
            {PREFIXES}
            INSERT DATA {{
                GRAPH {JOBS_GRAPH} {{
                    {sparql_escape_uri(task_uri)} a task:Task ;
                        adms:status {sparql_escape_uri(JOB_STATUSES["scheduled"])} ;
                        <{test_flag}> "true" .
                }}
            }}
        """)

        try:
            task = ConcreteTask(task_uri)
            task.change_state("busy")

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
            assert bindings[0]["status"]["value"] == JOB_STATUSES["busy"]

            task.change_state("success")
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
            assert bindings[0]["status"]["value"] == JOB_STATUSES["success"]
        finally:
            update(f"""
                DELETE WHERE {{
                    GRAPH {JOBS_GRAPH} {{
                        <{task_uri}> <{test_flag}> ?v .
                        <{task_uri}> ?p ?o .
                    }}
                }}
            """)


class TestTaskRunContextManager:
    @patch("decide_ai_service_base.task.Task.supported_operations")
    def test_run_success_path(self, mock_ops):
        from decide_ai_service_base.task import Task

        task_uri = "http://test.example/task/run-success-1"
        test_flag = "http://mu.semte.ch/vocabularies/ext/testCleanup"

        class SuccessTask(Task):
            __task_type__ = "http://test.example/operation/success"
            def process(self):
                self.processed = True

        mock_ops.return_value = [SuccessTask]

        update(f"""
            {PREFIXES}
            INSERT DATA {{
                GRAPH {JOBS_GRAPH} {{
                    {sparql_escape_uri(task_uri)} a task:Task ;
                        adms:status {sparql_escape_uri(JOB_STATUSES["scheduled"])} ;
                        task:operation <{SuccessTask.__task_type__}> ;
                        <{test_flag}> "true" .
                }}
            }}
        """)

        try:
            task = SuccessTask(task_uri)
            processed = []

            with task.run():
                processed.append(True)

            assert processed == [True]
            assert task.start_time is not None
            assert task.end_time is not None
            assert task.duration_in_seconds >= 0
        finally:
            update(f"""
                DELETE WHERE {{
                    GRAPH {JOBS_GRAPH} {{
                        <{task_uri}> <{test_flag}> ?v .
                        <{task_uri}> ?p ?o .
                    }}
                }}
            """)

    @patch("decide_ai_service_base.task.Task.supported_operations")
    def test_run_failure_path(self, mock_ops):
        from decide_ai_service_base.task import Task

        task_uri = "http://test.example/task/run-fail-1"
        test_flag = "http://mu.semte.ch/vocabularies/ext/testCleanup"

        class FailingTask(Task):
            __task_type__ = "http://test.example/operation/fail"
            def process(self):
                raise ValueError("Intentional test failure")

        mock_ops.return_value = [FailingTask]

        update(f"""
            {PREFIXES}
            INSERT DATA {{
                GRAPH {JOBS_GRAPH} {{
                    {sparql_escape_uri(task_uri)} a task:Task ;
                        adms:status {sparql_escape_uri(JOB_STATUSES["scheduled"])} ;
                        task:operation <{FailingTask.__task_type__}> ;
                        <{test_flag}> "true" .
                }}
            }}
        """)

        try:
            task = FailingTask(task_uri)
            with pytest.raises(ValueError, match="Intentional test failure"):
                task.execute()

            assert task.end_time is not None
        finally:
            update(f"""
                DELETE WHERE {{
                    GRAPH {JOBS_GRAPH} {{
                        <{task_uri}> <{test_flag}> ?v .
                        <{task_uri}> ?p ?o .
                    }}
                }}
            """)

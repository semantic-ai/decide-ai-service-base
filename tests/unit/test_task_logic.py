import datetime
from unittest.mock import patch, MagicMock

import pytest


class TestTaskDurationInSeconds:
    @patch.dict("sys.modules", {"helpers": MagicMock(), "escape_helpers": MagicMock(),
                                "decide_ai_service_base.sparql_config": MagicMock()})
    def test_duration_returns_none_when_no_times(self):
        from decide_ai_service_base.task import Task

        class ConcreteTask(Task):
            __task_type__ = "test:task"
            def process(self):
                pass

        task = ConcreteTask("http://example.org/task/1")
        assert task.duration_in_seconds is None

    @patch.dict("sys.modules", {"helpers": MagicMock(), "escape_helpers": MagicMock(),
                                "decide_ai_service_base.sparql_config": MagicMock()})
    def test_duration_returns_none_when_only_start(self):
        from decide_ai_service_base.task import Task

        class ConcreteTask(Task):
            __task_type__ = "test:task"
            def process(self):
                pass

        task = ConcreteTask("http://example.org/task/1")
        task.start_time = datetime.datetime.now(datetime.timezone.utc)
        assert task.duration_in_seconds is None

    @patch.dict("sys.modules", {"helpers": MagicMock(), "escape_helpers": MagicMock(),
                                "decide_ai_service_base.sparql_config": MagicMock()})
    def test_duration_calculated_correctly(self):
        from decide_ai_service_base.task import Task

        class ConcreteTask(Task):
            __task_type__ = "test:task"
            def process(self):
                pass

        task = ConcreteTask("http://example.org/task/1")
        task.start_time = datetime.datetime(2024, 1, 1, 12, 0, 0, tzinfo=datetime.timezone.utc)
        task.end_time = datetime.datetime(2024, 1, 1, 12, 5, 30, tzinfo=datetime.timezone.utc)
        assert task.duration_in_seconds == 330


class TestTaskSupportedOperations:
    @patch.dict("sys.modules", {"helpers": MagicMock(), "escape_helpers": MagicMock(),
                                "decide_ai_service_base.sparql_config": MagicMock()})
    def test_supported_operations_returns_subclasses(self):
        from decide_ai_service_base.task import Task

        class OpA(Task):
            __task_type__ = "test:op-a"
            def process(self):
                pass

        class OpB(Task):
            __task_type__ = "test:op-b"
            def process(self):
                pass

        ops = Task.supported_operations()
        types = {op.__task_type__ for op in ops}
        assert "test:op-a" in types
        assert "test:op-b" in types

    @patch.dict("sys.modules", {"helpers": MagicMock(), "escape_helpers": MagicMock(),
                                "decide_ai_service_base.sparql_config": MagicMock()})
    def test_nested_subclasses(self):
        from decide_ai_service_base.task import Task

        class MidLevel(Task):
            def process(self):
                pass

        class LeafTask(MidLevel):
            __task_type__ = "test:leaf"
            def process(self):
                pass

        ops = Task.supported_operations()
        types = {op.__task_type__ for op in ops}
        assert "test:leaf" in types


class TestTaskLookup:
    @patch.dict("sys.modules", {"helpers": MagicMock(), "escape_helpers": MagicMock(),
                                "decide_ai_service_base.sparql_config": MagicMock()})
    def test_lookup_finds_matching_class(self):
        from decide_ai_service_base.task import Task

        class MyTask(Task):
            __task_type__ = "test:mytask"
            def process(self):
                pass

        found = Task.lookup("test:mytask")
        assert found is not None
        assert found.__task_type__ == "test:mytask"

    @patch.dict("sys.modules", {"helpers": MagicMock(), "escape_helpers": MagicMock(),
                                "decide_ai_service_base.sparql_config": MagicMock()})
    def test_lookup_returns_none_for_unknown(self):
        from decide_ai_service_base.task import Task
        found = Task.lookup("nonexistent:type")
        assert found is None


class TestTaskInit:
    @patch.dict("sys.modules", {"helpers": MagicMock(), "escape_helpers": MagicMock(),
                                "decide_ai_service_base.sparql_config": MagicMock()})
    def test_task_initial_state(self):
        from decide_ai_service_base.task import Task

        class ConcreteTask(Task):
            __task_type__ = "test:task"
            def process(self):
                pass

        task = ConcreteTask("http://example.org/task/1")
        assert task.task_uri == "http://example.org/task/1"
        assert task.results_container_uris == []
        assert task.source is None
        assert task.start_time is None
        assert task.end_time is None

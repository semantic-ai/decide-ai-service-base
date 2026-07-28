from decide_ai_service_base.schema import NotificationResponse, TaskOperationsResponse


class TestNotificationResponse:
    def test_create_with_values(self):
        resp = NotificationResponse(status="ok", message="Success")
        assert resp.status == "ok"
        assert resp.message == "Success"

    def test_model_dump(self):
        resp = NotificationResponse(status="error", message="Something failed")
        data = resp.model_dump()
        assert data == {"status": "error", "message": "Something failed"}

    def test_model_validate(self):
        data = {"status": "pending", "message": "In progress"}
        resp = NotificationResponse.model_validate(data)
        assert resp.status == "pending"
        assert resp.message == "In progress"

    def test_missing_fields_raises(self):
        import pytest
        from pydantic import ValidationError
        with pytest.raises(ValidationError):
            NotificationResponse(status="ok")

    def test_json_serialize(self):
        resp = NotificationResponse(status="ok", message="Done")
        import json
        j = json.loads(resp.model_dump_json())
        assert j["status"] == "ok"


class TestTaskOperationsResponse:
    def test_default_empty_list(self):
        resp = TaskOperationsResponse()
        assert resp.task_operations == []

    def test_with_operations(self):
        resp = TaskOperationsResponse(task_operations=["translate", "annotate"])
        assert resp.task_operations == ["translate", "annotate"]

    def test_model_dump(self):
        resp = TaskOperationsResponse(task_operations=["geo"])
        data = resp.model_dump()
        assert data == {"task_operations": ["geo"]}

    def test_type_validation(self):
        import pytest
        from pydantic import ValidationError
        with pytest.raises(ValidationError):
            TaskOperationsResponse(task_operations="not-a-list")

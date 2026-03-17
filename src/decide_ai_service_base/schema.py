from pydantic import BaseModel


class NotificationResponse(BaseModel):
    status: str
    message: str


class TaskOperationsResponse(BaseModel):
    task_operations: list[str] = []
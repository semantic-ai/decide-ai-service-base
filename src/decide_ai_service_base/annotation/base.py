import logging
from abc import ABC, abstractmethod
from typing import Iterator


class Annotation(ABC):
    """Base class for Open Annotation objects with provenance information."""

    def __init__(self, activity_id: str, source_uri: str, agent: str, agent_type: str):
        super().__init__()
        self.activity_id = activity_id
        self.source_uri = source_uri
        self.agent = agent
        self.agent_type = agent_type
        self.logger = logging.getLogger(__name__)

    @abstractmethod
    def add_to_triplestore_if_not_exists(self):
        """Insert this annotation into the triplestore."""
        pass

    @classmethod
    @abstractmethod
    def create_from_uri(cls, uri: str) -> Iterator['NERAnnotation']:
        """Create annotation instances from a URI in the triplestore."""
        pass
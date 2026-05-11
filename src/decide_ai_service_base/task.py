import contextlib
from datetime import datetime, timezone
import logging
from abc import ABC, abstractmethod
from string import Template
from typing import Optional, Type

from escape_helpers import sparql_escape_uri, sparql_escape_datetime
from helpers import query, update, log, logger

from .util import start_and_end_to_xsd_duration
from .sparql_config import get_prefixes_for_query, GRAPHS, JOB_STATUSES


class Task(ABC):
    """Base class for background tasks that process data from the triplestore."""

    def __init__(self, task_uri: str):
        super().__init__()
        self.task_uri = task_uri
        self.results_container_uris = []
        self.source: Optional[str] = None
        self.start_time: Optional[datetime] = None
        self.end_time: Optional[datetime] = None

    @property
    def duration(self) -> int | None:
        if self.end_time is not None and self.start_time is not None:
            return (self.end_time - self.start_time).total_seconds()
        return None

    @classmethod
    def supported_operations(cls) -> list[Type['Task']]:
        all_ops = []
        for subclass in cls.__subclasses__():
            if hasattr(subclass, '__task_type__'):
                all_ops.append(subclass)
            else:
                all_ops.extend(subclass.supported_operations())
        return all_ops

    @classmethod
    def lookup(cls, task_type: str) -> Optional['Task']:
        """
        Yield all subclasses of the given class, per:
        """
        for subclass in cls.supported_operations():
            if hasattr(subclass, '__task_type__') and subclass.__task_type__ == task_type:
                return subclass
        return None

    @classmethod
    def from_uri(cls, task_uri: str) -> 'Task':
        """Create a Task instance from its URI in the triplestore."""
        q = Template(
            get_prefixes_for_query("adms", "task") +
            """
            SELECT ?task ?taskType WHERE {
              VALUES ?task {
                $uri
              }
              ?task task:operation ?taskType .
            }
        """).substitute(uri=sparql_escape_uri(task_uri))
        for b in query(q, sudo=True).get('results').get('bindings'):
            candidate_cls = cls.lookup(b['taskType']['value'])
            if candidate_cls is not None:
                return candidate_cls(task_uri)
            raise RuntimeError(
                "Unknown task type {0}".format(b['taskType']['value']))
        raise RuntimeError("Task with uri {0} not found".format(task_uri))

    def change_state(self, new_state: str) -> None:
        """Update the task status in the triplestore."""

        # 1. Batch-insert results containers (if any)
        if self.results_container_uris:
            BATCH_SIZE = 50
            insert_template = Template(
                get_prefixes_for_query("task", "adms") +
                """
                INSERT DATA {
                GRAPH $graph {
                    $task $results_container_line .
                }
                }
                """
            )

            for i in range(0, len(self.results_container_uris), BATCH_SIZE):
                batch_uris = self.results_container_uris[i:i + BATCH_SIZE]
                results_container_line = " ;\n".join(
                    [f"task:resultsContainer {sparql_escape_uri(uri)}" for uri in batch_uris]
                )
                query_string = insert_template.substitute(
                    task=sparql_escape_uri(self.task_uri),
                    results_container_line=results_container_line,
                    graph=sparql_escape_uri(GRAPHS["jobs"])
                )
                update(query_string, sudo=True)
            
        # Calculate duration if start_time and end_time is available and we're transitioning to a terminal state
        duration = None
        if self.start_time and self.end_time and self.start_time and new_state in ("success", "failed"):
            duration = start_and_end_to_xsd_duration(self.end_time, self.start_time)
            
        duration_insert = f'?task schema:duration {sparql_escape_string(duration)}^^xsd:duration .' if duration else ""
        duration_delete = '?task schema:duration ?duration .' if duration else ""
        duration_optional = 'OPTIONAL { ?task schema:duration ?duration . }' if duration else ""

        # 2. Update any existing status
        update_query = Template(
            get_prefixes_for_query("task", "adms", "dct", "schema", "xsd") +
            """
            DELETE {
            GRAPH $graph {
                ?task adms:status ?status .
                ?task dct:modified ?modified.
                $duration_delete
            }
            }
            INSERT {
            GRAPH $graph {
                ?task adms:status $new_status .
                ?task dct:modified $modified .
                ?task schema:duration ?new_duration .
                $duration_insert
            }
            }
            WHERE {
            GRAPH $graph {
                VALUES ?task {
                  $task
                }
                ?task adms:status ?status .
                OPTIONAL { ?task dct:modified ?modified. }
                OPTIONAL { ?task schema:duration ?duration .}
                $duration_optional
            }
            }
            """
        )

        update(update_query.substitute(
            modified=sparql_escape_datetime(datetime.datetime.now()),
            new_status=sparql_escape_uri(JOB_STATUSES[new_state]),
            task=sparql_escape_uri(self.task_uri),
            graph=sparql_escape_uri(GRAPHS["jobs"]),
            duration_delete=duration_delete,
            duration_insert=duration_insert,
            duration_optional=duration_optional
        ), sudo=True)

    @contextlib.contextmanager
    def run(self):
        """Context manager for task execution with state transitions."""
        error_message = None
        new_state = None
        try:
            # This is the success path
            self.change_state("busy")
            self.start_time = datetime.now(timezone.utc)
            yield
            self.end_time = datetime.now(timezone.utc)
            new_state = "success"
        except Exception as e:
            # In case anything is wrong, write the error & prepare an error message
            from .util import write_error_log
            error_message = f"Task {self.task_uri} failed: {type(e).__name__}: {str(e)}"
            new_state = "failed"
            self.end_time = datetime.now(timezone.utc)
            logger.error(error_message, exc_info=True)
            raise
        finally:
            # Always update the state at the end, in case of faillure also write the error log to the triple store
            # Handle exceptions in case of a DB faillure.
            if new_state:
                try:
                    self.change_state(new_state)
                except Exception as state_error:
                    logger.error(f"Failed to update task {self.task_uri} status to {new_state}: {state_error}")

            if error_message and new_state == "failed":
                try:
                    write_error_log(self.task_uri, error_message)
                except Exception:
                    logger.error(f"Failed to write error log for task {self.task_uri}")

            log("Task {0} ended (final status {2}, duration {1} seconds)".format(self.task_uri, self.duration, new_state))

    def execute(self):
        """Run the task and handle state transitions."""
        with self.run():
            self.process()

    @abstractmethod
    def process(self):
        """Process task data (implemented by subclasses)."""
        pass

    def fetch_expression_data(self, expression_uri: str) -> str:
        """
        Retrieve text content for a specific expression URI.
        """
        query_template = Template(
            get_prefixes_for_query("eli", "eli-dl", "dct", "epvoc") +
            """
            SELECT DISTINCT ?title ?description ?decision_basis ?content
            WHERE {
            GRAPH ?graph {
                VALUES ?s { $expression }
                OPTIONAL { ?s eli:title ?title }
                OPTIONAL { ?s eli:description ?description }
                OPTIONAL { ?s eli-dl:decision_basis ?decision_basis }
                OPTIONAL { ?s epvoc:expressionContent ?content }
            }
            }
            """
        )

        query_result = query(
            query_template.substitute(expression=sparql_escape_uri(expression_uri)),
            sudo=True
        )

        bindings = query_result.get("results", {}).get("bindings", [])
        texts: list[str] = []
        seen = set()

        for binding in bindings:
            for field in ("content", "title", "description", "decision_basis"):
                value = binding.get(field, {}).get("value")
                if value and value not in seen:
                    texts.append(value)
                    seen.add(value)

        return "\n".join(texts)

    def resolve_projection_context(
            self,
            translated_expression_uri: str,
            translated_text: Optional[str] = None
    ) -> tuple[str, str]:
        """
        Resolve the original/source expression URI + text for a translated expression.
        Falls back to the translated expression itself if no source can be resolved.
        Does not mutate task-level source state.
        """
        source_uri: Optional[str] = None

        # 1) Prefer provenance from TranslationTask's eli:realizes annotation
        provenance_q = Template(
            get_prefixes_for_query("oa", "rdf", "eli") +
            """
            SELECT DISTINCT ?source WHERE {{
            GRAPH $graph {{
                ?ann a oa:Annotation ;
                    oa:motivatedBy oa:linking ;
                    oa:hasBody ?stmt ;
                    oa:hasTarget ?target .

                ?stmt a rdf:Statement ;
                    rdf:subject $translated ;
                    rdf:predicate eli:realizes ;
                    rdf:object ?work .

                ?target a oa:SpecificResource ;
                        oa:hasSource ?source .

                FILTER(?source != $translated)
            }}
            }}
            LIMIT 1
            """
        ).substitute(
            translated=sparql_escape_uri(translated_expression_uri),
            graph=sparql_escape_uri(GRAPHS["ai"])
        )

        provenance_bindings = query(provenance_q, sudo=True).get("results", {}).get("bindings", [])
        if provenance_bindings and "source" in provenance_bindings[0]:
            source_uri = provenance_bindings[0]["source"]["value"]

        if not source_uri:
            source_uri = translated_expression_uri

        if source_uri == translated_expression_uri:
            source_text = translated_text or self.fetch_expression_data(source_uri)
        else:
            source_text = self.fetch_expression_data(source_uri)

        return source_uri, source_text


class DecisionTask(Task, ABC):
    """Task that processes decision-making data with input and output containers."""

    def __init__(self, task_uri: str):
        super().__init__(task_uri)
        self.source_graph: Optional[str] = None

        q = Template(
            get_prefixes_for_query("dct", "task", "nfo") +
            """
        SELECT ?source WHERE {
          VALUES ?t {
            $task
          }
          ?t a task:Task .
          OPTIONAL { 
            ?t task:inputContainer ?ic . 
            OPTIONAL { ?ic a nfo:DataContainer ; task:hasResource ?source . }
          }
        }
        """).substitute(task=sparql_escape_uri(task_uri))
        r = query(q, sudo=True)
        bindings = r.get("results", {}).get("bindings", [])
        if not bindings or "source" not in bindings[0] or "value" not in bindings[0].get("source", {}):
            logger.warning(f"No source found for task {task_uri}")
            self.source = None
        else:
            self.source = bindings[0]["source"]["value"]

    def fetch_data(self) -> str:
        """Retrieve the input data for this task from the triplestore."""
        query_template = Template(
            get_prefixes_for_query("eli", "eli-dl", "dct", "epvoc") +
            """
            SELECT DISTINCT ?graph ?title ?description ?decision_basis ?content ?lang
            WHERE {
              GRAPH ?graph {
                VALUES ?s {
                  $source
                }
                ?s a ?thing .
                OPTIONAL { ?s eli:title ?title }
                OPTIONAL { ?s eli:description ?description }
                OPTIONAL { ?s eli-dl:decision_basis ?decision_basis }
                OPTIONAL { ?s epvoc:expressionContent ?content }
                OPTIONAL { ?s dct:language ?lang }
              }
            }
        """)

        query_result = query(query_template.substitute(
            source=sparql_escape_uri(self.source)
        ), sudo=True)

        bindings = query_result.get("results", {}).get("bindings", [])
        texts: list[str] = []
        seen = set()
        for binding in bindings:
            # Cache the graph of the source expression so we can reuse it later
            if not self.source_graph:
                self.source_graph = binding.get("graph", {}).get("value")
            for field in ("content", "title", "description", "decision_basis"):
                value = binding.get(field, {}).get("value")
                if value and value not in seen:
                    texts.append(value)
                    seen.add(value)

        return "\n".join(texts)

    def fetch_work_uri(self) -> Optional[str]:
        """
        Retrieve the eli:work realized by this expression, if available.
        """
        query_template = Template(
            get_prefixes_for_query("eli") +
            """
            SELECT ?work WHERE {
              GRAPH ?g {
                $source eli:realizes ?work .
              }
            }
            LIMIT 1
            """
        )

        query_result = query(
            query_template.substitute(source=sparql_escape_uri(self.source)),
            sudo=True
        )
        bindings = query_result.get("results", {}).get("bindings", [])
        if bindings and "work" in bindings[0]:
            work_uri = bindings[0]["work"]["value"]
            logger.info(
                f"Found work {work_uri} for expression {self.source}")
            return work_uri

        logger.warning(
            f"No eli:realizes work found for expression {self.source}")
        return None

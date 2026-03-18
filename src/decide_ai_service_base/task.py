import contextlib
import logging
from abc import ABC, abstractmethod
from string import Template
from typing import Optional, Type

from escape_helpers import sparql_escape_uri
from helpers import query, update

from .sparql_config import get_prefixes_for_query, GRAPHS, JOB_STATUSES


class Task(ABC):
    """Base class for background tasks that process data from the triplestore."""

    def __init__(self, task_uri: str):
        super().__init__()
        self.task_uri = task_uri
        self.results_container_uris = []
        self.logger = logging.getLogger(self.__class__.__name__)
        self.source: Optional[str] = None

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
              ?task task:operation ?taskType .
              FILTER(?task = $uri)
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

        # 1. Delete any existing status
        delete_query = Template(
            get_prefixes_for_query("task", "adms") +
            """
            DELETE {
            GRAPH <""" + GRAPHS["jobs"] + """> {
                ?task adms:status ?status .
            }
            }
            WHERE {
            GRAPH <""" + GRAPHS["jobs"] + """> {
                BIND($task AS ?task)
                ?task adms:status ?status .
            }
            }
            """
        )

        update(delete_query.substitute(
            task=sparql_escape_uri(self.task_uri)
        ), sudo=True)

        # 2. Insert the new status
        insert_query = Template(
            get_prefixes_for_query("task", "adms") +
            """
            INSERT {
            GRAPH <""" + GRAPHS["jobs"] + """> {
                ?task adms:status <$new_status> .
            }
            }
            WHERE {
                BIND($task AS ?task)
            }
            """
        )

        update(insert_query.substitute(
            new_status=JOB_STATUSES[new_state],
            task=sparql_escape_uri(self.task_uri)
        ), sudo=True)

        # Batch-insert results containers (if any)
        if self.results_container_uris:
            BATCH_SIZE = 50
            insert_template = Template(
                get_prefixes_for_query("task", "adms") +
                """
                INSERT {
                GRAPH <""" + GRAPHS["jobs"] + """> {
                    ?task $results_container_line .
                }
                }
                WHERE {
                    BIND($task AS ?task)
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
                    results_container_line=results_container_line
                )
                update(query_string, sudo=True)

    @contextlib.contextmanager
    def run(self):
        """Context manager for task execution with state transitions."""
        self.change_state("busy")
        try:
            yield
            self.change_state("success")
        except Exception as e:
            self.logger.error(
                f"Task {self.task_uri} failed: {type(e).__name__}: {str(e)}", exc_info=True)
            try:
                self.change_state("failed")
            except Exception as state_error:
                self.logger.error(
                    f"Failed to update task {self.task_uri} status to failed: {state_error}")
            raise

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
            f"""
            SELECT DISTINCT ?source WHERE {{
            GRAPH <{GRAPHS["ai"]}> {{
                ?ann a oa:Annotation ;
                    oa:motivatedBy oa:linking ;
                    oa:hasBody ?stmt ;
                    oa:hasTarget ?target .

                ?stmt a rdf:Statement ;
                    rdf:subject $translated ;
                    rdf:predicate eli:realizes ;
                    rdf:object ?work .

                ?target a oa:SpecificResource ;
                        oa:source ?source .

                FILTER(?source != $translated)
            }}
            }}
            LIMIT 1
            """
        ).substitute(translated=sparql_escape_uri(translated_expression_uri))

        provenance_bindings = query(provenance_q, sudo=True).get("results", {}).get("bindings", [])
        if provenance_bindings and "source" in provenance_bindings[0]:
            source_uri = provenance_bindings[0]["source"]["value"]

        # 2) Fallback: any other expression that realizes the same work
        if not source_uri:
            fallback_q = Template(
                get_prefixes_for_query("eli") +
                """
                SELECT DISTINCT ?source WHERE {
                GRAPH ?g {
                    $translated eli:realizes ?work .
                    ?source a eli:Expression ;
                            eli:realizes ?work .
                    FILTER(?source != $translated)
                }
                }
                LIMIT 1
                """
            ).substitute(translated=sparql_escape_uri(translated_expression_uri))

            fallback_bindings = query(fallback_q, sudo=True).get("results", {}).get("bindings", [])
            if fallback_bindings and "source" in fallback_bindings[0]:
                source_uri = fallback_bindings[0]["source"]["value"]

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
            self.logger.warning(f"No source found for task {task_uri}")
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
            self.logger.info(
                f"Found work {work_uri} for expression {self.source}")
            return work_uri

        self.logger.warning(
            f"No eli:realizes work found for expression {self.source}")
        return None

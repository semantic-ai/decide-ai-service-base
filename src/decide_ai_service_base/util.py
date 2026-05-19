import datetime
import time
from string import Template

from escape_helpers import sparql_escape_uri, sparql_escape_string
from helpers import query, log, update, logger

from .sparql_config import get_prefixes_for_query, GRAPHS, JOB_STATUSES, prefixed_log
from .task import Task


def wait_for_triplestore():
    triplestore_live = False
    log("Waiting for triplestore...")
    while not triplestore_live:
        try:
            result = query(
                """
                SELECT ?s WHERE {
                ?s ?p ?o.
                } LIMIT 1""",
                sudo=True)
            if result["results"]["bindings"][0]["s"]["value"]:
                triplestore_live = True
            else:
                raise Exception("triplestore not ready yet...")
        except Exception as _e:
            log("Triplestore not live yet, retrying...")
            time.sleep(1)
    log("Triplestore ready!")


def process_open_tasks():
    logger.info("Checking for open tasks...")
    uri = get_one_open_task()
    while uri is not None:
        logger.info(f"Processing {uri}")
        try:
            task = Task.from_uri(uri)
            task.execute()
        except Exception as e:
            logger.error(f"Error processing task {uri}: {e}", exc_info=True)
        uri = get_one_open_task()


def get_one_open_task() -> str | None:
    # Format VALUES clause properly - each URI on its own line, properly escaped
    operations = "\n                ".join(sparql_escape_uri(value.__task_type__) for value in Task.supported_operations())
    q = f"""
        {get_prefixes_for_query("task", "adms")}
        SELECT ?task WHERE {{
        GRAPH {sparql_escape_uri(GRAPHS["jobs"])} {{
            VALUES ?targetOperations {{
                {operations}
            }}
            ?task adms:status {sparql_escape_uri(JOB_STATUSES["scheduled"])} ;
                  task:operation ?targetOperations .
        }}
        }}
        limit 1
    """
    try:
        results = query(q, sudo=True)
        bindings = results.get("results", {}).get("bindings", [])
        if bindings and "task" in bindings[0]:
            return bindings[0]["task"]["value"]
    except Exception as e:
        logger.error(f"Error querying for open tasks: {e}", exc_info=True)
    return None


def fail_busy_and_scheduled_tasks():
    """
    Fails all busy tasks for the given operations (or all if none provided).
    """
    prefixed_log("Startup: failing busy tasks if there are any")

    operations = Task.supported_operations()

    # Build the VALUES clause dynamically
    operations_values = " ".join(sparql_escape_uri(op.__task_type__) for op in operations)

    q = Template(
        get_prefixes_for_query("task", "adms", "dct") +
        f"""
        DELETE {{
            GRAPH $graph {{
                ?task adms:status ?status .
            }}
        }}
        INSERT {{
            GRAPH $graph {{
                ?task adms:status {sparql_escape_uri(JOB_STATUSES["failed"])} .
            }}
        }}
        WHERE {{
            GRAPH $graph {{
                VALUES ?operation {{
                    {operations_values}
                }}
                VALUES ?status {{
                    {sparql_escape_uri(JOB_STATUSES["busy"])}
                }}
                ?task a task:Task ;
                      dct:isPartOf ?job ;
                      task:operation ?operation ;
                      adms:status ?status .
            }}
        }}
        """
    ).substitute(graph=sparql_escape_uri(GRAPHS["jobs"]))

    update(q, sudo=True)

def write_error_log(task_uri, error_message):
    import uuid
    container_id = str(uuid.uuid4())
    container_uri = f"http://data.lblod.info/id/data-container/{container_id}"
    error_uuid = str(uuid.uuid4())
    error_uri = f"http://data.lblod.info/id/error-message/{error_uuid}"

    q = Template(
        get_prefixes_for_query("task", "nfo", "mu", "ext", "dct") +
        """
        INSERT DATA {
            GRAPH $graph {
                $task task:resultsContainer $container .
                $container a nfo:DataContainer ;
                    mu:uuid $uuid ;
                    task:hasResource $error_uri .
                $error_uri a ext:ErrorMessage ;
                    mu:uuid $error_uuid ;
                    dct:description $error .
            }
        }
        """
    ).substitute(
        graph=sparql_escape_uri(GRAPHS["data_containers"]),
        container=sparql_escape_uri(container_uri),
        uuid=sparql_escape_string(container_id),
        error=sparql_escape_string(error_message),
        error_uuid=sparql_escape_string(error_uuid),
        error_uri=sparql_escape_uri(error_uri),
        task=sparql_escape_uri(task_uri)
    )

    update(q, sudo=True)
    return container_uri

def start_and_end_to_xsd_duration(start: datetime, end: datetime) -> str:
    total_seconds = int((end - start).total_seconds())
    sign = "-" if total_seconds < 0 else ""
    total_seconds = abs(total_seconds)

    days, remainder = divmod(total_seconds, 86400)
    hours, remainder = divmod(remainder, 3600)
    minutes, seconds = divmod(remainder, 60)

    date_part = f"{days}D" if days else ""
    time_part = "".join([
        f"{hours}H" if hours else "",
        f"{minutes}M" if minutes else "",
        f"{seconds}S" if seconds else "",
    ])

    return f"{sign}P{date_part}" + (f"T{time_part}" if time_part else "T0S")
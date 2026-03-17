import logging
import time

from escape_helpers import sparql_escape_uri
from helpers import query, log

from .sparql_config import get_prefixes_for_query, GRAPHS, JOB_STATUSES, TASK_OPERATIONS
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
    logger = logging.getLogger(__name__)
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
    operations = "\n                ".join(sparql_escape_uri(value) for value in TASK_OPERATIONS.values())
    q = f"""
        {get_prefixes_for_query("task", "adms")}
        SELECT ?task WHERE {{
        GRAPH <{GRAPHS["jobs"]}> {{
            VALUES ?targetOperations {{
                {operations}
            }}
            ?task adms:status <{JOB_STATUSES["scheduled"]}> ;
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
        logger = logging.getLogger(__name__)
        logger.error(f"Error querying for open tasks: {e}", exc_info=True)
    return None
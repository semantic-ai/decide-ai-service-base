from fastapi import FastAPI
import builtins
import datetime
import re
import time
import yaml
from string import Template
from threading import Lock
from typing import Optional

from escape_helpers import sparql_escape_uri, sparql_escape_string
from helpers import query, log, update, logger

from .sparql_config import CONFIG_REPO_URL, COMPOSE_FILE, COMPOSE_SERVICE, BASE_AGENT_URI, FORCE_VERSIONED_AGENT_URI, get_prefixes_for_query, GRAPHS, JOB_STATUSES, prefixed_log
from .task import Task
import os
import json
from pathlib import Path

app: FastAPI = getattr(builtins, "app", FastAPI())
app.state.agent_uri = None
    
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


class TaskProcessor:
    def __init__(self, lock: Lock):
        super().__init__()
        self.lock = lock

    def __call__(self):
        logger.info("Checking for open tasks...")
        with self.lock:
            uri = get_one_open_task()
            while uri is not None:
                logger.info(f"Processing {uri}")
                try:
                    task = Task.from_uri(uri)
                    task.execute()
                except Exception as e:
                    logger.error(f"Error processing task {uri}: {e}", exc_info=True)
                uri = get_one_open_task()


def process_open_tasks(lock: Lock):
    processor = TaskProcessor(lock)
    processor()


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

# suffix in case there are multiple agents in this one service, ideally one service = one agent
def get_agent_uri(agent_suffix:str = ""):
    if app.state.agent_uri:
        return app.state.agent_uri + agent_suffix

    config_as_string = fetch_config()
    existing_agent_uri = get_existing_config_uri(config_as_string)

    if existing_agent_uri:
        app.state.agent_uri = existing_agent_uri
        return existing_agent_uri + agent_suffix
    
    import uuid
    agent_id = str(uuid.uuid4())
    agent_uri = BASE_AGENT_URI + "/"+ agent_id

    store_config_uri(agent_uri, config_as_string)
        
    app.state.agent_uri = agent_uri
    return agent_uri + agent_suffix

def get_existing_config_uri(config_as_string):
    q = Template(
        get_prefixes_for_query("foaf", "ext", "schema", "tcs", "prov") +
        """
        SELECT ?agent_uri WHERE {
            ?agent_uri a ext:AgentConfig ;
                        ext:agentConfig $config_as_string .
        }
        """
    ).substitute(
        config_as_string=sparql_escape_string(config_as_string),
    )

    res = query(q, sudo=True)
    return res["results"]["bindings"][0]["agent_uri"]["value"] if res["results"]["bindings"] else None

def store_config_uri(config_uri, config_as_string):
    q = Template(
        get_prefixes_for_query("foaf", "ext", "schema", "tcs", "prov") +
        """
        INSERT DATA {
            GRAPH $graph {
                ?configuration_uri a ext:AgentConfig ;
                        ext:agentConfig $config_as_string .
            }
        }
        """
    ).substitute(
        graph=sparql_escape_uri(GRAPHS["jobs"]),
        configuration_uri=sparql_escape_uri(config_uri),
        config_as_string=sparql_escape_string(config_as_string),
    )

    update(q, sudo=True)

def fetch_config():
    """
    Recursively concatenate the COMPOSE_FILE with the files in the /config directory with their file paths.
    """
    compose = {}
    with open(COMPOSE_FILE) as compose_file:
        raw_compose = yaml.safe_load(compose_file)
        compose = raw_compose["services"][COMPOSE_SERVICE]

    mounts = {}
    for volume in compose['volumes']:
        mount_point = volume.split(":")[1]

        if mount_point in ["/app", COMPOSE_FILE]:
            # safety: ignore /app path in case we're in dev mode, don't include the whole source
            # also ignore compose file, we only need the service part of it
            continue

        if Path(mount_point).is_file():
            track_file_content(mounts, mount_point)
        else:
            for root, _, files in os.walk(mount_point):
                for file in files:
                    track_file_content(mounts, file, root)

    return json.dumps({"compose": compose, "config": mounts}, sort_keys=True, indent=2)

def track_file_content(mounts, file, root="/"):
    file_path = Path(root) / file
    absolute_path = str(file_path)

    # Read the JSON file
    with open(file_path, "r", encoding="utf-8") as f:
        try:
            content = f.read()
            mounts[absolute_path] = content
        except json.JSONDecodeError as e:
            print(f"Error reading {file_path}: {e}")

def write_agent_info(service_base: str, agent_suffix:str = ""):
    agent_uri = get_agent_uri(agent_suffix)
    separator = ""
    if agent_suffix and not (agent_suffix.startswith("/") or agent_suffix.startswith("#")):
        separator = "/"

    repo_triples = ""
    if CONFIG_REPO_URL:
        # take everything before the /tree/ or /commit/ part of the url and put it in repo
        match = re.search(r'^(.*?)(?:/commit/|/tree/)', CONFIG_REPO_URL)
        repo = match.group(1) if match else CONFIG_REPO_URL
        repo_triples = Template(
        """
        $configuration_uri schema:url $config_repo_url .
        $configuration_uri schema:codeRepository $repo .
        """
        ).substitute(
            configuration_uri=sparql_escape_uri(agent_uri),
            config_repo_url=sparql_escape_uri(CONFIG_REPO_URL),
            repo=sparql_escape_uri(repo)
        )

    q = Template(
        get_prefixes_for_query("foaf", "ext", "schema", "tcs", "prov") +
        """
        INSERT DATA {
            GRAPH $graph {
                $configuration_uri a tcs:InstancePipelineComponent, foaf:Agent ;
                    prov:specializationOf $service_base .
                $repo_triples
            }
        }
        """
    ).substitute(
        graph=sparql_escape_uri(GRAPHS["jobs"]),
        configuration_uri=sparql_escape_uri(agent_uri),
        service_base=sparql_escape_uri(service_base+separator+agent_suffix),
        repo_triples=repo_triples
    )

    update(q, sudo=True)
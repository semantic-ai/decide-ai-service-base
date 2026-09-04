import builtins
import datetime
import hashlib
import json
import os
import re
import time
import uuid
from pathlib import Path
from string import Template
from threading import Lock
from typing import Optional

import yaml
from escape_helpers import sparql_escape_string, sparql_escape_uri
from fastapi import FastAPI
from helpers import log, logger, query, update

from .sparql_config import (BASE_AGENT_URI, BASE_CONFIG_URI, COMPOSE_FILE,
                            COMPOSE_SERVICE, IGNORE_MOUNT_REGEX,
                            GRAPHS, JOB_STATUSES,
                            get_prefixes_for_query, prefixed_log)
from .task import Task

app: FastAPI = getattr(builtins, "app", FastAPI())
# note only threadsafe during startup and for reading during service operation
# should be fine as you should register agent_uris during startup
app.state.agent_uris = {}
app.state.config_uri = None
    
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
    agent_uri = app.state.agent_uris.get(agent_suffix, None)
    if not agent_uri:
        raise ValueError("Requested agent uri, but the agent was not registered. Call write_agent_info at startup.")
    return agent_uri

def ensure_config_uri():
    if app.state.config_uri:
        return app.state.config_uri

    config = fetch_config()
    config_as_string = json.dumps(config, sort_keys=True, indent=2)
    sha256 = hashlib.sha256()
    sha256.update(config_as_string.encode())
    
    hashed_config = sha256.hexdigest()

    existing_config_uri = get_existing_config_uri(hashed_config)

    if existing_config_uri:
        app.state.config_uri = existing_config_uri
        return existing_config_uri
    
    config_id = str(uuid.uuid4())
    config_uri = BASE_CONFIG_URI + config_id

    store_config_uri(config_uri, config_id, config, hashed_config)
        
    app.state.config_uri = config_uri
    return config_uri

def get_existing_config_uri(hashed_config):
    q = Template(
        get_prefixes_for_query("ext") +
        """
        SELECT ?agent_config WHERE {
            ?agent_config a ext:AgentConfig ;
                        ext:configHash $hashed_config .
        }
        """
    ).substitute(
        hashed_config=sparql_escape_string(hashed_config),
    )

    res = query(q, sudo=True)
    return res["results"]["bindings"][0]["agent_config"]["value"] if res["results"]["bindings"] else None

def store_config_uri(config_uri, config_id, config, hashed_config):
    q = Template(
        get_prefixes_for_query("ext", "mu") +
        """
        INSERT DATA {
            GRAPH $graph {
                $configuration_uri a ext:AgentConfig ;
                        mu:uuid $config_id ;
                        ext:configHash $config_hash .
            }
        }
        """
    ).substitute(
        graph=sparql_escape_uri(GRAPHS["jobs"]),
        configuration_uri=sparql_escape_uri(config_uri),
        config_id=sparql_escape_string(config_id),
        config_hash=sparql_escape_string(hashed_config),
    )

    for key,value in config["config"].items():
        store_config_part(config_uri, value, key)
    
    compose = json.dumps(config["compose"], sort_keys=True, indent=2)
    store_config_part(config_uri, compose,"docker-compose.yml:service")

    update(q, sudo=True)

def store_config_part(config_uri, config, path):
    part_id = str(uuid.uuid4())
    part_uri = BASE_CONFIG_URI + "part/" + part_id;


    q = Template(
        get_prefixes_for_query("mu", "ext") +
        """
        INSERT DATA {
            GRAPH $graph {
                $configuration_uri ext:hasConfigFile $part_uri .
                
                $part_uri a ext:ConfigPart ;
                          mu:uuid $part_id ;
                          ext:configPath $config_path ;
                          ext:configText $config_as_string .
            }
        }
        """
    ).substitute(
        graph=sparql_escape_uri(GRAPHS["jobs"]),
        part_uri=sparql_escape_uri(part_uri),
        part_id=sparql_escape_string(part_id),
        configuration_uri=sparql_escape_uri(config_uri),
        config_path=sparql_escape_string(path),
        config_as_string=sparql_escape_string(config),
    )

    update(q, sudo=True)

def fetch_config():
    """
    Recursively concatenate the COMPOSE_FILE with the files in the /config directory with their file paths.
    """
    compose = {}
    try:
        with open(COMPOSE_FILE) as compose_file:
            raw_compose = yaml.safe_load(compose_file)
            compose = raw_compose["services"][COMPOSE_SERVICE]
    except:
        raise ValueError(f"Could not find valid compose file at {COMPOSE_FILE} with service {COMPOSE_SERVICE}. A compose file with a service is necessary to find the image version and configuration of the ai system used")

    mounts = {}
    for volume in compose.get('volumes', []):
        mount_point = volume.split(":")[1]
        
        if mount_point == COMPOSE_FILE or re.search(IGNORE_MOUNT_REGEX, mount_point):
            # safety: ignore /app and /data 
            # also ignore compose file, we only need the service part of it
            continue

        if Path(mount_point).is_file():
            track_file_content(mounts, mount_point)
        else:
            for root, _, files in os.walk(mount_point):
                for file in files:
                    track_file_content(mounts, file, root)

    return {"compose": compose, "config": mounts}

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

def write_agent_info(service_base: str, agent_suffix_or_subcomponent:str = ""):
    agent_config_uri = ensure_config_uri()

    agent_uri = ensure_agent_uri(agent_config_uri, service_base, agent_suffix_or_subcomponent)

    app.state.agent_uris[agent_suffix_or_subcomponent] = agent_uri

def ensure_agent_uri(agent_config_uri, service_base, agent_suffix_or_sub_component):
    [base_agent_uri, is_sub_component] = _build_base_agent_uri(service_base, agent_suffix_or_sub_component)
    q = Template(
        get_prefixes_for_query("foaf", "ext", "schema", "tcs", "prov") +
        """
        SELECT ?agent_uri {
            GRAPH $graph {
                ?agent_uri a tcs:InstancePipelineComponent, foaf:Agent ;
                    prov:specializationOf $service_base ;
                    ext:hasConfig $agent_config_uri .
            }
        } LIMIT 1
        """
    ).substitute(
        graph=sparql_escape_uri(GRAPHS["jobs"]),
        agent_config_uri=sparql_escape_uri(agent_config_uri),
        service_base=sparql_escape_uri(base_agent_uri)
    )

    result = query(q, sudo=True)

    agent_uri = result["results"]["bindings"][0]["agent_uri"]["value"] if result["results"]["bindings"] else None

    if agent_uri:
        return agent_uri    

    agent_id = str(uuid.uuid4())
    agent_uri = BASE_AGENT_URI + agent_id
    if agent_suffix_or_sub_component and not is_sub_component:
        agent_uri = agent_uri + "/" + agent_suffix_or_sub_component
    
    q = Template(
        get_prefixes_for_query("foaf", "ext", "schema", "tcs", "prov", "mu") +
        """
        INSERT DATA {
            GRAPH $graph {
                $agent_uri a tcs:InstancePipelineComponent, foaf:Agent ;
                    mu:uuid $agent_id ;
                    prov:specializationOf $base_agent_uri ;
                    ext:hasConfig $agent_config_uri .
            }
        }
        """
    ).substitute(
        graph=sparql_escape_uri(GRAPHS["jobs"]),
        agent_config_uri=sparql_escape_uri(agent_config_uri),
        base_agent_uri=sparql_escape_uri(base_agent_uri),
        agent_id=sparql_escape_string(agent_id),
        agent_uri=sparql_escape_uri(agent_uri)
    )

    update(q, sudo=True)
    
    return agent_uri

def _build_base_agent_uri(service_base, agent_suffix_or_sub_component):
    is_sub_component = False
    if agent_suffix_or_sub_component:
        if agent_suffix_or_sub_component.startswith("http://") or agent_suffix_or_sub_component.startswith("https://"):
            service_base = agent_suffix_or_sub_component
            is_sub_component = True
        elif not (agent_suffix_or_sub_component.startswith("/") or agent_suffix_or_sub_component.startswith("#")):
            service_base=f"{service_base}/{agent_suffix_or_sub_component}"
    return [service_base, is_sub_component]
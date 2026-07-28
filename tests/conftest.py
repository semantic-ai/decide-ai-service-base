import os
import sys
import pytest

# Ensure src/ is on the path so top-level helpers.py and escape_helpers.py are importable
SRC_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), "src")
if SRC_DIR not in sys.path:
    sys.path.insert(0, SRC_DIR)

# Set required environment variables before any module imports
os.environ.setdefault("MU_SPARQL_ENDPOINT", "http://localhost:8890/sparql")
os.environ.setdefault("MU_SPARQL_UPDATEPOINT", "http://localhost:8890/sparql")
os.environ.setdefault("MU_APPLICATION_GRAPH", "http://mu.semte.ch/graphs/jobs")
os.environ.setdefault("LOG_LEVEL", "WARNING")
os.environ.setdefault("LOG_SPARQL_ALL", "false")
os.environ.setdefault("LOG_SPARQL_QUERIES", "false")
os.environ.setdefault("LOG_SPARQL_UPDATES", "false")
os.environ.setdefault("MU_SPARQL_TIMEOUT", "30")

# Graph environment variables
os.environ.setdefault("PUBLIC_GRAPH", "http://mu.semte.ch/graphs/public")
os.environ.setdefault("JOB_GRAPH", "http://mu.semte.ch/graphs/jobs")
os.environ.setdefault("DATA_CONTAINER_GRAPH", "http://mu.semte.ch/graphs/data-containers")
os.environ.setdefault("HARVEST_COLLECTIONS_GRAPH", "http://mu.semte.ch/graphs/harvest-collections")
os.environ.setdefault("REMOTE_OBJECT_GRAPH", "http://mu.semte.ch/graphs/remote-objects")
os.environ.setdefault("FILES_GRAPH", "http://mu.semte.ch/graphs/files")
os.environ.setdefault("OPARL_TEMP_GRAPH", "http://mu.semte.ch/graphs/oparl-temp")
os.environ.setdefault("OSLO_TEMP_GRAPH", "http://mu.semte.ch/graphs/oslo-temp")
os.environ.setdefault("EXPRESSIONS_GRAPH", "http://mu.semte.ch/graphs/expressions")
os.environ.setdefault("WORKS_GRAPH", "http://mu.semte.ch/graphs/works")
os.environ.setdefault("MANIFESTATIONS_GRAPH", "http://mu.semte.ch/graphs/manifestations")
os.environ.setdefault("AI_GRAPH", "http://mu.semte.ch/graphs/ai")

# Agent versioning
os.environ.setdefault("BASE_AGENT_URI", "http://lblod.data.gift/id/components/")
os.environ.setdefault("BASE_CONFIG_URI", "http://lblod.data.gift/id/configurations/")
os.environ.setdefault("COMPOSE_FILE", "docker-compose.yaml")
os.environ.setdefault("COMPOSE_SERVICE", "ai")
os.environ.setdefault("IGNORE_MOUNT_REGEX", "^(/data)|(/app)")


@pytest.fixture(autouse=True)
def reset_pricing_cache():
    """Reset the pricing cache before each test."""
    import decide_ai_service_base.pricing as pricing_mod
    pricing_mod._PRICING_CACHE = None
    pricing_mod._PRICING_CACHE_TIME = 0
    yield
    pricing_mod._PRICING_CACHE = None
    pricing_mod._PRICING_CACHE_TIME = 0

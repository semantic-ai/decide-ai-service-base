# DECIDe AI Service Base package

Base package for implementing AI services in the template for the DECIDe project, containing shared code
for all service implementations.

Currently contains
- Implementations of the AI Annotation types defined in the project (= what all our services produce)
- Prefixes and other SparQL config
- Task base class, for implementing a Task in de Pipeline/Job framework
- Various utils

## Disclaimer
This package expects to be installed in the Python template (as it depends on code therein) and won't work when installed outside of this environment

## Installation
Go to the release page to download the wheel, and install with pip

## Agent Versioning

This package automatically tracks the version of the AI agent, as well as the config used for the agent. To this end, it uses the following environment variables:

- `BASE_AGENT_URI`: the base URI to create agent uris from by appending a uuid, defaults to `"http://lblod.data.gift/id/components/"`
- `BASE_CONFIG_URI`: the base URI to create agent configurations from, defaults to `"http://lblod.data.gift/id/configurations/"`
- `COMPOSE_FILE`: the location where the docker compose file that specifies this service's configuration is mounted at. Defaults to `"docker-compose.yaml"`.
- `COMPOSE_SERVICE`: the name of the service in that docker compose file, defaults to `"ai"`
- `IGNORE_MOUNT_REGEX`: a regex that specifies which volumes of the service are to be ignored when comparing and storing the configuration, defaults to `"^(/data)|(/app)"`

At service startup, services are required to register all their agent uris by running:

```python
  write_agent_info("http://lblod.data.gift/id/components/named-entity-linking/v1.0.0")
  # or in case there are multiple agents in this single service:
  write_agent_info("http://lblod.data.gift/id/components/named-entity-recognition/v1.0.0", "ner_extractor")
  write_agent_info("http://lblod.data.gift/id/components/named-entity-recognition/v1.0.0", "segmenter")
  write_agent_info("http://lblod.data.gift/id/components/named-entity-recognition/v1.0.0", "translator")
```

This will compute the config based on the `COMPOSE_FILE` and the volumes of the service in that file. It concatenates the config and the volumes and SHA-256 hashes them. If a config with that hash already exists, it is reused, else a new config is written and new versioned agent URIs are linked to that config.

Once the agents are registered, services can simply call:

```python
get_agent_uri()
# or in case there are multiple agents in this single service
get_agent_uri("ner_extractor")
```

to get the versioned agent uri to write annotation or other data for. This function will error if the agent hasn't been registered yet.

The format of the config data that is written to the triplestore is as follows

```SPARQL
PREFIX ext: <http://mu.semte.ch/vocabularies/ext/>
PREFIX mu: <http://mu.semte.ch/vocabularies/core/>
 
INSERT DATA {
    GRAPH <http://mu.semte.ch/graphs/harvesting> {
        <http://lblod.data.gift/id/configurations/81e5ad8d-471a-4e56-a506-da6f93852e92> a ext:AgentConfig ;
                mu:uuid """81e5ad8d-471a-4e56-a506-da6f93852e92""" ;
                ext:configHash """6bd0cf0b3bd176b7d32ae29ff21038ebbca1d9944f27514ffdf8ddd9b4584a89""" .
    }
};

PREFIX mu: <http://mu.semte.ch/vocabularies/core/>
PREFIX ext: <http://mu.semte.ch/vocabularies/ext/>
 
INSERT DATA {
    GRAPH <http://mu.semte.ch/graphs/harvesting> {
        <http://lblod.data.gift/id/configurations/81e5ad8d-471a-4e56-a506-da6f93852e92> ext:hasConfigFile <http://lblod.data.gift/id/configurations/part/99dfa4f4-b503-48c8-9bda-b0eff72364c1> .
        
        <http://lblod.data.gift/id/configurations/part/99dfa4f4-b503-48c8-9bda-b0eff72364c1> a ext:ConfigPart ;
                  mu:uuid """99dfa4f4-b503-48c8-9bda-b0eff72364c1""" ;
                  ext:configPath """/config/example/b.json""" ;
                  ext:configText """{ \"bar\": 12 }
""" .
    }
};

PREFIX foaf: <http://xmlns.com/foaf/0.1/>
PREFIX ext: <http://mu.semte.ch/vocabularies/ext/>
PREFIX schema: <https://schema.org/>
PREFIX tcs: <https://w3id.org/tcs#>
PREFIX prov: <http://www.w3.org/ns/prov#>
PREFIX mu: <http://mu.semte.ch/vocabularies/core/>

INSERT DATA {
    GRAPH <http://mu.semte.ch/graphs/harvesting> {
        <http://lblod.data.gift/id/components/9b5d565f-184c-41e7-81b7-92a7256abbb8> a tcs:InstancePipelineComponent, foaf:Agent ;
            mu:uuid """9b5d565f-184c-41e7-81b7-92a7256abbb8""" ;
            prov:specializationOf <http://lblod.data.gift/id/components/named-entity-linking/v1.0.0> ;
            ext:hasConfig <http://lblod.data.gift/id/configurations/81e5ad8d-471a-4e56-a506-da6f93852e92> .
    }
}
```

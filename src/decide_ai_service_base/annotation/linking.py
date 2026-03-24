import uuid
from string import Template
from typing import Iterator

from escape_helpers import sparql_escape_uri
from helpers import query, update

from .base import Annotation
from ..sparql_config import get_prefixes_for_query, GRAPHS


class LinkingAnnotation(Annotation):
    """Annotation linking a resource to a classification/class."""

    def __init__(self, activity_id: str, source_uri: str, class_uri: str, agent: str, agent_type: str):
        super().__init__(activity_id, source_uri, agent, agent_type)
        self.class_uri = class_uri

    @classmethod
    def create_from_uri(cls, uri: str) -> Iterator['LinkingAnnotation']:
        query_template = Template(
            get_prefixes_for_query("oa", "prov", "rdf") +
            """
        SELECT ?activity ?body ?agent ?agentType
        WHERE {
          VALUES ?target {
            $uri
          }
          
          VALUES ?motivation {
            oa:classifying
          }
          
          ?annotation a oa:Annotation ;
                       oa:hasTarget ?target .
          ?annotation oa:hasBody ?body.
          OPTIONAL { ?annotation oa:motivatedBy ?motivation . }

          OPTIONAL {
              ?activity a prov:Activity ;
              prov:generated ?annotation ;
              prov:wasAssociatedWith ?agent .

              OPTIONAL { ?agent rdf:type ?agentType . }
          }
        }
        """)
        query_result = query(
            query_template.substitute(
                uri=sparql_escape_uri(uri)
            ),
            sudo=True
        )

        if not query_result['results']['bindings']:
            return
            yield

        for item in query_result['results']['bindings']:
            yield cls(item['activity']['value'], uri, item['body']['value'], item['agent']['value'],
                      item['agentType']['value'])

    def add_to_triplestore_if_not_exists(self):
        query_template = Template(
            get_prefixes_for_query("ex", "oa", "mu", "prov", "foaf", "dct", "skolem", "nif") +
            """
            INSERT {
              GRAPH $graph {
                  $activity_id a prov:Activity;
                     prov:generated $annotation_id;
                     prov:wasAssociatedWith $user;
                     mu:uuid "$activity_uuid" .

                  $annotation_id a oa:Annotation ;
                                 mu:uuid "$id";
                                 oa:hasBody $clz ;
                                 nif:confidence 1 ;
                                 oa:motivatedBy oa:classifying ;
                                 oa:hasTarget $uri .
              }
            } WHERE {
              GRAPH $graph {
                  FILTER NOT EXISTS { 
                    ?existingAnn a oa:Annotation ;
                        oa:hasBody $clz ;
                        oa:motivatedBy oa:classifying ;
                        oa:hasTarget $uri .

                    ?existingAct a prov:Activity ;
                     prov:generated ?existingAnn ;
                     prov:wasAssociatedWith $user .
                  }
              }
            }
            """)
        query_string = query_template.substitute(
            id=str(uuid.uuid1()),
            activity_uuid=str(uuid.uuid4()),
            annotation_id=sparql_escape_uri("http://example.org/{0}".format(uuid.uuid4())),
            activity_id=sparql_escape_uri(self.activity_id),
            uri=sparql_escape_uri(self.source_uri),
            user=sparql_escape_uri(self.agent),
            clz=sparql_escape_uri(self.class_uri),
            graph=sparql_escape_uri(GRAPHS["ai"])
        )
        try:
            update(query_string, sudo=True)
        except Exception as e:
            error_msg = f"Failed to insert LinkingAnnotation to triplestore for source {self.source_uri}: {e}"
            self.logger.error(error_msg, exc_info=True)
            raise RuntimeError(error_msg) from e

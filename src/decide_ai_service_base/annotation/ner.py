import uuid
from string import Template
from typing import Iterator, Optional

from escape_helpers import sparql_escape_uri, sparql_escape_float
from helpers import query, update

from .base import Annotation
from ..sparql_config import get_prefixes_for_query, GRAPHS


class NERAnnotation(Annotation):
    """Named Entity Recognition annotation with text position selectors."""

    def __init__(self, activity_id: str, source_uri: str, class_uri: str, start: Optional[int], end: Optional[int],
                 agent: str, agent_type: str, confidence: float = 1.0):
        super().__init__(activity_id, source_uri, agent, agent_type)
        self.class_uri = class_uri
        self.start = start
        self.end = end
        self.confidence = confidence

    @classmethod
    def create_from_uri(cls, uri: str) -> Iterator['NERAnnotation']:
        query_template = Template(
            get_prefixes_for_query("oa", "prov", "rdf") +
            """
        SELECT ?activity ?body ?start ?end ?agent ?agentType
        WHERE {
          VALUES ?source {
            $uri
          }
          VALUES ?motivation {
            oa:tagging
          }
          
          ?annotation a oa:Annotation ;
                       oa:hasTarget ?target .
          ?target a oa:SpecificResource ;
                  oa:source ?source .
          OPTIONAL {
              ?target oa:selector ?selector .
              ?selector a oa:TextPositionSelector ;
                      oa:start ?start; oa:end ?end .
          }
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
        for item in query_result['results']['bindings']:
            start_val = int(item['start']['value']) if item.get('start') else None
            end_val = int(item['end']['value']) if item.get('end') else None
            yield cls(item['activity']['value'], uri, item['body']['value'], start_val,
                      end_val, item['agent']['value'], item['agentType']['value'])

    def add_to_triplestore_if_not_exists(self):
        # Generate URIs
        annotation_id = sparql_escape_uri("http://example.org/{0}".format(uuid.uuid4()))
        part_of_id = sparql_escape_uri("http://www.example.org/id/.well-known/genid/{0}".format(uuid.uuid4()))
        uri = sparql_escape_uri(self.source_uri)

        # Build selector parts with actual values substituted
        selector_part, selector_filter = self._build_selector_parts(part_of_id, uri)

        query_template = Template(
            get_prefixes_for_query("ex", "oa", "mu", "prov", "foaf", "dct", "skolem", "nif", "locn", "geosparql") +
            """
            INSERT {
              GRAPH $graph {
                  $activity_id a prov:Activity;
                     prov:generated $annotation_id;
                     prov:wasAssociatedWith $user;
                     mu:uuid "$activity_uuid" . 
                     .
                  $annotation_id a oa:Annotation ;
                                 mu:uuid "$id";
                                 oa:hasBody $clz ;
                                 nif:confidence $confidence ;
                                 oa:motivatedBy oa:tagging ;
                                 oa:hasTarget $part_of_id .

                  $selector_part

                  $extra
              }
            } WHERE {
              GRAPH $graph {
                  FILTER NOT EXISTS {
                    ?existingAnn a oa:Annotation ;
                        oa:hasBody $clz ;
                        oa:motivatedBy oa:tagging ;
                        oa:hasTarget ?existingTarget .

                    ?existingAct a prov:Activity ;
                     prov:generated ?existingAnn ;
                     prov:wasAssociatedWith $user .

                    $selector_filter
                  }
              }
            }
            """)
        query_string = query_template.substitute(
            id=str(uuid.uuid1()),
            annotation_id=annotation_id,
            activity_uuid=str(uuid.uuid4()),
            activity_id=sparql_escape_uri(self.activity_id),
            part_of_id=part_of_id,
            user=sparql_escape_uri(self.agent),
            clz=sparql_escape_uri(self.class_uri),
            confidence=sparql_escape_float(self.confidence),
            extra=self.get_extra_inserts(),
            selector_part=selector_part,
            selector_filter=selector_filter,
            graph=GRAPHS['ai']
        )

        try:
            update(query_string, sudo=True)
        except Exception as e:
            error_msg = f"Failed to insert NERAnnotation to triplestore for source {self.source_uri}: {e}"
            self.logger.error(error_msg, exc_info=True)
            raise RuntimeError(error_msg) from e

    def _build_selector_parts(self, part_of_id: str, uri: str):
        """Helper method to build selector SPARQL parts conditionally.

        Args:
            part_of_id: The escaped URI for the SpecificResource
            uri: The escaped source URI

        Returns tuple: (selector_part, selector_filter)
        """
        if self.start is not None and self.end is not None:
            selector_id = sparql_escape_uri("http://www.example.org/id/.well-known/genid/{0}".format(uuid.uuid4()))
            selector_part = f"""
                  {part_of_id} a oa:SpecificResource ;
                              oa:source {uri} ;
                              oa:selector {selector_id} .

                  {selector_id} a oa:TextPositionSelector ;
                               oa:start {self.start} ;
                               oa:end {self.end} ."""
            selector_filter = f"""
                    ?existingTarget a oa:SpecificResource ;
                        oa:source {uri} ;
                        oa:selector ?existingSelector .

                    ?existingSelector a oa:TextPositionSelector ;
                          oa:start {self.start} ;
                          oa:end {self.end} ."""
        else:
            selector_part = f"""
                  {part_of_id} a oa:SpecificResource ;
                              oa:source {uri} ."""
            selector_filter = f"""
                    ?existingTarget a oa:SpecificResource ;
                        oa:source {uri} .
                    FILTER NOT EXISTS {{ ?existingTarget oa:selector ?anySelector . }}"""
        return selector_part, selector_filter

    def _build_skolem_parts(self, skolem_uri: str, subject: str, predicate: str, object: str,
                            entity_class: Optional[str]) -> tuple[str, str]:
        """
        Helper method to build skolem SPARQL parts.

        Args:
            skolem_uri: The escaped URI for the skolemized statement
            subject: The escaped subject URI
            predicate: The predicate (already escaped or prefixed)
            object: The escaped object (URI or literal)
            entity_class: Optional entity class to determine additional triples

        Returns:
            A tuple of (skolem_parts, skolem_filter) strings to be included in the SPARQL query
        """
        sparql_class = None
        if entity_class and "date" not in entity_class.lower():
            if entity_class == "MANDATARY":
                sparql_class = "foaf:Person"
            elif entity_class == "ADMINISTRATIVE_BODY":
                sparql_class = "org:Organization"

        if sparql_class:
            entity_class_uuid = f"skolem:{uuid.uuid4()}"

            skolem_parts = f"""
                {skolem_uri} a rdf:Statement ;
                  rdf:subject {subject} ;
                  rdf:predicate {predicate} ;
                  rdf:object {entity_class_uuid} .

                {entity_class_uuid} a {sparql_class} ;
                  rdfs:label {object} .
                """
            skolem_filter = f"""
                ?existingSkolem a rdf:Statement ;
                  rdf:subject {subject} ;
                  rdf:predicate {predicate} ;
                  rdf:object ?existingObject .

                ?existingObject a {sparql_class} ;
                  rdfs:label {object} .
                """

        else:
            skolem_parts = f"""
                {skolem_uri} a rdf:Statement ;
                  rdf:subject {subject} ;
                  rdf:predicate {predicate} ;
                  rdf:object {object} .
                """
            skolem_filter = f"""
                ?existingSkolem a rdf:Statement ;
                  rdf:subject {subject} ;
                  rdf:predicate {predicate} ;
                  rdf:object {object} .
                """

        return skolem_parts, skolem_filter

    def get_extra_inserts(self) -> str:
        """Return additional SPARQL triples to insert for this annotation type."""
        return ""

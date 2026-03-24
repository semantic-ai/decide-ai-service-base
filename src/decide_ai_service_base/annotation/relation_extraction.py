import uuid
from string import Template
from typing import Iterator, Optional

from escape_helpers import sparql_escape_uri, sparql_escape_float
from helpers import query, update

from .ner import NERAnnotation
from ..sparql_config import get_prefixes_for_query, GRAPHS


class RelationExtractionAnnotation(NERAnnotation):
    """NER annotation representing an RDF statement (subject-predicate-object triple)."""

    def __init__(self, subject: str, predicate: str, obj: str, activity_id: str, source_uri: str,
                 start: Optional[int], end: Optional[int], agent: str, agent_type: str,
                 confidence: float = 1.0, entity_class: Optional[str] = None):
        super().__init__(activity_id, source_uri, predicate, start, end, agent, agent_type)
        self.predicate = predicate
        self.object = obj
        self.subject = subject
        self.confidence = confidence
        self.entity_class = entity_class

    @classmethod
    def create_from_uri(cls, uri: str) -> Iterator['RelationExtractionAnnotation']:
        query_template = Template(
            get_prefixes_for_query("oa", "prov", "rdf") +
            """
                SELECT ?activity ?start ?end ?agent ?agentType ?subj ?pred ?obj
                WHERE {
                  VALUES ?source {
                    $uri
                  }

                  VALUES ?motivation {
                    oa:linking
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
                  ?body a rdf:Statement ; rdf:subject ?subj; rdf:predicate ?pred; rdf:object ?obj .
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
            yield cls(item['subj']['value'], item['pred']['value'], item['obj']['value'], item['activity']['value'],
                      uri,
                      start_val, end_val, item['agent']['value'], item.get('agentType', {}).get('value'))

    def add_to_triplestore_if_not_exists(self) -> str:
        """
        Insert this annotation into the triplestore.

        Returns:
            The URI of the created annotation
        """
        annotation_uri = "http://example.org/{0}".format(uuid.uuid4())
        part_of_id = sparql_escape_uri("http://www.example.org/id/.well-known/genid/{0}".format(uuid.uuid4()))
        uri = sparql_escape_uri(self.source_uri)

        # Build skolem parts with actual values substituted
        skolem_uri = f"skolem:{uuid.uuid4()}"
        skolem_parts, skolem_filter = self._build_skolem_parts(
            skolem_uri,
            sparql_escape_uri(self.subject),
            self.predicate,
            self.object,
            self.entity_class
        )

        # Build selector parts with actual values substituted
        selector_part, selector_filter = self._build_selector_parts(
            part_of_id, uri)

        query_template = Template(
            get_prefixes_for_query("ex", "oa", "mu", "prov", "foaf", "dct", "skolem", "nif", "rdf", "eli", "org",
                                   "rdfs", "eli-dl") +
            """
            INSERT {
              GRAPH $graph {
                  $activity_id a prov:Activity;
                     prov:generated $annotation_id;
                     prov:wasAssociatedWith $user ;
                     mu:uuid "$activity_uuid" .

                  $annotation_id a oa:Annotation ;
                     mu:uuid "$id";
                     oa:hasBody $skolem ;
                     nif:confidence $confidence ;
                     oa:motivatedBy oa:linking ;
                     oa:hasTarget $part_of_id .
                  $skolem_parts
                  $selector_part
              }
            } WHERE {
              GRAPH $graph {
                  FILTER NOT EXISTS { 
                    ?existingAnn a oa:Annotation ;
                        oa:hasBody ?existingSkolem ;
                        oa:motivatedBy oa:linking ;
                        oa:hasTarget ?existingTarget .

                    ?existingAct a prov:Activity ;
                         prov:generated ?existingAnn ;
                         prov:wasAssociatedWith $user .

                    $skolem_filter
                    $selector_filter
                  }
              }
            }
            """)
        query_string = query_template.substitute(
            id=str(uuid.uuid1()),
            annotation_id=sparql_escape_uri(annotation_uri),
            activity_uuid=str(uuid.uuid4()),
            activity_id=sparql_escape_uri(self.activity_id),
            user=sparql_escape_uri(self.agent),
            skolem=skolem_uri,
            subject=sparql_escape_uri(self.subject),
            pred=self.predicate,  # Already escaped or prefixed name
            obj=self.object,  # Already escaped (string literal or URI)
            confidence=sparql_escape_float(self.confidence),
            part_of_id=part_of_id,
            skolem_parts=skolem_parts,
            selector_part=selector_part,
            skolem_filter=skolem_filter,
            selector_filter=selector_filter,
            graph=sparql_escape_uri(GRAPHS["ai"])
        )
        try:
            update(query_string, sudo=True)
        except Exception as e:
            error_msg = f"Failed to insert RelationExtractionAnnotation to triplestore for subject {self.subject}: {e}"
            self.logger.error(error_msg, exc_info=True)
            raise RuntimeError(error_msg) from e
        return annotation_uri

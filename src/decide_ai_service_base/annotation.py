import logging
from typing import Optional, Iterator, Any
from abc import ABC, abstractmethod
from string import Template
import uuid

from helpers import query, update
from escape_helpers import sparql_escape_uri, sparql_escape_string, sparql_escape_float
from .sparql_config import get_prefixes_for_query, GRAPHS


class Annotation(ABC):
    """Base class for Open Annotation objects with provenance information."""

    def __init__(self, activity_id: str, source_uri: str, agent: str, agent_type: str):
        super().__init__()
        self.activity_id = activity_id
        self.source_uri = source_uri
        self.agent = agent
        self.agent_type = agent_type
        self.logger = logging.getLogger(__name__)

    @abstractmethod
    def add_to_triplestore_if_not_exists(self):
        """Insert this annotation into the triplestore."""
        pass

    @classmethod
    @abstractmethod
    def create_from_uri(cls, uri: str) -> Iterator['NERAnnotation']:
        """Create annotation instances from a URI in the triplestore."""
        pass


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
          ?annotation a oa:Annotation ;
                       oa:hasTarget ?target .
          ?annotation oa:hasBody ?body.
          OPTIONAL { ?annotation oa:motivatedBy ?motivation . }

          # Example filter (uncomment and edit as needed):
          VALUES ?target {
            $uri
          }
          VALUES ?motivation {
            oa:classifying
          }

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

          # Example filter (uncomment and edit as needed):
          VALUES ?source {
            $uri
          }
          VALUES ?motivation {
            oa:tagging
          }

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
    
    def _build_skolem_parts(self, skolem_uri: str, subject: str, predicate: str, object: str, entity_class: Optional[str]) -> tuple[str, str]:
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


class GeoAnnotation(NERAnnotation):
    """NER annotation with geographic location data (GeoJSON)."""

    def __init__(self, geojson: dict, *args, **kwargs):
        super().__init__(*args, **kwargs)
        logging.debug(f"GeoAnnotation geojson: {geojson}")

        # Parse GeoJSON based on geometry type
        self.geojson = geojson
        self.geo_type = geojson.get("type", "Point")
        coords = geojson.get("coordinates", [])

        # Handle different geometry types
        if self.geo_type == "Point":
            # Point: [lon, lat]
            if len(coords) >= 2:
                self.geometry = f"{coords[0]} {coords[1]}"
                self.lat = coords[1]
                self.lon = coords[0]
            else:
                self.geometry = ""
                self.lat = 0
                self.lon = 0
        elif self.geo_type == "LineString":
            # LineString: [[lon, lat], [lon, lat], ...]
            self.geometry = ", ".join(f"{x} {y}" for x, y in coords)
            # Use first point as representative location
            if coords:
                self.lat = coords[0][1]
                self.lon = coords[0][0]
            else:
                self.lat = 0
                self.lon = 0
        elif self.geo_type == "Polygon":
            # Polygon: [[[lon, lat], [lon, lat], ...]]
            outer_ring = coords[0] if coords else []
            self.geometry = ", ".join(f"{x} {y}" for x, y in outer_ring)
            if outer_ring:
                self.lat = outer_ring[0][1]
                self.lon = outer_ring[0][0]
            else:
                self.lat = 0
                self.lon = 0
        else:
            # Fallback
            self.geometry = str(coords)
            self.lat = 0
            self.lon = 0

    def get_extra_inserts(self) -> str:
        # Choose WKT type based on geometry type
        if self.geo_type == "Point":
            wkt_geom = f"POINT({self.geometry})"
        elif self.geo_type == "LineString":
            wkt_geom = f"LINESTRING({self.geometry})"
        elif self.geo_type == "Polygon":
            wkt_geom = f"POLYGON(({self.geometry}))"
        else:
            wkt_geom = f"POINT({self.geometry})"

        return Template(
            """
            $body a dcterms:Location ;
              locn:geometry $geom .

            $geom a locn:Geometry ;
              geosparql:asWKT $wkt .
            """
        ).substitute(
            body=sparql_escape_uri(self.class_uri),
            wkt=sparql_escape_string(f"SRID=4326;{wkt_geom}^^geosparql:wktLiteral"),
            geom=sparql_escape_uri(f"http://data.lblod.info/id/geometries/{uuid.uuid4()}")
        )


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

                  # Example filter (uncomment and edit as needed):
                  FILTER(?source = $uri)
                  FILTER(?motivation = oa:linking)

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
            get_prefixes_for_query("ex", "oa", "mu", "prov", "foaf", "dct", "skolem", "nif", "rdf", "eli", "org", "rdfs") +
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
            error_msg = f"Failed to insert TripletAnnotation to triplestore for subject {self.subject}: {e}"
            self.logger.error(error_msg, exc_info=True)
            raise RuntimeError(error_msg) from e
        return annotation_uri

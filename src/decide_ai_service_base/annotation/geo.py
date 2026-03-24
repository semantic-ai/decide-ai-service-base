import uuid
from string import Template
import logging

from .ner import NERAnnotation

from escape_helpers import sparql_escape_uri, sparql_escape_string
from helpers import query, update


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

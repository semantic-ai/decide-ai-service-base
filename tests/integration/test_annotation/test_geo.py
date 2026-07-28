import pytest

from helpers import query, update
from escape_helpers import sparql_escape_uri
from decide_ai_service_base.sparql_config import GRAPHS


@pytest.fixture(autouse=True)
def cleanup_geo_data():
    yield
    try:
        update(f"""
            DELETE WHERE {{
                GRAPH {sparql_escape_uri(GRAPHS["ai"])} {{
                    ?s <http://mu.semte.ch/vocabularies/ext/testGeoCleanup> "true" .
                    ?s ?p ?o .
                }}
            }}
        """)
    except Exception:
        pass


class TestGeoAnnotation:
    def test_point_geometry(self):
        from decide_ai_service_base.annotation import GeoAnnotation

        geojson = {
            "type": "Point",
            "coordinates": [3.7172, 51.2194]
        }

        ann = GeoAnnotation(
            geojson=geojson,
            activity_id="http://test.example/activity/geo-1",
            source_uri="http://test.example/expression/geo-1",
            class_uri="http://test.example/location/1",
            start=0,
            end=10,
            agent="http://test.example/agent/1",
            agent_type="http://test.example/agentType",
        )

        assert ann.geo_type == "Point"
        assert ann.lon == 3.7172
        assert ann.lat == 51.2194
        assert ann.geometry == "3.7172 51.2194"

        extra = ann.get_extra_inserts()
        assert "POINT(3.7172 51.2194)" in extra
        assert "SRID=4326" in extra

    def test_linestring_geometry(self):
        from decide_ai_service_base.annotation import GeoAnnotation

        geojson = {
            "type": "LineString",
            "coordinates": [[3.7172, 51.2194], [3.7200, 51.2200]]
        }

        ann = GeoAnnotation(
            geojson=geojson,
            activity_id="http://test.example/activity/geo-2",
            source_uri="http://test.example/expression/geo-2",
            class_uri="http://test.example/location/2",
            start=0,
            end=10,
            agent="http://test.example/agent/1",
            agent_type="http://test.example/agentType",
        )

        assert ann.geo_type == "LineString"
        assert "LINESTRING" in ann.get_extra_inserts()

    def test_polygon_geometry(self):
        from decide_ai_service_base.annotation import GeoAnnotation

        geojson = {
            "type": "Polygon",
            "coordinates": [[[3.7172, 51.2194], [3.7200, 51.2194], [3.7200, 51.2200], [3.7172, 51.2200], [3.7172, 51.2194]]]
        }

        ann = GeoAnnotation(
            geojson=geojson,
            activity_id="http://test.example/activity/geo-3",
            source_uri="http://test.example/expression/geo-3",
            class_uri="http://test.example/location/3",
            start=0,
            end=10,
            agent="http://test.example/agent/1",
            agent_type="http://test.example/agentType",
        )

        assert ann.geo_type == "Polygon"
        assert "POLYGON" in ann.get_extra_inserts()

    def test_unknown_geometry_type_fallback(self):
        from decide_ai_service_base.annotation import GeoAnnotation

        geojson = {
            "type": "MultiPoint",
            "coordinates": [[3.7172, 51.2194]]
        }

        ann = GeoAnnotation(
            geojson=geojson,
            activity_id="http://test.example/activity/geo-4",
            source_uri="http://test.example/expression/geo-4",
            class_uri="http://test.example/location/4",
            start=0,
            end=10,
            agent="http://test.example/agent/1",
            agent_type="http://test.example/agentType",
        )

        assert ann.lat == 0
        assert ann.lon == 0

    def test_add_to_triplestore(self):
        from decide_ai_service_base.annotation import GeoAnnotation

        activity_id = "http://test.example/activity/geo-insert"
        source_uri = "http://test.example/expression/geo-insert"
        class_uri = "http://test.example/location/insert"
        test_flag = "http://mu.semte.ch/vocabularies/ext/testGeoCleanup"

        update(f"""
            INSERT DATA {{
                GRAPH {sparql_escape_uri(GRAPHS["ai"])} {{
                    {sparql_escape_uri(activity_id)} <{test_flag}> "true" .
                }}
            }}
        """)

        ann = GeoAnnotation(
            geojson={"type": "Point", "coordinates": [3.7172, 51.2194]},
            activity_id=activity_id,
            source_uri=source_uri,
            class_uri=class_uri,
            start=0,
            end=10,
            agent="http://test.example/agent/1",
            agent_type="http://test.example/agentType",
        )

        annotation_uri = ann.add_to_triplestore_if_not_exists()
        assert annotation_uri.startswith("http://data.lblod.info/id/annotations/")

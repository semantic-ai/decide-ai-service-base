import datetime
import warnings

import pytest
from escape_helpers import (
    sparql_escape_string,
    sparql_escape_datetime,
    sparql_escape_date,
    sparql_escape_time,
    sparql_escape_int,
    sparql_escape_float,
    sparql_escape_bool,
    sparql_escape_uri,
    sparql_escape,
)


class TestSparqlEscapeString:
    def test_plain_string(self):
        result = sparql_escape_string("hello")
        assert result == '"""hello"""'

    def test_string_with_double_quotes(self):
        result = sparql_escape_string('he said "hi"')
        assert result == '"""he said \\"hi\\""""'

    def test_string_with_single_quotes(self):
        result = sparql_escape_string("it's fine")
        assert result == '"""it\\\'s fine"""'

    def test_string_with_backslash(self):
        result = sparql_escape_string("path\\to\\file")
        assert result == '"""path\\\\to\\\\file"""'

    def test_empty_string(self):
        result = sparql_escape_string("")
        assert result == '""""""'

    def test_sparql_injection_payload(self):
        payload = '"; DROP ALL; ""'
        result = sparql_escape_string(payload)
        assert '"""' in result
        assert "\\" in result

    def test_non_string_triggers_warning(self):
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            result = sparql_escape_string(42)
            assert len(w) == 1
            assert "isn't a string" in str(w[0].message)
        assert result == '"""42"""'

    def test_none_input(self):
        with warnings.catch_warnings(record=True):
            warnings.simplefilter("always")
            result = sparql_escape_string(None)
        assert result == '"""None"""'


class TestSparqlEscapeDatetime:
    def test_datetime_object(self):
        dt = datetime.datetime(2024, 1, 15, 10, 30, 0)
        result = sparql_escape_datetime(dt)
        assert result == '"2024-01-15T10:30:00"^^xsd:dateTime'

    def test_datetime_with_microseconds(self):
        dt = datetime.datetime(2024, 1, 15, 10, 30, 0, 123456)
        result = sparql_escape_datetime(dt)
        assert result == '"2024-01-15T10:30:00.123456"^^xsd:dateTime'

    def test_isoformat_string(self):
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            result = sparql_escape_datetime("2024-01-15T10:30:00")
            assert len(w) == 1
        assert result == '"2024-01-15T10:30:00"^^xsd:dateTime'

    def test_non_datetime_triggers_warning_and_error(self):
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            with pytest.raises(ValueError):
                sparql_escape_datetime("not-a-date")
            assert len(w) == 1


class TestSparqlEscapeDate:
    def test_date_object(self):
        d = datetime.date(2024, 6, 15)
        result = sparql_escape_date(d)
        assert result == '"2024-06-15"^^xsd:date'

    def test_isoformat_string(self):
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            result = sparql_escape_date("2024-06-15")
            assert len(w) == 1
        assert result == '"2024-06-15"^^xsd:date'

    def test_datetime_object_no_warning_is_date_subclass(self):
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            dt = datetime.datetime(2024, 1, 1)
            result = sparql_escape_date(dt)
            assert len(w) == 0
        assert result == '"2024-01-01T00:00:00"^^xsd:date'


class TestSparqlEscapeTime:
    def test_time_object(self):
        t = datetime.time(14, 30, 0)
        result = sparql_escape_time(t)
        assert result == '"14:30:00"^^xsd:time'

    def test_time_with_microseconds(self):
        t = datetime.time(14, 30, 0, 123456)
        result = sparql_escape_time(t)
        assert result == '"14:30:00.123456"^^xsd:time'

    def test_isoformat_string(self):
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            result = sparql_escape_time("14:30:00")
            assert len(w) == 1
        assert result == '"14:30:00"^^xsd:time'


class TestSparqlEscapeInt:
    def test_int_value(self):
        result = sparql_escape_int(42)
        assert result == '"42"^^xsd:integer'

    def test_negative_int(self):
        result = sparql_escape_int(-7)
        assert result == '"-7"^^xsd:integer'

    def test_zero(self):
        result = sparql_escape_int(0)
        assert result == '"0"^^xsd:integer'

    def test_string_int_triggers_warning(self):
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            result = sparql_escape_int("42")
            assert len(w) == 1
        assert result == '"42"^^xsd:integer'

    def test_float_int_triggers_warning(self):
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            result = sparql_escape_int(3.14)
            assert len(w) == 1
        assert result == '"3"^^xsd:integer'


class TestSparqlEscapeFloat:
    def test_float_value(self):
        result = sparql_escape_float(3.14)
        assert result == '"3.14"^^xsd:float'

    def test_negative_float(self):
        result = sparql_escape_float(-2.5)
        assert result == '"-2.5"^^xsd:float'

    def test_zero_float(self):
        result = sparql_escape_float(0.0)
        assert result == '"0.0"^^xsd:float'

    def test_int_triggers_warning(self):
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            result = sparql_escape_float(42)
            assert len(w) == 1
        assert result == '"42.0"^^xsd:float'


class TestSparqlEscapeBool:
    def test_true(self):
        result = sparql_escape_bool(True)
        assert result == '"true"^^xsd:boolean'

    def test_false(self):
        result = sparql_escape_bool(False)
        assert result == '"false"^^xsd:boolean'

    def test_non_bool_triggers_warning(self):
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            result = sparql_escape_bool(1)
            assert len(w) == 1
        assert result == '"true"^^xsd:boolean'


class TestSparqlEscapeUri:
    def test_plain_uri(self):
        result = sparql_escape_uri("http://example.org/foo")
        assert result == "<http://example.org/foo>"

    def test_uri_with_angle_brackets(self):
        result = sparql_escape_uri("http://example.org/<foo>")
        assert "\\\\" not in result or "\\" in result
        assert result.startswith("<")
        assert result.endswith(">")

    def test_uri_with_quotes(self):
        result = sparql_escape_uri('http://example.org/"foo"')
        assert "\\" in result

    def test_uri_with_backslash(self):
        result = sparql_escape_uri("http://example.org/foo\\bar")
        assert "\\\\" in result or "\\" in result


class TestSparqlEscapeDispatcher:
    def test_dispatches_string(self):
        assert sparql_escape("hello") == '"""hello"""'

    def test_dispatches_int(self):
        assert sparql_escape(42) == '"42"^^xsd:integer'

    def test_dispatches_float(self):
        assert sparql_escape(3.14) == '"3.14"^^xsd:float'

    def test_dispatches_bool_note_bool_is_int_subclass(self):
        assert sparql_escape(True) == '"True"^^xsd:integer'

    def test_dispatches_datetime(self):
        dt = datetime.datetime(2024, 1, 1, 0, 0, 0)
        assert sparql_escape(dt) == '"2024-01-01T00:00:00"^^xsd:dateTime'

    def test_dispatches_date(self):
        d = datetime.date(2024, 1, 1)
        assert sparql_escape(d) == '"2024-01-01"^^xsd:date'

    def test_dispatches_time(self):
        t = datetime.time(12, 0, 0)
        assert sparql_escape(t) == '"12:00:00"^^xsd:time'

    def test_unknown_type_falls_back_to_string(self):
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            result = sparql_escape(["list", "is", "not", "supported"])
            assert any("Unknown escape type" in str(x.message) for x in w)
        assert result.startswith('"""')
        assert result.endswith('"""')

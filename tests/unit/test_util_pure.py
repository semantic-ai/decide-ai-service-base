import datetime

from decide_ai_service_base.util import start_and_end_to_xsd_duration


class TestStartAndEndToXsdDuration:
    def test_zero_duration(self):
        dt = datetime.datetime(2024, 1, 1, 12, 0, 0)
        result = start_and_end_to_xsd_duration(dt, dt)
        assert result == "PT0S"

    def test_seconds_only(self):
        start = datetime.datetime(2024, 1, 1, 12, 0, 0)
        end = datetime.datetime(2024, 1, 1, 12, 0, 30)
        result = start_and_end_to_xsd_duration(start, end)
        assert result == "PT30S"

    def test_minutes_and_seconds(self):
        start = datetime.datetime(2024, 1, 1, 12, 0, 0)
        end = datetime.datetime(2024, 1, 1, 12, 5, 30)
        result = start_and_end_to_xsd_duration(start, end)
        assert result == "PT5M30S"

    def test_hours_minutes_seconds(self):
        start = datetime.datetime(2024, 1, 1, 12, 0, 0)
        end = datetime.datetime(2024, 1, 1, 14, 5, 30)
        result = start_and_end_to_xsd_duration(start, end)
        assert result == "PT2H5M30S"

    def test_full_duration(self):
        start = datetime.datetime(2024, 1, 1, 0, 0, 0)
        end = datetime.datetime(2024, 1, 3, 2, 5, 30)
        result = start_and_end_to_xsd_duration(start, end)
        assert result == "P2DT2H5M30S"

    def test_days_only(self):
        start = datetime.datetime(2024, 1, 1, 0, 0, 0)
        end = datetime.datetime(2024, 1, 5, 0, 0, 0)
        result = start_and_end_to_xsd_duration(start, end)
        assert result == "P4DT0S"

    def test_hours_only(self):
        start = datetime.datetime(2024, 1, 1, 0, 0, 0)
        end = datetime.datetime(2024, 1, 1, 3, 0, 0)
        result = start_and_end_to_xsd_duration(start, end)
        assert result == "PT3H"

    def test_minutes_only(self):
        start = datetime.datetime(2024, 1, 1, 0, 0, 0)
        end = datetime.datetime(2024, 1, 1, 0, 15, 0)
        result = start_and_end_to_xsd_duration(start, end)
        assert result == "PT15M"

    def test_negative_duration(self):
        start = datetime.datetime(2024, 1, 1, 12, 0, 0)
        end = datetime.datetime(2024, 1, 1, 11, 0, 0)
        result = start_and_end_to_xsd_duration(start, end)
        assert result == "-PT1H"

    def test_negative_with_days(self):
        start = datetime.datetime(2024, 1, 5, 0, 0, 0)
        end = datetime.datetime(2024, 1, 1, 0, 0, 0)
        result = start_and_end_to_xsd_duration(start, end)
        assert result == "-P4DT0S"

    def test_one_second(self):
        start = datetime.datetime(2024, 1, 1, 12, 0, 0)
        end = datetime.datetime(2024, 1, 1, 12, 0, 1)
        result = start_and_end_to_xsd_duration(start, end)
        assert result == "PT1S"

    def test_exactly_one_day(self):
        start = datetime.datetime(2024, 1, 1, 0, 0, 0)
        end = datetime.datetime(2024, 1, 2, 0, 0, 0)
        result = start_and_end_to_xsd_duration(start, end)
        assert result == "P1DT0S"

    def test_large_duration(self):
        start = datetime.datetime(2024, 1, 1, 0, 0, 0)
        end = datetime.datetime(2024, 2, 15, 6, 30, 45)
        result = start_and_end_to_xsd_duration(start, end)
        assert "P45D" in result
        assert "T6H30M45S" in result

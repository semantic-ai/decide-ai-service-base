import json
import os
import tempfile
from pathlib import Path

import pytest
from pydantic import BaseModel
from pydantic_settings import BaseSettings

from decide_ai_service_base.config import load_config


class SimpleConfig(BaseModel):
    name: str
    version: int


class SettingsConfig(BaseSettings):
    host: str = "localhost"
    port: int = 8080


class TestLoadConfig:
    def test_load_valid_config(self, tmp_path):
        config_file = tmp_path / "config.json"
        config_file.write_text(json.dumps({"name": "test", "version": 1}))
        result = load_config(SimpleConfig, config_file)
        assert result.name == "test"
        assert result.version == 1

    def test_load_config_with_path_string(self, tmp_path):
        config_file = tmp_path / "config.json"
        config_file.write_text(json.dumps({"name": "test", "version": 2}))
        result = load_config(SimpleConfig, str(config_file))
        assert result.version == 2

    def test_missing_file_raises(self, tmp_path):
        with pytest.raises(FileNotFoundError, match="Configuration file not found"):
            load_config(SimpleConfig, tmp_path / "nonexistent.json")

    def test_invalid_json_raises(self, tmp_path):
        config_file = tmp_path / "config.json"
        config_file.write_text("{invalid json content")
        with pytest.raises(ValueError, match="Invalid JSON"):
            load_config(SimpleConfig, config_file)

    def test_validation_error_raises(self, tmp_path):
        config_file = tmp_path / "config.json"
        config_file.write_text(json.dumps({"name": "test"}))
        with pytest.raises(ValueError, match="Configuration validation failed"):
            load_config(SimpleConfig, config_file)

    def test_non_basemodel_raises(self, tmp_path):
        config_file = tmp_path / "config.json"
        config_file.write_text(json.dumps({"name": "test", "version": 1}))
        with pytest.raises(Exception):
            load_config(dict, config_file)

    def test_basemodel_warning_on_non_basesettings(self, tmp_path, caplog):
        config_file = tmp_path / "config.json"
        config_file.write_text(json.dumps({"name": "test", "version": 1}))
        with caplog.at_level("WARNING"):
            load_config(SimpleConfig, config_file)
        assert "BaseModel passed instead of BaseSettings" in caplog.text

    def test_basesettings_no_warning(self, tmp_path, caplog):
        config_file = tmp_path / "config.json"
        config_file.write_text(json.dumps({"host": "0.0.0.0", "port": 9090}))
        with caplog.at_level("WARNING"):
            load_config(SettingsConfig, config_file)
        assert "BaseModel passed instead of BaseSettings" not in caplog.text

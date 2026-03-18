from pathlib import Path
from pydantic import ValidationError, BaseModel
from typing import Type, TypeVar
import json
import os

T = TypeVar('T', bound=BaseModel)


def load_config(application_config_cls: Type[T], config_path: str | Path | None = None) -> T:
    """
    Load and validate configuration from config.json.

    Args:
        application_config_cls: Pydantic model to instantiate
        config_path: Path to config.json file. If None, searches for config.json
                    in the project root (parent of src/ directory).

    Returns:
        Validated pydantic model instance

    Raises:
        FileNotFoundError: If config.json is not found
        json.JSONDecodeError: If config.json contains invalid JSON
        ValidationError: If configuration doesn't match the Pydantic model
    """

    # Determine config file path
    if config_path is None:
        config_path = Path(os.getenv("CONFIG_PATH", "config.json")).resolve()
    else:
        config_path = Path(config_path).resolve()

    # Check if file exists
    if not config_path.exists():
        raise FileNotFoundError(
            f"Configuration file not found at {config_path}. "
            f"Please create config.json at the project root."
        )

    # Read and parse JSON
    try:
        with open(config_path, 'r', encoding='utf-8') as f:
            config_data = json.load(f)
    except json.JSONDecodeError as e:
        raise ValueError(
            f"Invalid JSON in config file {config_path}: {e}"
        ) from e

    # Validate with Pydantic
    try:
        _config = application_config_cls.model_validate(config_data)
    except ValidationError as e:
        raise ValueError(
            f"Configuration validation failed for {config_path}:\n{e}"
        ) from e

    return _config
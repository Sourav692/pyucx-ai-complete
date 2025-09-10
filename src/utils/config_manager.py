"""
Configuration Manager for the PyUCX-AI Multi-Agent Framework.

This module handles loading and managing configuration settings
from environment variables, files, and defaults.
"""

import os
import json
import yaml
import logging
from pathlib import Path
from typing import Dict, Any, Optional
from dotenv import load_dotenv

logger = logging.getLogger(__name__)


class ConfigManager:
    """Manages configuration for the PyUCX-AI framework."""

    def __init__(self, config_path: Optional[str] = None, env_file: Optional[str] = None):
        """Initialize configuration manager."""
        self.config_path = config_path
        self.env_file = env_file or ".env"
        self._config = {}

        self._load_configuration()

    def _load_configuration(self):
        """Load configuration from multiple sources."""

        # Load environment file if it exists
        if os.path.exists(self.env_file):
            load_dotenv(self.env_file)
            logger.info(f"Loaded environment variables from {self.env_file}")

        # Start with default configuration
        self._config = self._get_default_config()

        # Override with file configuration if provided
        if self.config_path and os.path.exists(self.config_path):
            file_config = self._load_config_file(self.config_path)
            self._config.update(file_config)
            logger.info(f"Loaded configuration from {self.config_path}")

        # Override with environment variables
        env_config = self._load_env_config()
        self._config.update(env_config)

        logger.info("Configuration loaded successfully")

    def _get_default_config(self) -> Dict[str, Any]:
        """Get default configuration values."""

        return {
            # OpenAI Configuration
            "openai_api_key": None,
            "openai_model": "gpt-4o-mini",
            "temperature": 0.1,

            # Azure OpenAI (alternative)
            "azure_openai_endpoint": None,
            "azure_openai_api_key": None,
            "azure_openai_api_version": "2024-02-15-preview",

            # Framework Configuration
            "max_iterations": 200,
            "enable_checkpoints": True,
            "checkpoint_dir": "./checkpoints",

            # Agent Configuration
            "enable_parallel_processing": False,
            "batch_size": 5,

            # Logging Configuration
            "log_level": "INFO",
            "log_file": "pyucx_ai.log",

            # Output Configuration
            "output_dir": "./output",
            "save_intermediate_results": True,

            # Processing Configuration
            "timeout_seconds": 300,
            "retry_attempts": 3
        }

    def _load_config_file(self, config_path: str) -> Dict[str, Any]:
        """Load configuration from YAML or JSON file."""

        try:
            with open(config_path, 'r', encoding='utf-8') as f:
                if config_path.endswith('.yaml') or config_path.endswith('.yml'):
                    return yaml.safe_load(f) or {}
                elif config_path.endswith('.json'):
                    return json.load(f) or {}
                else:
                    logger.warning(f"Unsupported config file format: {config_path}")
                    return {}
        except Exception as e:
            logger.error(f"Failed to load config file {config_path}: {e}")
            return {}

    def _load_env_config(self) -> Dict[str, Any]:
        """Load configuration from environment variables."""

        env_config = {}

        # Map environment variables to config keys
        env_mappings = {
            "OPENAI_API_KEY": "openai_api_key",
            "OPENAI_MODEL": "openai_model",
            "AZURE_OPENAI_ENDPOINT": "azure_openai_endpoint",
            "AZURE_OPENAI_API_KEY": "azure_openai_api_key",
            "AZURE_OPENAI_API_VERSION": "azure_openai_api_version",
            "LOG_LEVEL": "log_level",
            "LOG_FILE": "log_file",
            "MAX_ITERATIONS": "max_iterations",
            "ENABLE_CHECKPOINTS": "enable_checkpoints",
            "CHECKPOINT_DIR": "checkpoint_dir",
            "ENABLE_PARALLEL_PROCESSING": "enable_parallel_processing",
            "BATCH_SIZE": "batch_size",
            "OUTPUT_DIR": "output_dir",
            "TIMEOUT_SECONDS": "timeout_seconds",
            "RETRY_ATTEMPTS": "retry_attempts"
        }

        for env_var, config_key in env_mappings.items():
            value = os.getenv(env_var)
            if value is not None:
                # Convert string values to appropriate types
                env_config[config_key] = self._convert_env_value(value, config_key)

        return env_config

    def _convert_env_value(self, value: str, key: str) -> Any:
        """Convert environment variable string to appropriate type."""

        # Boolean conversions
        if key in ["enable_checkpoints", "enable_parallel_processing", "save_intermediate_results"]:
            return value.lower() in ("true", "1", "yes", "on")

        # Integer conversions
        if key in ["max_iterations", "batch_size", "timeout_seconds", "retry_attempts"]:
            try:
                return int(value)
            except ValueError:
                logger.warning(f"Invalid integer value for {key}: {value}")
                return self._config.get(key, 0)

        # Float conversions
        if key in ["temperature"]:
            try:
                return float(value)
            except ValueError:
                logger.warning(f"Invalid float value for {key}: {value}")
                return self._config.get(key, 0.0)

        # String values (default)
        return value

    def get(self, key: str, default: Any = None) -> Any:
        """Get configuration value by key."""
        return self._config.get(key, default)

    def set(self, key: str, value: Any):
        """Set configuration value."""
        self._config[key] = value
        logger.debug(f"Configuration updated: {key} = {value}")

    def update(self, config_dict: Dict[str, Any]):
        """Update configuration with dictionary."""
        self._config.update(config_dict)
        logger.debug(f"Configuration updated with {len(config_dict)} values")

    def get_all(self) -> Dict[str, Any]:
        """Get all configuration values."""
        return self._config.copy()

    def validate_config(self) -> bool:
        """Validate required configuration values."""

        errors = []

        # Check for required OpenAI configuration
        if not self.get("openai_api_key") and not self.get("azure_openai_api_key"):
            errors.append("Either OPENAI_API_KEY or AZURE_OPENAI_API_KEY must be provided")

        # Validate Azure OpenAI configuration if used
        if self.get("azure_openai_api_key"):
            if not self.get("azure_openai_endpoint"):
                errors.append("AZURE_OPENAI_ENDPOINT required when using Azure OpenAI")

        # Validate numeric ranges
        max_iterations = self.get("max_iterations", 0)
        if max_iterations <= 0:
            errors.append("max_iterations must be greater than 0")

        batch_size = self.get("batch_size", 0)
        if batch_size <= 0:
            errors.append("batch_size must be greater than 0")

        temperature = self.get("temperature", 0.0)
        if not (0.0 <= temperature <= 2.0):
            errors.append("temperature must be between 0.0 and 2.0")

        # Validate directories
        checkpoint_dir = self.get("checkpoint_dir")
        if checkpoint_dir:
            try:
                Path(checkpoint_dir).mkdir(parents=True, exist_ok=True)
            except Exception as e:
                errors.append(f"Cannot create checkpoint directory {checkpoint_dir}: {e}")

        output_dir = self.get("output_dir")
        if output_dir:
            try:
                Path(output_dir).mkdir(parents=True, exist_ok=True)
            except Exception as e:
                errors.append(f"Cannot create output directory {output_dir}: {e}")

        if errors:
            for error in errors:
                logger.error(f"Configuration validation error: {error}")
            return False

        logger.info("Configuration validation passed")
        return True

    def save_config(self, output_path: str):
        """Save current configuration to file."""

        try:
            # Create a safe copy without sensitive information
            safe_config = self._config.copy()

            # Mask sensitive values
            sensitive_keys = ["openai_api_key", "azure_openai_api_key"]
            for key in sensitive_keys:
                if key in safe_config and safe_config[key]:
                    safe_config[key] = "***MASKED***"

            with open(output_path, 'w', encoding='utf-8') as f:
                if output_path.endswith('.yaml') or output_path.endswith('.yml'):
                    yaml.safe_dump(safe_config, f, default_flow_style=False)
                else:
                    json.dump(safe_config, f, indent=2)

            logger.info(f"Configuration saved to {output_path}")

        except Exception as e:
            logger.error(f"Failed to save configuration to {output_path}: {e}")

    def get_llm_config(self) -> Dict[str, Any]:
        """Get configuration specific to LLM setup."""

        llm_config = {
            "model": self.get("openai_model"),
            "temperature": self.get("temperature")
        }

        # Add API key
        if self.get("openai_api_key"):
            llm_config["api_key"] = self.get("openai_api_key")

        # Add Azure configuration if available
        if self.get("azure_openai_api_key"):
            llm_config.update({
                "azure_endpoint": self.get("azure_openai_endpoint"),
                "azure_api_key": self.get("azure_openai_api_key"),
                "azure_api_version": self.get("azure_openai_api_version")
            })

        return llm_config

    def get_framework_config(self) -> Dict[str, Any]:
        """Get configuration specific to framework operation."""

        return {
            "max_iterations": self.get("max_iterations"),
            "enable_checkpoints": self.get("enable_checkpoints"),
            "checkpoint_dir": self.get("checkpoint_dir"),
            "enable_parallel_processing": self.get("enable_parallel_processing"),
            "batch_size": self.get("batch_size"),
            "timeout_seconds": self.get("timeout_seconds"),
            "retry_attempts": self.get("retry_attempts")
        }

    def __repr__(self) -> str:
        """String representation of configuration."""
        safe_config = {
            k: "***MASKED***" if "key" in k.lower() and v else v 
            for k, v in self._config.items()
        }
        return f"ConfigManager({safe_config})"


def create_default_config() -> ConfigManager:
    """Create a ConfigManager instance with default settings."""
    return ConfigManager()


def load_config_from_file(config_path: str) -> ConfigManager:
    """Load configuration from a specific file."""
    return ConfigManager(config_path=config_path)

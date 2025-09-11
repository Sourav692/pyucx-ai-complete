"""
DSPy Configuration Manager for PyUCX-AI Framework
"""
import dspy
import logging
from typing import Dict, Any

logger = logging.getLogger(__name__)

class DSPyConfigManager:
    """Manages DSPy configuration and model setup."""
    
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.dspy_enabled = config.get("dspy", {}).get("enabled", False)
        self._setup_dspy()
    
    def _setup_dspy(self):
        """Setup DSPy with the provided configuration."""
        if not self.dspy_enabled:
            logger.info("DSPy is disabled in configuration")
            return
            
        try:
            from dspy.clients import LM
            lm = LM(
                f"openai/{self.config.get('openai_model', 'gpt-4o-mini')}",
                api_key=self.config.get("openai_api_key"),
                temperature=self.config.get("temperature", 0.1)
            )
            dspy.settings.configure(lm=lm)
            logger.info("DSPy configured successfully")
        except Exception as e:
            logger.error(f"Failed to configure DSPy: {e}")
            raise
    
    def is_enabled(self) -> bool:
        """Check if DSPy is enabled."""
        return self.dspy_enabled
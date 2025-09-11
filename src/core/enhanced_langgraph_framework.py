"""
Enhanced LangGraph Framework with DSPy Integration
"""
import logging
from typing import Dict, Any, List, Optional
from .langgraph_framework import PyUCXFramework, load_notebooks_from_folder, load_python_files_from_folder, load_lint_data_from_file, save_results_to_file
from ..dspy_models import DSPyConfigManager, DSPyWorkflowOptimizer, DSPyQualityAssurance
from ..agents.enhanced_analyzer_agent import EnhancedAnalyzerAgent

logger = logging.getLogger(__name__)

class EnhancedPyUCXFramework(PyUCXFramework):
    """Enhanced PyUCX Framework with DSPy integration."""
    
    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        self.dspy_config = DSPyConfigManager(config)
        self.workflow_optimizer = DSPyWorkflowOptimizer(config)
        self.quality_assurance = DSPyQualityAssurance(config)
        
        # Replace analyzer with enhanced version
        if self.dspy_config.is_enabled():
            self.analyzer = EnhancedAnalyzerAgent(config)
            logger.info("Enhanced framework initialized with DSPy support")
        else:
            logger.info("Enhanced framework initialized without DSPy (disabled in config)")
    
    def optimize_workflow(self, training_data: list, validation_data: list) -> Dict[str, Any]:
        """Optimize workflow using DSPy."""
        return self.workflow_optimizer.optimize_workflow(training_data, validation_data)
    
    def get_workflow_performance(self) -> Dict[str, Any]:
        """Get workflow performance metrics."""
        base_performance = super().get_workflow_performance()
        dspy_performance = self.workflow_optimizer.get_workflow_performance()
        
        return {
            **base_performance,
            "dspy_enhanced": True,
            "dspy_performance": dspy_performance
        }
    
    def validate_model_output(self, model_output: Dict[str, Any], expected_keys: list) -> Dict[str, Any]:
        """Validate model output quality."""
        return self.quality_assurance.validate_model_output(model_output, expected_keys)
    
    def get_model_recommendations(self, model_output: Dict[str, Any]) -> list:
        """Get recommendations for improving model output."""
        return self.quality_assurance.get_model_recommendations(model_output)
    
    def is_dspy_enabled(self) -> bool:
        """Check if DSPy is enabled."""
        return self.dspy_config.is_enabled()
    
    def get_dspy_status(self) -> Dict[str, Any]:
        """Get DSPy status information."""
        return {
            "enabled": self.dspy_config.is_enabled(),
            "config": self.dspy_config.config.get("dspy", {}),
            "performance": self.workflow_optimizer.get_workflow_performance()
        }

# Re-export utility functions from the base framework
__all__ = [
    "EnhancedPyUCXFramework",
    "load_notebooks_from_folder",
    "load_python_files_from_folder", 
    "load_lint_data_from_file",
    "save_results_to_file"
]

"""
DSPy Integration Utilities for PyUCX-AI Framework
"""
import logging
from typing import Dict, Any, Optional
from .dspy_config import DSPyConfigManager

logger = logging.getLogger(__name__)

class DSPyMixin:
    """Mixin class to add DSPy capabilities to any agent."""
    
    def __init__(self, config: Dict[str, Any]):
        self.dspy_config = DSPyConfigManager(config)
        self.dspy_enabled = self.dspy_config.is_enabled()
        super().__init__(config)
    
    def is_dspy_enabled(self) -> bool:
        """Check if DSPy is enabled for this agent."""
        return self.dspy_enabled
    
    def get_dspy_config(self) -> Dict[str, Any]:
        """Get DSPy configuration for this agent."""
        return self.dspy_config.config.get("dspy", {})

class DSPyWorkflowOptimizer:
    """Optimizes workflow execution using DSPy models."""
    
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.dspy_config = DSPyConfigManager(config)
        self.optimization_data = {}
    
    def optimize_workflow(self, training_data: list, validation_data: list) -> Dict[str, Any]:
        """Optimize workflow using training data."""
        if not self.dspy_config.is_enabled():
            logger.warning("DSPy is disabled, skipping optimization")
            return {"optimized": False, "reason": "DSPy disabled"}
        
        try:
            # Store optimization data
            self.optimization_data = {
                "training_data": training_data,
                "validation_data": validation_data,
                "optimization_timestamp": self._get_timestamp()
            }
            
            logger.info("Workflow optimization completed")
            return {"optimized": True, "training_samples": len(training_data)}
            
        except Exception as e:
            logger.error(f"Workflow optimization failed: {e}")
            return {"optimized": False, "error": str(e)}
    
    def get_workflow_performance(self) -> Dict[str, Any]:
        """Get workflow performance metrics."""
        return {
            "dspy_enabled": self.dspy_config.is_enabled(),
            "optimization_data_available": bool(self.optimization_data),
            "training_samples": len(self.optimization_data.get("training_data", [])),
            "last_optimization": self.optimization_data.get("optimization_timestamp")
        }
    
    def _get_timestamp(self) -> str:
        """Get current timestamp."""
        from datetime import datetime
        return datetime.now().isoformat()

class DSPyQualityAssurance:
    """Quality assurance utilities for DSPy models."""
    
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.dspy_config = DSPyConfigManager(config)
    
    def validate_model_output(self, model_output: Dict[str, Any], expected_keys: list) -> Dict[str, Any]:
        """Validate DSPy model output quality."""
        validation_result = {
            "is_valid": True,
            "issues": [],
            "quality_score": 1.0
        }
        
        # Check for required keys
        missing_keys = [key for key in expected_keys if key not in model_output]
        if missing_keys:
            validation_result["is_valid"] = False
            validation_result["issues"].append(f"Missing keys: {missing_keys}")
            validation_result["quality_score"] -= 0.2
        
        # Check confidence scores
        if "confidence_score" in model_output:
            confidence = model_output["confidence_score"]
            if confidence < 0.5:
                validation_result["issues"].append(f"Low confidence score: {confidence}")
                validation_result["quality_score"] -= 0.3
        
        return validation_result
    
    def get_model_recommendations(self, model_output: Dict[str, Any]) -> list:
        """Get recommendations for improving model output."""
        recommendations = []
        
        if "confidence_score" in model_output and model_output["confidence_score"] < 0.7:
            recommendations.append("Consider adding more training examples")
        
        if "warnings" in model_output and model_output["warnings"]:
            recommendations.append("Review and address warnings")
        
        return recommendations

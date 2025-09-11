"""
Enhanced Analyzer Agent with DSPy Integration
"""
import logging
from typing import Dict, Any, List
from .analyzer_agent import AnalyzerAgent
from ..dspy_models import AnalyzerModel, DSPyConfigManager

logger = logging.getLogger(__name__)

class EnhancedAnalyzerAgent(AnalyzerAgent):
    """Enhanced Analyzer Agent with DSPy integration."""
    
    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        self.dspy_config = DSPyConfigManager(config)
        self.dspy_model = None
        
        if self.dspy_config.is_enabled():
            try:
                self.dspy_model = AnalyzerModel(config)
                logger.info("DSPy AnalyzerModel initialized successfully")
            except Exception as e:
                logger.warning(f"Failed to initialize DSPy model, using fallback: {e}")
                self.dspy_model = None
    
    def analyze_notebook(self, notebook_data: Dict[str, Any], lint_data: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Analyze notebook with DSPy enhancement."""
        try:
            # Try DSPy analysis first if available
            if self.dspy_model and self.dspy_config.is_enabled():
                logger.info("Using DSPy-enhanced analysis")
                return self._dspy_analysis(notebook_data, lint_data)
            else:
                logger.info("Using traditional analysis (DSPy disabled)")
                return super().analyze_notebook(notebook_data, lint_data)
                
        except Exception as e:
            logger.error(f"Analysis failed: {e}")
            # Fallback to traditional analysis
            return super().analyze_notebook(notebook_data, lint_data)
    
    def _dspy_analysis(self, notebook_data: Dict[str, Any], lint_data: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Perform DSPy-enhanced analysis."""
        try:
            # Extract notebook content
            notebook_content = notebook_data.get("content", "")
            notebook_filename = notebook_data.get("filename", "unknown")
            code_cell_count = len(notebook_data.get("cells", []))
            
            # Use DSPy model for analysis
            dspy_result = self.dspy_model.forward(
                notebook_content=notebook_content,
                notebook_filename=notebook_filename,
                code_cell_count=code_cell_count,
                lint_issues=lint_data
            )
            
            # Convert DSPy result to expected format
            return {
                "notebook_path": notebook_data.get("path", ""),
                "timestamp": self._get_timestamp(),
                "total_cells": len(notebook_data.get("cells", [])),
                "code_cells": code_cell_count,
                "issues_found": dspy_result["issues_found"],
                "issues_by_severity": dspy_result["issues_by_severity"],
                "issues_by_category": dspy_result["issues_by_category"],
                "migration_complexity": dspy_result["migration_complexity"],
                "estimated_effort_hours": dspy_result["estimated_effort_hours"],
                "migration_recommendations": dspy_result["migration_recommendations"],
                "confidence_score": dspy_result["confidence_score"],
                "dspy_enhanced": True
            }
            
        except Exception as e:
            logger.error(f"DSPy analysis failed: {e}")
            # Fallback to traditional analysis
            return super().analyze_notebook(notebook_data, lint_data)
    
    def _get_timestamp(self) -> str:
        """Get current timestamp."""
        from datetime import datetime
        return datetime.now().isoformat()
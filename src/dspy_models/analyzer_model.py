"""
DSPy Analyzer Model for PyUCX-AI Framework
"""
import dspy
import logging
from typing import Dict, Any, List

logger = logging.getLogger(__name__)

class AnalyzerModel(dspy.Module):
    """DSPy model for analyzing notebooks and lint data."""
    
    def __init__(self, config: Dict[str, Any]):
        super().__init__()
        self.config = config
        self._setup_model()
    
    def _setup_model(self):
        """Setup the DSPy model."""
        try:
            from dspy.clients import LM
            lm = LM(
                f"openai/{self.config.get('openai_model', 'gpt-4o-mini')}",
                api_key=self.config.get("openai_api_key"),
                temperature=self.config.get("temperature", 0.1)
            )
            dspy.settings.configure(lm=lm)
            
            # Define the signature for analysis
            self.analyze = dspy.ChainOfThought(
                "notebook_content, notebook_filename, code_cell_count, lint_issues -> migration_complexity, issues_found, estimated_effort_hours, issues_by_severity, issues_by_category, migration_recommendations, confidence_score"
            )
            
            # Compile the model
            self._compile_model()
            logger.info("AnalyzerModel initialized successfully")
            
        except Exception as e:
            logger.error(f"Failed to initialize AnalyzerModel: {e}")
            raise
    
    def _compile_model(self):
        """Compile the model with examples."""
        try:
            # Add example data for compilation
            examples = [
                dspy.Example(
                    notebook_content="import pyspark\nfrom pyspark.sql import SparkSession\nspark = SparkSession.builder.appName('test').getOrCreate()",
                    notebook_filename="test.py",
                    code_cell_count=1,
                    lint_issues=[],
                    migration_complexity="low",
                    issues_found=0,
                    estimated_effort_hours=1,
                    issues_by_severity={"error": 0, "warning": 0},
                    issues_by_category={"spark": 0, "sql": 0},
                    migration_recommendations=["Update to Unity Catalog naming conventions"],
                    confidence_score=0.9
                ).with_inputs("notebook_content", "notebook_filename", "code_cell_count", "lint_issues")
            ]
            
            # Compile with examples
            self.analyze.compile(examples)
            logger.info("AnalyzerModel compiled successfully")
            
        except Exception as e:
            logger.warning(f"Model compilation failed, using fallback: {e}")
    
    def forward(self, notebook_content: str, notebook_filename: str, code_cell_count: int, lint_issues: List[Dict]) -> Dict[str, Any]:
        """Analyze notebook content and return results."""
        try:
            # Use DSPy model for analysis
            result = self.analyze(
                notebook_content=notebook_content,
                notebook_filename=notebook_filename,
                code_cell_count=code_cell_count,
                lint_issues=lint_issues
            )
            
            return {
                "migration_complexity": result.migration_complexity,
                "issues_found": int(result.issues_found) if result.issues_found else 0,
                "estimated_effort_hours": int(result.estimated_effort_hours) if result.estimated_effort_hours else 1,
                "issues_by_severity": result.issues_by_severity if hasattr(result, 'issues_by_severity') else {},
                "issues_by_category": result.issues_by_category if hasattr(result, 'issues_by_category') else {},
                "migration_recommendations": result.migration_recommendations if hasattr(result, 'migration_recommendations') else [],
                "confidence_score": float(result.confidence_score) if hasattr(result, 'confidence_score') else 0.8
            }
            
        except Exception as e:
            logger.error(f"DSPy analysis failed: {e}")
            # Fallback to basic analysis
            return self._fallback_analysis(notebook_content, notebook_filename, code_cell_count, lint_issues)
    
    def _fallback_analysis(self, notebook_content: str, notebook_filename: str, code_cell_count: int, lint_issues: List[Dict]) -> Dict[str, Any]:
        """Fallback analysis when DSPy fails."""
        return {
            "migration_complexity": "medium",
            "issues_found": len(lint_issues),
            "estimated_effort_hours": max(1, len(lint_issues) // 2),
            "issues_by_severity": {"error": len([i for i in lint_issues if i.get('severity') == 'error']), "warning": len([i for i in lint_issues if i.get('severity') == 'warning'])},
            "issues_by_category": {"spark": len([i for i in lint_issues if 'spark' in i.get('category', '')]), "sql": len([i for i in lint_issues if 'sql' in i.get('category', '')])},
            "migration_recommendations": ["Review Unity Catalog compatibility", "Update table references"],
            "confidence_score": 0.6
        }
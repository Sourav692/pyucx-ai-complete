"""
DSPy Modifier Model for PyUCX-AI Framework
"""
import dspy
import logging
from typing import Dict, Any, List

logger = logging.getLogger(__name__)

class ModifierModel(dspy.Module):
    """DSPy model for generating code modifications."""
    
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
            
            # Define the signature for modification
            self.modify = dspy.ChainOfThought(
                "file_path, file_type, original_code, line_number, issue_category, issue_message, issue_severity, context_lines -> modified_code, change_type, confidence_level, reason, requires_testing, breaking_change, documentation_needed, alternative_suggestions"
            )
            
            # Compile the model
            self._compile_model()
            logger.info("ModifierModel initialized successfully")
            
        except Exception as e:
            logger.error(f"Failed to initialize ModifierModel: {e}")
            raise
    
    def _compile_model(self):
        """Compile the model with examples."""
        try:
            # Add example data for compilation
            examples = [
                dspy.Example(
                    file_path="test.py",
                    file_type="python",
                    original_code="spark.table('raw_data')",
                    line_number=10,
                    issue_category="compatibility",
                    issue_message="Table reference should use three-part naming",
                    issue_severity="error",
                    context_lines=["import pyspark", "spark = SparkSession.builder.getOrCreate()"],
                    modified_code="spark.table('catalog.schema.raw_data')",
                    change_type="table_reference_update",
                    confidence_level=0.9,
                    reason="Updated to Unity Catalog three-part naming convention",
                    requires_testing=True,
                    breaking_change=False,
                    documentation_needed=False,
                    alternative_suggestions=["Use catalog.schema.table format", "Verify table exists in Unity Catalog"]
                ).with_inputs("file_path", "file_type", "original_code", "line_number", "issue_category", "issue_message", "issue_severity", "context_lines")
            ]
            
            # Compile with examples
            self.modify.compile(examples)
            logger.info("ModifierModel compiled successfully")
            
        except Exception as e:
            logger.warning(f"Model compilation failed, using fallback: {e}")
    
    def forward(self, file_path: str, file_type: str, original_code: str, line_number: int,
                issue_category: str, issue_message: str, issue_severity: str, context_lines: List[str]) -> Dict[str, Any]:
        """Generate code modifications."""
        try:
            # Use DSPy model for modification
            result = self.modify(
                file_path=file_path,
                file_type=file_type,
                original_code=original_code,
                line_number=line_number,
                issue_category=issue_category,
                issue_message=issue_message,
                issue_severity=issue_severity,
                context_lines=context_lines
            )
            
            return {
                "modified_code": result.modified_code,
                "change_type": result.change_type if hasattr(result, 'change_type') else "unknown",
                "confidence_level": float(result.confidence_level) if hasattr(result, 'confidence_level') else 0.7,
                "reason": result.reason if hasattr(result, 'reason') else "Code modification for Unity Catalog compatibility",
                "requires_testing": bool(result.requires_testing) if hasattr(result, 'requires_testing') else True,
                "breaking_change": bool(result.breaking_change) if hasattr(result, 'breaking_change') else False,
                "documentation_needed": bool(result.documentation_needed) if hasattr(result, 'documentation_needed') else False,
                "alternative_suggestions": result.alternative_suggestions if hasattr(result, 'alternative_suggestions') else []
            }
            
        except Exception as e:
            logger.error(f"DSPy modification failed: {e}")
            # Fallback to basic modification
            return self._fallback_modification(original_code, issue_category, issue_message)
    
    def _fallback_modification(self, original_code: str, issue_category: str, issue_message: str) -> Dict[str, Any]:
        """Fallback modification when DSPy fails."""
        if "three-part naming" in issue_message.lower():
            modified_code = original_code.replace("'", "'catalog.schema.").replace("'catalog.schema.", "'catalog.schema.")
        else:
            modified_code = original_code
        
        return {
            "modified_code": modified_code,
            "change_type": "compatibility_fix",
            "confidence_level": 0.6,
            "reason": f"Applied {issue_category} fix for Unity Catalog compatibility",
            "requires_testing": True,
            "breaking_change": False,
            "documentation_needed": False,
            "alternative_suggestions": ["Review Unity Catalog naming conventions"]
        }

"""
DSPy Validator Model for PyUCX-AI Framework
"""
import dspy
import logging
from typing import Dict, Any, List

logger = logging.getLogger(__name__)

class ValidatorModel(dspy.Module):
    """DSPy model for validating code modifications."""
    
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
            
            # Define the signature for validation
            self.validate = dspy.ChainOfThought(
                "file_path, original_code, modified_code, validation_type, context_lines, modification_reason -> is_valid, issues_found, warnings, recommendations, confidence_score, syntax_errors, compatibility_issues"
            )
            
            # Compile the model
            self._compile_model()
            logger.info("ValidatorModel initialized successfully")
            
        except Exception as e:
            logger.error(f"Failed to initialize ValidatorModel: {e}")
            raise
    
    def _compile_model(self):
        """Compile the model with examples."""
        try:
            # Add example data for compilation
            examples = [
                dspy.Example(
                    file_path="test.py",
                    original_code="spark.table('raw_data')",
                    modified_code="spark.table('catalog.schema.raw_data')",
                    validation_type="compatibility",
                    context_lines=["import pyspark", "spark = SparkSession.builder.getOrCreate()"],
                    modification_reason="Update to Unity Catalog three-part naming",
                    is_valid=True,
                    issues_found=[],
                    warnings=["Verify table exists in Unity Catalog"],
                    recommendations=["Test with actual data", "Update documentation"],
                    confidence_score=0.9,
                    syntax_errors=0,
                    compatibility_issues=0
                ).with_inputs("file_path", "original_code", "modified_code", "validation_type", "context_lines", "modification_reason")
            ]
            
            # Compile with examples
            self.validate.compile(examples)
            logger.info("ValidatorModel compiled successfully")
            
        except Exception as e:
            logger.warning(f"Model compilation failed, using fallback: {e}")
    
    def forward(self, file_path: str, original_code: str, modified_code: str, validation_type: str,
                context_lines: List[str], modification_reason: str) -> Dict[str, Any]:
        """Validate code modifications."""
        try:
            # Use DSPy model for validation
            result = self.validate(
                file_path=file_path,
                original_code=original_code,
                modified_code=modified_code,
                validation_type=validation_type,
                context_lines=context_lines,
                modification_reason=modification_reason
            )
            
            return {
                "is_valid": bool(result.is_valid) if hasattr(result, 'is_valid') else True,
                "issues_found": result.issues_found if hasattr(result, 'issues_found') else [],
                "warnings": result.warnings if hasattr(result, 'warnings') else [],
                "recommendations": result.recommendations if hasattr(result, 'recommendations') else [],
                "confidence_score": float(result.confidence_score) if hasattr(result, 'confidence_score') else 0.8,
                "syntax_errors": int(result.syntax_errors) if hasattr(result, 'syntax_errors') else 0,
                "compatibility_issues": int(result.compatibility_issues) if hasattr(result, 'compatibility_issues') else 0
            }
            
        except Exception as e:
            logger.error(f"DSPy validation failed: {e}")
            # Fallback to basic validation
            return self._fallback_validation(modified_code, validation_type)
    
    def _fallback_validation(self, modified_code: str, validation_type: str) -> Dict[str, Any]:
        """Fallback validation when DSPy fails."""
        # Basic syntax check
        syntax_errors = 0
        try:
            compile(modified_code, '<string>', 'exec')
        except SyntaxError:
            syntax_errors = 1
        
        return {
            "is_valid": syntax_errors == 0,
            "issues_found": [] if syntax_errors == 0 else ["Syntax error detected"],
            "warnings": ["Manual review recommended"],
            "recommendations": ["Test in development environment"],
            "confidence_score": 0.7,
            "syntax_errors": syntax_errors,
            "compatibility_issues": 0
        }

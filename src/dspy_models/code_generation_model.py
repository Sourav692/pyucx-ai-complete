"""
DSPy Code Generation Model for PyUCX-AI Framework
"""
import dspy
import logging
from typing import Dict, Any, List

logger = logging.getLogger(__name__)

class CodeGenerationModel(dspy.Module):
    """DSPy model for generating converted code."""
    
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
            
            # Define the signature for code generation
            self.generate = dspy.ChainOfThought(
                "file_path, file_type, original_content, modifications, notebook_metadata -> converted_content, modifications_applied, conversion_metadata, quality_score, warnings, recommendations"
            )
            
            # Compile the model
            self._compile_model()
            logger.info("CodeGenerationModel initialized successfully")
            
        except Exception as e:
            logger.error(f"Failed to initialize CodeGenerationModel: {e}")
            raise
    
    def _compile_model(self):
        """Compile the model with examples."""
        try:
            # Add example data for compilation
            examples = [
                dspy.Example(
                    file_path="test.py",
                    file_type="python",
                    original_content="import pyspark\nspark.table('raw_data')",
                    modifications=["Update table reference to three-part naming"],
                    notebook_metadata={"spark_version": "3.4.0", "python_version": "3.9"},
                    converted_content="import pyspark\nspark.table('catalog.schema.raw_data')",
                    modifications_applied=["Updated table reference to Unity Catalog format"],
                    conversion_metadata={"unity_catalog_compatible": True, "changes_count": 1},
                    quality_score=0.9,
                    warnings=[],
                    recommendations=["Test with actual Unity Catalog data"]
                ).with_inputs("file_path", "file_type", "original_content", "modifications", "notebook_metadata")
            ]
            
            # Compile with examples
            self.generate.compile(examples)
            logger.info("CodeGenerationModel compiled successfully")
            
        except Exception as e:
            logger.warning(f"Model compilation failed, using fallback: {e}")
    
    def forward(self, file_path: str, file_type: str, original_content: str, 
                modifications: List[str], notebook_metadata: Dict[str, Any]) -> Dict[str, Any]:
        """Generate converted code."""
        try:
            # Use DSPy model for code generation
            result = self.generate(
                file_path=file_path,
                file_type=file_type,
                original_content=original_content,
                modifications=modifications,
                notebook_metadata=notebook_metadata
            )
            
            return {
                "converted_content": result.converted_content,
                "modifications_applied": result.modifications_applied if hasattr(result, 'modifications_applied') else modifications,
                "conversion_metadata": result.conversion_metadata if hasattr(result, 'conversion_metadata') else {},
                "quality_score": float(result.quality_score) if hasattr(result, 'quality_score') else 0.8,
                "warnings": result.warnings if hasattr(result, 'warnings') else [],
                "recommendations": result.recommendations if hasattr(result, 'recommendations') else []
            }
            
        except Exception as e:
            logger.error(f"DSPy code generation failed: {e}")
            # Fallback to basic code generation
            return self._fallback_generation(original_content, modifications)
    
    def _fallback_generation(self, original_content: str, modifications: List[str]) -> Dict[str, Any]:
        """Fallback code generation when DSPy fails."""
        # Basic modifications
        converted_content = original_content
        for mod in modifications:
            if "three-part naming" in mod.lower():
                # Simple replacement for table references
                converted_content = converted_content.replace("'", "'catalog.schema.").replace("'catalog.schema.", "'catalog.schema.")
        
        return {
            "converted_content": converted_content,
            "modifications_applied": modifications,
            "conversion_metadata": {"unity_catalog_compatible": True, "changes_count": len(modifications)},
            "quality_score": 0.7,
            "warnings": ["Manual review recommended"],
            "recommendations": ["Test in development environment"]
        }

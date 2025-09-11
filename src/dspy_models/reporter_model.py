"""
DSPy Reporter Model for PyUCX-AI Framework
"""
import dspy
import logging
from typing import Dict, Any, List

logger = logging.getLogger(__name__)

class ReporterModel(dspy.Module):
    """DSPy model for generating reports."""
    
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
            
            # Define the signature for reporting
            self.report = dspy.ChainOfThought(
                "notebook_info, analysis_summary, migration_plan_summary, modifications_summary, validation_summary, is_final_report, overall_statistics -> report_content, executive_summary, recommendations, risk_assessment, timeline_estimation, quality_score, report_type"
            )
            
            # Compile the model
            self._compile_model()
            logger.info("ReporterModel initialized successfully")
            
        except Exception as e:
            logger.error(f"Failed to initialize ReporterModel: {e}")
            raise
    
    def _compile_model(self):
        """Compile the model with examples."""
        try:
            # Add example data for compilation
            examples = [
                dspy.Example(
                    notebook_info={"total_notebooks": 10, "processed": 10},
                    analysis_summary={"issues_found": 25, "complexity": "medium"},
                    migration_plan_summary={"estimated_days": 5, "priority": "high"},
                    modifications_summary={"code_changes": 15, "breaking_changes": 2},
                    validation_summary={"passed": 8, "failed": 2},
                    is_final_report=True,
                    overall_statistics={"total_issues": 25, "resolved": 20},
                    report_content="Migration analysis completed for 10 notebooks...",
                    executive_summary="10 notebooks analyzed, 25 issues found, 5-day migration plan",
                    recommendations=["Set up Unity Catalog test environment", "Train development team"],
                    risk_assessment="Medium risk due to 2 breaking changes",
                    timeline_estimation="5 days for complete migration",
                    quality_score=0.85,
                    report_type="migration_analysis"
                ).with_inputs("notebook_info", "analysis_summary", "migration_plan_summary", "modifications_summary", "validation_summary", "is_final_report", "overall_statistics")
            ]
            
            # Compile with examples
            self.report.compile(examples)
            logger.info("ReporterModel compiled successfully")
            
        except Exception as e:
            logger.warning(f"Model compilation failed, using fallback: {e}")
    
    def forward(self, notebook_info: Dict[str, Any], analysis_summary: Dict[str, Any], 
                migration_plan_summary: Dict[str, Any], modifications_summary: Dict[str, Any],
                validation_summary: Dict[str, Any], is_final_report: bool, 
                overall_statistics: Dict[str, Any]) -> Dict[str, Any]:
        """Generate report."""
        try:
            # Use DSPy model for reporting
            result = self.report(
                notebook_info=notebook_info,
                analysis_summary=analysis_summary,
                migration_plan_summary=migration_plan_summary,
                modifications_summary=modifications_summary,
                validation_summary=validation_summary,
                is_final_report=is_final_report,
                overall_statistics=overall_statistics
            )
            
            return {
                "report_content": result.report_content,
                "executive_summary": result.executive_summary if hasattr(result, 'executive_summary') else "Migration analysis completed",
                "recommendations": result.recommendations if hasattr(result, 'recommendations') else [],
                "risk_assessment": result.risk_assessment if hasattr(result, 'risk_assessment') else "Medium risk",
                "timeline_estimation": result.timeline_estimation if hasattr(result, 'timeline_estimation') else "TBD",
                "quality_score": float(result.quality_score) if hasattr(result, 'quality_score') else 0.8,
                "report_type": result.report_type if hasattr(result, 'report_type') else "migration_analysis"
            }
            
        except Exception as e:
            logger.error(f"DSPy reporting failed: {e}")
            # Fallback to basic reporting
            return self._fallback_reporting(notebook_info, analysis_summary, overall_statistics)
    
    def _fallback_reporting(self, notebook_info: Dict[str, Any], analysis_summary: Dict[str, Any], 
                           overall_statistics: Dict[str, Any]) -> Dict[str, Any]:
        """Fallback reporting when DSPy fails."""
        return {
            "report_content": f"Migration analysis completed for {notebook_info.get('total_notebooks', 0)} notebooks",
            "executive_summary": f"Analysis found {analysis_summary.get('issues_found', 0)} issues across {notebook_info.get('total_notebooks', 0)} notebooks",
            "recommendations": ["Review Unity Catalog documentation", "Set up test environment"],
            "risk_assessment": "Medium risk - manual review recommended",
            "timeline_estimation": "TBD based on complexity",
            "quality_score": 0.7,
            "report_type": "migration_analysis"
        }

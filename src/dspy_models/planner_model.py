"""
DSPy Planner Model for PyUCX-AI Framework
"""
import dspy
import logging
from typing import Dict, Any

logger = logging.getLogger(__name__)

class PlannerModel(dspy.Module):
    """DSPy model for creating migration plans."""
    
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
            
            # Define the signature for planning
            self.plan = dspy.ChainOfThought(
                "notebook_path, migration_complexity, issues_found, issues_by_severity, issues_by_category, estimated_effort_hours, migration_recommendations -> priority, estimated_duration_days, preparation_steps, code_changes, testing_steps, deployment_steps, dependencies, prerequisites, risks, mitigation_strategies, timeline_phases"
            )
            
            # Compile the model
            self._compile_model()
            logger.info("PlannerModel initialized successfully")
            
        except Exception as e:
            logger.error(f"Failed to initialize PlannerModel: {e}")
            raise
    
    def _compile_model(self):
        """Compile the model with examples."""
        try:
            # Add example data for compilation
            examples = [
                dspy.Example(
                    notebook_path="test.py",
                    migration_complexity="medium",
                    issues_found=3,
                    issues_by_severity={"error": 1, "warning": 2},
                    issues_by_category={"spark": 2, "sql": 1},
                    estimated_effort_hours=4,
                    migration_recommendations=["Update table references", "Fix deprecated methods"],
                    priority="high",
                    estimated_duration_days=2,
                    preparation_steps=["Review Unity Catalog documentation", "Set up test environment"],
                    code_changes=["Update table names", "Replace deprecated methods"],
                    testing_steps=["Unit tests", "Integration tests"],
                    deployment_steps=["Staging deployment", "Production deployment"],
                    dependencies=["Unity Catalog access", "Updated Spark version"],
                    prerequisites=["Team training", "Environment setup"],
                    risks=["Data loss", "Downtime"],
                    mitigation_strategies=["Backup data", "Rollback plan"],
                    timeline_phases=["Preparation (1 day)", "Implementation (1 day)"]
                ).with_inputs("notebook_path", "migration_complexity", "issues_found", "issues_by_severity", "issues_by_category", "estimated_effort_hours", "migration_recommendations")
            ]
            
            # Compile with examples
            self.plan.compile(examples)
            logger.info("PlannerModel compiled successfully")
            
        except Exception as e:
            logger.warning(f"Model compilation failed, using fallback: {e}")
    
    def forward(self, notebook_path: str, migration_complexity: str, issues_found: int, 
                issues_by_severity: Dict, issues_by_category: Dict, estimated_effort_hours: int, 
                migration_recommendations: list) -> Dict[str, Any]:
        """Create migration plan."""
        try:
            # Use DSPy model for planning
            result = self.plan(
                notebook_path=notebook_path,
                migration_complexity=migration_complexity,
                issues_found=issues_found,
                issues_by_severity=issues_by_severity,
                issues_by_category=issues_by_category,
                estimated_effort_hours=estimated_effort_hours,
                migration_recommendations=migration_recommendations
            )
            
            return {
                "priority": result.priority,
                "estimated_duration_days": int(result.estimated_duration_days) if result.estimated_duration_days else 1,
                "preparation_steps": result.preparation_steps if hasattr(result, 'preparation_steps') else [],
                "code_changes": result.code_changes if hasattr(result, 'code_changes') else [],
                "testing_steps": result.testing_steps if hasattr(result, 'testing_steps') else [],
                "deployment_steps": result.deployment_steps if hasattr(result, 'deployment_steps') else [],
                "dependencies": result.dependencies if hasattr(result, 'dependencies') else [],
                "prerequisites": result.prerequisites if hasattr(result, 'prerequisites') else [],
                "risks": result.risks if hasattr(result, 'risks') else [],
                "mitigation_strategies": result.mitigation_strategies if hasattr(result, 'mitigation_strategies') else [],
                "timeline_phases": result.timeline_phases if hasattr(result, 'timeline_phases') else []
            }
            
        except Exception as e:
            logger.error(f"DSPy planning failed: {e}")
            # Fallback to basic planning
            return self._fallback_planning(notebook_path, migration_complexity, issues_found, estimated_effort_hours)
    
    def _fallback_planning(self, notebook_path: str, migration_complexity: str, issues_found: int, estimated_effort_hours: int) -> Dict[str, Any]:
        """Fallback planning when DSPy fails."""
        return {
            "priority": "medium" if migration_complexity == "low" else "high",
            "estimated_duration_days": max(1, estimated_effort_hours // 8),
            "preparation_steps": ["Review Unity Catalog documentation", "Set up test environment"],
            "code_changes": ["Update table references", "Fix compatibility issues"],
            "testing_steps": ["Unit tests", "Integration tests"],
            "deployment_steps": ["Staging deployment", "Production deployment"],
            "dependencies": ["Unity Catalog access"],
            "prerequisites": ["Team training"],
            "risks": ["Data loss"],
            "mitigation_strategies": ["Backup data", "Rollback plan"],
            "timeline_phases": ["Preparation", "Implementation", "Testing"]
        }
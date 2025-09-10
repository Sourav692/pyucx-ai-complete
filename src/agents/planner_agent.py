"""
Planner Agent for the PyUCX-AI Multi-Agent Framework.

This agent creates detailed migration plans based on analysis results,
including timeline, dependencies, and risk assessment.
"""

import json
import logging
from typing import Dict, List, Any, Optional
from langchain_core.messages import HumanMessage, SystemMessage

from .base_agent import BaseAgent
from ..core.agent_state import AgentState, MigrationPlan

logger = logging.getLogger(__name__)


class PlannerAgent(BaseAgent):
    """Agent responsible for creating migration plans."""

    def execute(self, state: AgentState) -> AgentState:
        """Create a detailed migration plan for the current notebook."""

        try:
            self._increment_iteration(state)
            self._add_message(state, "Creating migration plan")

            # Get current analysis result
            current_analysis = self._get_current_analysis(state)
            if not current_analysis:
                return self._handle_error(state, "No analysis result found for planning")

            # Create migration plan
            migration_plan = self._create_migration_plan(current_analysis, state)

            # Add plan to state
            state["migration_plans"].append(migration_plan.to_dict())

            self._add_message(
                state,
                f"Created migration plan for {migration_plan.notebook_path}: "
                f"Priority {migration_plan.priority}, "
                f"Estimated {migration_plan.estimated_duration_days} days"
            )

            return state

        except Exception as e:
            return self._handle_error(state, f"Planning failed: {str(e)}")

    def _get_current_analysis(self, state: AgentState) -> Optional[Dict[str, Any]]:
        """Get the most recent analysis result."""
        analysis_results = state.get("analysis_results", [])
        if analysis_results:
            return analysis_results[-1]
        return None

    def _create_migration_plan(
        self, 
        analysis: Dict[str, Any], 
        state: AgentState
    ) -> MigrationPlan:
        """Create a comprehensive migration plan."""

        notebook_path = analysis["notebook_path"]
        complexity = analysis.get("migration_complexity", "medium")
        issues_found = analysis.get("issues_found", 0)

        plan = MigrationPlan(notebook_path=notebook_path)

        # Set priority based on complexity and issues
        plan.priority = self._determine_priority(complexity, issues_found, analysis)

        # Generate plan steps using LLM
        plan_details = self._generate_plan_details(analysis)

        # Populate plan components
        plan.preparation_steps = plan_details.get("preparation_steps", [])
        plan.code_changes = plan_details.get("code_changes", [])
        plan.testing_steps = plan_details.get("testing_steps", [])
        plan.deployment_steps = plan_details.get("deployment_steps", [])

        # Set dependencies and prerequisites
        plan.dependencies = self._identify_dependencies(analysis)
        plan.prerequisites = self._identify_prerequisites(analysis)

        # Risk assessment
        plan.risks = self._assess_risks(analysis)
        plan.mitigation_strategies = self._develop_mitigation_strategies(plan.risks)

        # Timeline estimation
        plan.estimated_duration_days = self._estimate_timeline(complexity, issues_found)
        plan.timeline_phases = self._create_timeline_phases(plan)

        return plan

    def _determine_priority(
        self, 
        complexity: str, 
        issues_found: int, 
        analysis: Dict[str, Any]
    ) -> str:
        """Determine migration priority based on analysis."""

        # Check for critical issues
        issues_by_severity = analysis.get("issues_by_severity", {})
        error_count = issues_by_severity.get("error", 0)

        if error_count > 5 or complexity == "high":
            return "high"
        elif error_count > 0 or complexity == "medium":
            return "medium"
        else:
            return "low"

    def _generate_plan_details(self, analysis: Dict[str, Any]) -> Dict[str, List]:
        """Generate detailed plan steps using LLM."""

        try:
            notebook_path = analysis["notebook_path"]
            complexity = analysis.get("migration_complexity", "medium")
            issues = analysis.get("compatibility_issues", [])

            # Prepare context for LLM
            issues_summary = self._format_issues_for_planning(issues)

            prompt = f"""
            Create a detailed migration plan for the following Jupyter notebook to Unity Catalog:

            Notebook: {notebook_path}
            Complexity: {complexity}
            Issues Summary:
            {issues_summary}

            Provide a structured migration plan with the following sections:

            1. PREPARATION_STEPS: Steps to prepare for migration
            2. CODE_CHANGES: Specific code modifications needed
            3. TESTING_STEPS: Steps to validate the migration
            4. DEPLOYMENT_STEPS: Steps to deploy to production

            Format each section as a numbered list. Be specific and actionable.
            """

            messages = [
                SystemMessage(content="You are a Unity Catalog migration expert. Create detailed, actionable migration plans."),
                HumanMessage(content=prompt)
            ]

            response = self.llm.invoke(messages)
            return self._parse_plan_response(response.content)

        except Exception as e:
            logger.error(f"Failed to generate plan details: {e}")
            return self._get_default_plan_structure()

    def _format_issues_for_planning(self, issues: List[Dict[str, Any]]) -> str:
        """Format compatibility issues for planning context."""

        if not issues:
            return "No compatibility issues found."

        lines = []
        categories = {}

        for issue in issues:
            category = issue.get("category", "general")
            if category not in categories:
                categories[category] = []
            categories[category].append(issue)

        for category, category_issues in categories.items():
            lines.append(f"\n{category.upper()}:")
            for issue in category_issues[:3]:  # Show top 3 per category
                severity = issue.get("severity", "")
                message = issue.get("message", "")
                lines.append(f"  - [{severity}] {message}")

        return "\n".join(lines)

    def _parse_plan_response(self, response_text: str) -> Dict[str, List]:
        """Parse LLM response into structured plan sections."""

        sections = {
            "preparation_steps": [],
            "code_changes": [],
            "testing_steps": [],
            "deployment_steps": []
        }

        current_section = None

        for line in response_text.split("\n"):
            line = line.strip()
            if not line:
                continue

            # Identify section headers
            if "PREPARATION" in line.upper():
                current_section = "preparation_steps"
            elif "CODE" in line.upper() and "CHANGE" in line.upper():
                current_section = "code_changes"
            elif "TESTING" in line.upper():
                current_section = "testing_steps"
            elif "DEPLOYMENT" in line.upper():
                current_section = "deployment_steps"
            elif current_section and (line.startswith("-") or line[0].isdigit()):
                # Extract step text
                step_text = line.lstrip("0123456789.-").strip()
                if step_text:
                    sections[current_section].append(step_text)

        # Ensure all sections have at least default steps
        for section, steps in sections.items():
            if not steps:
                sections[section] = self._get_default_steps(section)

        return sections

    def _get_default_plan_structure(self) -> Dict[str, List]:
        """Get default plan structure when LLM fails."""

        return {
            "preparation_steps": [
                "Review notebook dependencies and imports",
                "Backup current notebook version",
                "Set up Unity Catalog test environment",
                "Identify data sources and table dependencies"
            ],
            "code_changes": [
                "Update table references to use three-part naming",
                "Modify Spark configuration for Unity Catalog",
                "Update authentication and connection parameters",
                "Review and update SQL queries for compatibility"
            ],
            "testing_steps": [
                "Test notebook execution in Unity Catalog environment",
                "Validate data access and query results",
                "Performance testing and optimization",
                "End-to-end integration testing"
            ],
            "deployment_steps": [
                "Deploy to staging environment",
                "User acceptance testing",
                "Production deployment",
                "Monitor and validate post-deployment"
            ]
        }

    def _get_default_steps(self, section: str) -> List[str]:
        """Get default steps for a specific section."""

        defaults = {
            "preparation_steps": [
                "Review notebook for Unity Catalog readiness",
                "Backup existing notebook",
                "Prepare test environment"
            ],
            "code_changes": [
                "Update table references",
                "Modify authentication parameters",
                "Review Spark configurations"
            ],
            "testing_steps": [
                "Execute notebook in test environment",
                "Validate output and performance",
                "Run integration tests"
            ],
            "deployment_steps": [
                "Deploy to staging",
                "Production deployment",
                "Post-deployment validation"
            ]
        }

        return defaults.get(section, ["Complete section tasks"])

    def _identify_dependencies(self, analysis: Dict[str, Any]) -> List[str]:
        """Identify migration dependencies."""

        dependencies = []

        # Check for Spark-related dependencies
        issues_by_category = analysis.get("issues_by_category", {})

        if issues_by_category.get("spark", 0) > 0:
            dependencies.append("Spark 3.x compatibility verification")

        if issues_by_category.get("sql", 0) > 0:
            dependencies.append("SQL query validation and testing")

        if issues_by_category.get("databricks", 0) > 0:
            dependencies.append("Databricks-specific feature migration")

        # General dependencies
        dependencies.extend([
            "Unity Catalog workspace setup",
            "Access permissions configuration",
            "Test environment provisioning"
        ])

        return dependencies

    def _identify_prerequisites(self, analysis: Dict[str, Any]) -> List[str]:
        """Identify migration prerequisites."""

        return [
            "Unity Catalog enabled in workspace",
            "Appropriate access permissions granted",
            "Backup of original notebook created",
            "Test data available in Unity Catalog",
            "Migration timeline approved by stakeholders"
        ]

    def _assess_risks(self, analysis: Dict[str, Any]) -> List[Dict[str, str]]:
        """Assess migration risks."""

        risks = []

        complexity = analysis.get("migration_complexity", "medium")
        issues_count = analysis.get("issues_found", 0)

        if complexity == "high":
            risks.append({
                "risk": "High complexity migration may require significant code changes",
                "impact": "high",
                "probability": "medium"
            })

        if issues_count > 10:
            risks.append({
                "risk": "Large number of compatibility issues may extend timeline",
                "impact": "medium",
                "probability": "high"
            })

        # General risks
        risks.extend([
            {
                "risk": "Performance degradation after migration",
                "impact": "medium",
                "probability": "low"
            },
            {
                "risk": "Data access permission issues",
                "impact": "high",
                "probability": "medium"
            },
            {
                "risk": "Unexpected compatibility issues during testing",
                "impact": "medium",
                "probability": "medium"
            }
        ])

        return risks

    def _develop_mitigation_strategies(self, risks: List[Dict[str, str]]) -> List[str]:
        """Develop mitigation strategies for identified risks."""

        strategies = [
            "Conduct thorough testing in staging environment before production",
            "Implement rollback plan for quick reversion if needed",
            "Schedule migration during low-usage periods",
            "Have Unity Catalog expert available during migration",
            "Monitor performance metrics closely post-migration"
        ]

        return strategies

    def _estimate_timeline(self, complexity: str, issues_count: int) -> int:
        """Estimate timeline in days."""

        base_days = {
            "low": 1,
            "medium": 3,
            "high": 7
        }

        days = base_days.get(complexity, 3)

        # Add days based on issue count
        if issues_count > 10:
            days += 2
        elif issues_count > 5:
            days += 1

        return days

    def _create_timeline_phases(self, plan: MigrationPlan) -> List[Dict[str, Any]]:
        """Create timeline phases for the migration plan."""

        total_days = plan.estimated_duration_days

        phases = [
            {
                "phase": "Preparation",
                "duration_days": max(1, total_days // 4),
                "tasks": plan.preparation_steps
            },
            {
                "phase": "Code Changes",
                "duration_days": max(1, total_days // 2),
                "tasks": ["Implement code modifications", "Review changes"]
            },
            {
                "phase": "Testing",
                "duration_days": max(1, total_days // 4),
                "tasks": plan.testing_steps
            },
            {
                "phase": "Deployment",
                "duration_days": 1,
                "tasks": plan.deployment_steps
            }
        ]

        return phases

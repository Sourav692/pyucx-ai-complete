"""
Analyzer Agent for the PyUCX-AI Multi-Agent Framework.

This agent analyzes Jupyter notebooks and UCX lint data to identify
Unity Catalog migration issues and compatibility problems.
"""

import json
import logging
from typing import Dict, List, Any, Optional
from langchain_core.messages import HumanMessage, SystemMessage

from .base_agent import BaseAgent
from ..core.agent_state import AgentState, LintIssue, AnalysisResult

logger = logging.getLogger(__name__)


class AnalyzerAgent(BaseAgent):
    """Agent responsible for analyzing notebooks and lint data."""

    def execute(self, state: AgentState) -> AgentState:
        """Analyze the current notebook and associated lint issues."""

        try:
            self._increment_iteration(state)
            self._add_message(state, "Starting notebook analysis")

            # Get current notebook or next unprocessed one
            current_notebook = self._get_current_notebook(state)
            if not current_notebook:
                self._add_message(state, "No more notebooks to process")
                state["should_continue"] = False
                return state

            state["current_notebook"] = current_notebook
            
            # Reset iteration count for new file processing
            if state.get("iteration_count", 0) > 0:
                state["iteration_count"] = 0

            # Get lint issues for this notebook
            lint_issues = self._get_lint_issues_for_notebook(state, current_notebook)
            state["current_lint_issues"] = lint_issues

            # Perform detailed analysis
            analysis_result = self._analyze_notebook(current_notebook, lint_issues)

            # Add analysis result to state
            state["analysis_results"].append(analysis_result.to_dict())

            self._add_message(
                state, 
                f"Completed analysis of {current_notebook['filename']}: "
                f"{len(lint_issues)} issues found, "
                f"complexity: {analysis_result.migration_complexity}"
            )

            return state

        except Exception as e:
            return self._handle_error(state, f"Analysis failed: {str(e)}")

    def _get_current_notebook(self, state: AgentState) -> Optional[Dict[str, Any]]:
        """Get the next notebook to process."""

        # Count unique notebooks that have been converted (fully processed)
        converted_notebooks = set()
        for converted in state.get("converted_notebooks", []):
            converted_notebooks.add(converted.get("original_path"))
        
        notebooks = state["notebooks"]
        
        # Find the first notebook that hasn't been converted yet
        for notebook in notebooks:
            notebook_path = notebook.get("path")
            if notebook_path not in converted_notebooks:
                return notebook
        return None

    def _get_lint_issues_for_notebook(
        self, 
        state: AgentState, 
        notebook: Dict[str, Any]
    ) -> List[Dict[str, Any]]:
        """Get lint issues specific to the current notebook."""

        notebook_path = notebook["path"]
        notebook_filename = notebook["filename"]

        # Look for issues matching this notebook
        all_lint_data = state["lint_data"]
        issues = []

        for file_path, file_issues in all_lint_data.items():
            # Match by filename or path
            if (notebook_filename in file_path or 
                notebook_path in file_path or
                file_path.endswith(notebook_filename)):
                issues.extend(file_issues)

        return issues

    def _analyze_notebook(
        self, 
        notebook: Dict[str, Any], 
        lint_issues: List[Dict[str, Any]]
    ) -> AnalysisResult:
        """Perform detailed analysis of the notebook."""

        analysis = AnalysisResult(notebook_path=notebook["path"])

        # Basic metrics
        analysis.total_cells = notebook.get("cell_count", 0)
        analysis.code_cells = notebook.get("code_cell_count", 0)
        analysis.issues_found = len(lint_issues)

        # Convert lint issues to LintIssue objects
        analysis.compatibility_issues = [
            LintIssue.from_dict(issue) for issue in lint_issues
        ]

        # Analyze issue severity distribution
        severity_counts = {}
        category_counts = {}

        for issue in lint_issues:
            severity = issue.get("severity", "unknown")
            category = issue.get("category", "general")

            severity_counts[severity] = severity_counts.get(severity, 0) + 1
            category_counts[category] = category_counts.get(category, 0) + 1

        analysis.issues_by_severity = severity_counts
        analysis.issues_by_category = category_counts

        # Determine complexity and effort
        analysis.migration_complexity = self._assess_complexity(notebook, lint_issues)
        analysis.estimated_effort_hours = self._estimate_effort(
            analysis.migration_complexity, 
            len(lint_issues),
            analysis.code_cells
        )

        # Generate recommendations using LLM
        analysis.migration_recommendations = self._generate_recommendations(
            notebook, 
            lint_issues
        )

        return analysis

    def _assess_complexity(
        self, 
        notebook: Dict[str, Any], 
        lint_issues: List[Dict[str, Any]]
    ) -> str:
        """Assess migration complexity based on notebook and issues."""

        code_cells = notebook.get("code_cell_count", 0)
        error_count = len([i for i in lint_issues if i.get("severity") == "error"])
        warning_count = len([i for i in lint_issues if i.get("severity") == "warning"])

        # Calculate complexity score
        complexity_score = 0

        # Base complexity from code size
        if code_cells > 20:
            complexity_score += 3
        elif code_cells > 10:
            complexity_score += 2
        elif code_cells > 5:
            complexity_score += 1

        # Add complexity from errors
        complexity_score += error_count * 2
        complexity_score += warning_count

        # Check for complex patterns
        has_spark_sql = any("spark" in str(issue.get("category", "")).lower() for issue in lint_issues)
        has_databricks_specific = any("databricks" in str(issue.get("message", "")).lower() for issue in lint_issues)

        if has_spark_sql:
            complexity_score += 2
        if has_databricks_specific:
            complexity_score += 1

        # Classify complexity
        if complexity_score >= 8:
            return "high"
        elif complexity_score >= 4:
            return "medium"
        else:
            return "low"

    def _estimate_effort(self, complexity: str, issue_count: int, code_cells: int) -> float:
        """Estimate effort in hours based on complexity."""

        base_hours = {
            "low": 2.0,
            "medium": 8.0,
            "high": 24.0
        }

        hours = base_hours.get(complexity, 4.0)

        # Add time based on issue count
        hours += issue_count * 0.5

        # Add time based on code size
        hours += code_cells * 0.2

        return round(hours, 1)

    def _generate_recommendations(
        self, 
        notebook: Dict[str, Any], 
        lint_issues: List[Dict[str, Any]]
    ) -> List[str]:
        """Generate migration recommendations using LLM."""

        if not lint_issues:
            return ["No compatibility issues found. Notebook should migrate easily."]

        try:
            # Prepare context for LLM
            issue_summary = self._summarize_issues(lint_issues)

            prompt = f"""
            Analyze the following Unity Catalog migration issues for a Jupyter notebook 
            and provide specific, actionable recommendations:

            Notebook: {notebook.get('filename', 'Unknown')}
            Code Cells: {notebook.get('code_cell_count', 0)}

            Issues Found:
            {issue_summary}

            Provide 3-5 specific recommendations for migrating this notebook to Unity Catalog.
            Focus on practical steps and prioritize the most critical issues.
            """

            messages = [
                SystemMessage(content="You are a Unity Catalog migration expert. Provide specific, actionable recommendations."),
                HumanMessage(content=prompt)
            ]

            response = self.llm.invoke(messages)

            # Parse recommendations from response
            recommendations_text = response.content
            recommendations = [
                rec.strip("- ").strip() 
                for rec in recommendations_text.split("\n") 
                if rec.strip() and not rec.strip().startswith("#")
            ]

            return recommendations[:5]  # Limit to 5 recommendations

        except Exception as e:
            logger.error(f"Failed to generate recommendations: {e}")
            return [
                "Review and update Spark SQL queries for Unity Catalog compatibility",
                "Update table references to use three-part naming (catalog.schema.table)",
                "Test data access permissions in Unity Catalog environment",
                "Validate query performance after migration"
            ]

    def _summarize_issues(self, lint_issues: List[Dict[str, Any]]) -> str:
        """Create a summary of lint issues for LLM analysis."""

        summary_lines = []

        # Group by category
        categories = {}
        for issue in lint_issues:
            category = issue.get("category", "general")
            if category not in categories:
                categories[category] = []
            categories[category].append(issue)

        for category, issues in categories.items():
            summary_lines.append(f"\n{category.upper()} Issues ({len(issues)}):")

            for issue in issues[:3]:  # Show up to 3 issues per category
                severity = issue.get("severity", "unknown")
                message = issue.get("message", "No message")
                line = issue.get("line_number", 0)
                summary_lines.append(f"  - Line {line}: [{severity}] {message}")

            if len(issues) > 3:
                summary_lines.append(f"  ... and {len(issues) - 3} more issues")

        return "\n".join(summary_lines)

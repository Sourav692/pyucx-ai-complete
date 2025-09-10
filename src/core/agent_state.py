"""
Core state management classes for the PyUCX-AI Multi-Agent Framework.

This module defines the state classes used throughout the LangGraph workflow
to manage notebook analysis, lint data, and agent communications.
"""

from typing import List, Dict, Any, Optional, TypedDict, Annotated
from dataclasses import dataclass, field
from datetime import datetime
import operator


class AgentState(TypedDict):
    """Main state class for the LangGraph workflow."""

    # Input data
    notebooks: List[Dict[str, Any]]
    lint_data: Dict[str, List[Dict[str, Any]]]

    # Processing state
    current_notebook: Optional[Dict[str, Any]]
    current_lint_issues: List[Dict[str, Any]]

    # Analysis results
    analysis_results: Annotated[List[Dict[str, Any]], operator.add]
    migration_plans: Annotated[List[Dict[str, Any]], operator.add]
    code_modifications: Annotated[List[Dict[str, Any]], operator.add]
    validation_results: Annotated[List[Dict[str, Any]], operator.add]
    converted_notebooks: Annotated[List[Dict[str, Any]], operator.add]

    # Agent communications
    agent_messages: Annotated[List[Dict[str, Any]], operator.add]

    # Workflow control
    iteration_count: int
    max_iterations: int
    should_continue: bool
    current_agent: str

    # Final outputs
    final_report: Optional[Dict[str, Any]]
    success: bool
    errors: List[str]


@dataclass
class NotebookInfo:
    """Information about a Jupyter notebook."""

    filename: str
    path: str
    content: str
    cells: List[Dict[str, Any]] = field(default_factory=list)
    metadata: Dict[str, Any] = field(default_factory=dict)
    cell_count: int = 0
    code_cell_count: int = 0

    def __post_init__(self):
        """Calculate derived fields after initialization."""
        if self.cells:
            self.cell_count = len(self.cells)
            self.code_cell_count = len([c for c in self.cells if c.get("cell_type") == "code"])


@dataclass
class LintIssue:
    """Represents a UCX lint issue."""

    file_path: str
    line_number: int
    column_number: int
    issue_type: str
    severity: str
    message: str
    code_snippet: str = ""
    suggested_fix: str = ""
    category: str = ""

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary representation."""
        return {
            "file_path": self.file_path,
            "line_number": self.line_number,
            "column_number": self.column_number,
            "issue_type": self.issue_type,
            "severity": self.severity,
            "message": self.message,
            "code_snippet": self.code_snippet,
            "suggested_fix": self.suggested_fix,
            "category": self.category
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "LintIssue":
        """Create LintIssue from dictionary."""
        return cls(
            file_path=data.get("file_path", ""),
            line_number=data.get("line_number", 0),
            column_number=data.get("column_number", 0),
            issue_type=data.get("issue_type", ""),
            severity=data.get("severity", ""),
            message=data.get("message", ""),
            code_snippet=data.get("code_snippet", ""),
            suggested_fix=data.get("suggested_fix", ""),
            category=data.get("category", "")
        )


@dataclass
class AnalysisResult:
    """Results from notebook analysis."""

    notebook_path: str
    timestamp: datetime = field(default_factory=datetime.now)

    # Analysis summary
    total_cells: int = 0
    code_cells: int = 0
    issues_found: int = 0

    # Issue breakdown
    issues_by_severity: Dict[str, int] = field(default_factory=dict)
    issues_by_category: Dict[str, int] = field(default_factory=dict)

    # Detailed findings
    compatibility_issues: List[LintIssue] = field(default_factory=list)
    migration_recommendations: List[str] = field(default_factory=list)

    # Complexity assessment
    migration_complexity: str = "low"  # low, medium, high
    estimated_effort_hours: float = 0.0

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary representation."""
        return {
            "notebook_path": self.notebook_path,
            "timestamp": self.timestamp.isoformat(),
            "total_cells": self.total_cells,
            "code_cells": self.code_cells,
            "issues_found": self.issues_found,
            "issues_by_severity": self.issues_by_severity,
            "issues_by_category": self.issues_by_category,
            "compatibility_issues": [issue.to_dict() for issue in self.compatibility_issues],
            "migration_recommendations": self.migration_recommendations,
            "migration_complexity": self.migration_complexity,
            "estimated_effort_hours": self.estimated_effort_hours
        }


@dataclass
class MigrationPlan:
    """Migration plan for a notebook."""

    notebook_path: str
    priority: str = "medium"  # low, medium, high, critical

    # Migration steps
    preparation_steps: List[str] = field(default_factory=list)
    code_changes: List[Dict[str, Any]] = field(default_factory=list)
    testing_steps: List[str] = field(default_factory=list)
    deployment_steps: List[str] = field(default_factory=list)

    # Dependencies and prerequisites
    dependencies: List[str] = field(default_factory=list)
    prerequisites: List[str] = field(default_factory=list)

    # Risk assessment
    risks: List[Dict[str, str]] = field(default_factory=list)
    mitigation_strategies: List[str] = field(default_factory=list)

    # Timeline
    estimated_duration_days: int = 1
    timeline_phases: List[Dict[str, Any]] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary representation."""
        return {
            "notebook_path": self.notebook_path,
            "priority": self.priority,
            "preparation_steps": self.preparation_steps,
            "code_changes": self.code_changes,
            "testing_steps": self.testing_steps,
            "deployment_steps": self.deployment_steps,
            "dependencies": self.dependencies,
            "prerequisites": self.prerequisites,
            "risks": self.risks,
            "mitigation_strategies": self.mitigation_strategies,
            "estimated_duration_days": self.estimated_duration_days,
            "timeline_phases": self.timeline_phases
        }


@dataclass
class CodeModification:
    """Represents a code modification for UC migration."""

    file_path: str
    cell_index: int = -1
    line_number: int = 0

    # Change details
    change_type: str = "replace"  # replace, insert, delete, modify
    original_code: str = ""
    modified_code: str = ""

    # Context
    reason: str = ""
    issue_type: str = ""
    confidence_level: float = 0.8  # 0.0 to 1.0

    # Metadata
    requires_testing: bool = True
    breaking_change: bool = False
    documentation_needed: bool = False

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary representation."""
        return {
            "file_path": self.file_path,
            "cell_index": self.cell_index,
            "line_number": self.line_number,
            "change_type": self.change_type,
            "original_code": self.original_code,
            "modified_code": self.modified_code,
            "reason": self.reason,
            "issue_type": self.issue_type,
            "confidence_level": self.confidence_level,
            "requires_testing": self.requires_testing,
            "breaking_change": self.breaking_change,
            "documentation_needed": self.documentation_needed
        }


@dataclass
class ValidationResult:
    """Results from code validation."""

    file_path: str
    validation_type: str  # syntax, compatibility, performance

    # Results
    is_valid: bool = True
    issues_found: List[str] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)

    # Metrics
    syntax_errors: int = 0
    compatibility_issues: int = 0
    performance_warnings: int = 0

    # Recommendations
    recommendations: List[str] = field(default_factory=list)
    next_steps: List[str] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary representation."""
        return {
            "file_path": self.file_path,
            "validation_type": self.validation_type,
            "is_valid": self.is_valid,
            "issues_found": self.issues_found,
            "warnings": self.warnings,
            "syntax_errors": self.syntax_errors,
            "compatibility_issues": self.compatibility_issues,
            "performance_warnings": self.performance_warnings,
            "recommendations": self.recommendations,
            "next_steps": self.next_steps
        }


def create_initial_state(
    notebooks: List[Dict[str, Any]],
    lint_data: Dict[str, List[Dict[str, Any]]],
    max_iterations: int = 200
) -> AgentState:
    """Create initial state for the workflow."""

    return {
        # Input data
        "notebooks": notebooks,
        "lint_data": lint_data,

        # Processing state
        "current_notebook": None,
        "current_lint_issues": [],

        # Analysis results
        "analysis_results": [],
        "migration_plans": [],
        "code_modifications": [],
        "validation_results": [],
        "converted_notebooks": [],

        # Agent communications
        "agent_messages": [],

        # Workflow control
        "iteration_count": 0,
        "max_iterations": max_iterations,
        "should_continue": True,
        "current_agent": "analyzer",

        # Final outputs
        "final_report": None,
        "success": False,
        "errors": []
    }


def should_continue_workflow(state: AgentState) -> bool:
    """Determine if the workflow should continue."""

    if not state["should_continue"]:
        return False

    if state["iteration_count"] >= state["max_iterations"]:
        return False

    if state["errors"] and len(state["errors"]) > 5:
        return False

    # The workflow should continue as long as we haven't reached max iterations
    # and there are no critical errors. The agent flow will handle notebook processing.
    return True


def get_next_agent(state: AgentState) -> str:
    """Determine the next agent to run based on current state."""

    current = state["current_agent"]

    # Agent flow: analyzer -> planner -> modifier -> validator -> code_generation -> reporter
    agent_flow = {
        "analyzer": "planner",
        "planner": "modifier", 
        "modifier": "validator",
        "validator": "code_generation",
        "code_generation": "reporter",
        "reporter": "analyzer"  # Loop back for next notebook
    }

    # Check if we need to move to the next notebook
    if current == "reporter":
        # Count unique notebooks that have been converted (fully processed)
        converted_notebooks = set()
        for converted in state.get("converted_notebooks", []):
            converted_notebooks.add(converted.get("original_path"))
        
        total_notebooks = len(state["notebooks"])
        
        if len(converted_notebooks) < total_notebooks:
            # Clear current notebook to start processing the next one
            state["current_notebook"] = None
            return "analyzer"  # Process next notebook
        else:
            return "END"  # All notebooks processed

    return agent_flow.get(current, "analyzer")


def get_current_notebook_for_processing(state: AgentState) -> Optional[Dict[str, Any]]:
    """Get the current notebook to process based on workflow state.

    This function provides centralized notebook selection logic for all agents.
    It manages the processing state and ensures notebooks are processed sequentially.

    Args:
        state: Current agent state containing notebooks and processing information

    Returns:
        Current notebook to process, or None if all notebooks are processed
    """

    # Check if there's already a current notebook being processed
    if state["current_notebook"] is not None:
        return state["current_notebook"]

    # Get list of all notebooks
    notebooks = state["notebooks"]
    if not notebooks:
        return None

    # Determine processing progress based on completed analysis results
    processed_count = len(state["analysis_results"])

    # If all notebooks have been analyzed, we're done
    if processed_count >= len(notebooks):
        return None

    # Return the next notebook to process
    next_notebook = notebooks[processed_count]

    # Update the current notebook in state (this will be used by subsequent agents)
    # Note: This is for tracking - actual state update happens in agent execution
    return next_notebook


def set_current_notebook(state: AgentState, notebook: Optional[Dict[str, Any]]) -> None:
    """Set the current notebook being processed.

    Args:
        state: Agent state to update
        notebook: Notebook to set as current, or None to clear
    """
    state["current_notebook"] = notebook


def mark_notebook_completed(state: AgentState) -> None:
    """Mark the current notebook as completed and clear it from current processing.

    Args:
        state: Agent state to update
    """
    state["current_notebook"] = None


def get_notebook_processing_status(state: AgentState) -> Dict[str, Any]:
    """Get detailed processing status for notebooks.

    Args:
        state: Current agent state

    Returns:
        Dictionary with processing statistics
    """
    notebooks = state["notebooks"]
    total_notebooks = len(notebooks)
    processed_count = len(state["analysis_results"])

    return {
        "total_notebooks": total_notebooks,
        "processed_count": processed_count,
        "remaining_count": total_notebooks - processed_count,
        "current_notebook": state["current_notebook"],
        "progress_percentage": (processed_count / total_notebooks * 100) if total_notebooks > 0 else 0
    }

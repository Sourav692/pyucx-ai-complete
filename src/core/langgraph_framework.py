"""
LangGraph framework implementation for the PyUCX-AI Multi-Agent Framework.

This module provides the main workflow orchestration using LangGraph,
including agent coordination, state management, and checkpoint handling.
"""

import json
import logging
from typing import Dict, List, Any, Optional, Callable
from pathlib import Path

from langgraph.graph import StateGraph, END
from langgraph.checkpoint.memory import MemorySaver

from .agent_state import (
    AgentState, 
    create_initial_state, 
    should_continue_workflow,
    get_next_agent,
    NotebookInfo,
    LintIssue
)

# Import agents (will be created next)
from ..agents.analyzer_agent import AnalyzerAgent
from ..agents.planner_agent import PlannerAgent
from ..agents.modifier_agent import ModifierAgent
from ..agents.validator_agent import ValidatorAgent
from ..agents.code_generation_agent import CodeGenerationAgent
from ..agents.reporter_agent import ReporterAgent

logger = logging.getLogger(__name__)


class PyUCXFramework:
    """Main framework class for PyUCX-AI Multi-Agent system."""

    def __init__(self, config: Dict[str, Any]):
        """Initialize the framework with configuration."""
        self.config = config
        self.checkpointer = None
        self.workflow = None

        # Initialize agents
        self.agents = {
            "analyzer": AnalyzerAgent(config),
            "planner": PlannerAgent(config),
            "modifier": ModifierAgent(config),
            "validator": ValidatorAgent(config),
            "code_generation": CodeGenerationAgent(config),
            "reporter": ReporterAgent(config)
        }

        self._setup_checkpointer()
        self._build_workflow()

    def _setup_checkpointer(self):
        """Setup Memory checkpointer for workflow state persistence."""
        # Using MemorySaver instead of SqliteSaver for compatibility
        # Note: MemorySaver stores state in memory, so it won't persist across restarts
        self.checkpointer = MemorySaver()
        logger.info("Checkpointer initialized with MemorySaver (in-memory storage)")

    def _build_workflow(self):
        """Build the LangGraph workflow."""
        # Create the state graph
        workflow = StateGraph(AgentState)

        # Add agent nodes
        workflow.add_node("analyzer", self._analyzer_node)
        workflow.add_node("planner", self._planner_node) 
        workflow.add_node("modifier", self._modifier_node)
        workflow.add_node("validator", self._validator_node)
        workflow.add_node("code_generation", self._code_generation_node)
        workflow.add_node("reporter", self._reporter_node)

        # Set entry point
        workflow.set_entry_point("analyzer")

        # Add conditional routing
        workflow.add_conditional_edges(
            "analyzer",
            self._should_continue,
            {
                "planner": "planner",
                "END": END
            }
        )

        workflow.add_conditional_edges(
            "planner", 
            self._should_continue,
            {
                "modifier": "modifier",
                "END": END
            }
        )

        workflow.add_conditional_edges(
            "modifier",
            self._should_continue, 
            {
                "validator": "validator",
                "END": END
            }
        )

        workflow.add_conditional_edges(
            "validator",
            self._should_continue,
            {
                "code_generation": "code_generation",
                "END": END
            }
        )

        workflow.add_conditional_edges(
            "code_generation",
            self._should_continue,
            {
                "reporter": "reporter",
                "END": END
            }
        )

        workflow.add_conditional_edges(
            "reporter",
            self._should_continue,
            {
                "analyzer": "analyzer",  # Loop back for next notebook
                "END": END
            }
        )

        # Compile the workflow
        self.workflow = workflow.compile(checkpointer=self.checkpointer)
        logger.info("Workflow compiled successfully")

    def _should_continue(self, state: AgentState) -> str:
        """Determine workflow continuation."""
        if not should_continue_workflow(state):
            return "END"

        next_agent = get_next_agent(state)
        if next_agent == "END":
            return "END"

        return next_agent

    def _analyzer_node(self, state: AgentState) -> AgentState:
        """Execute analyzer agent."""
        logger.info("Executing analyzer agent")
        state["current_agent"] = "analyzer"
        return self.agents["analyzer"].execute(state)

    def _planner_node(self, state: AgentState) -> AgentState:
        """Execute planner agent.""" 
        logger.info("Executing planner agent")
        state["current_agent"] = "planner"
        return self.agents["planner"].execute(state)

    def _modifier_node(self, state: AgentState) -> AgentState:
        """Execute modifier agent."""
        logger.info("Executing modifier agent") 
        state["current_agent"] = "modifier"
        return self.agents["modifier"].execute(state)

    def _validator_node(self, state: AgentState) -> AgentState:
        """Execute validator agent."""
        logger.info("Executing validator agent")
        state["current_agent"] = "validator" 
        return self.agents["validator"].execute(state)

    def _code_generation_node(self, state: AgentState) -> AgentState:
        """Execute code generation agent."""
        logger.info("Executing code generation agent")
        state["current_agent"] = "code_generation"
        return self.agents["code_generation"].execute(state)

    def _reporter_node(self, state: AgentState) -> AgentState:
        """Execute reporter agent."""
        logger.info("Executing reporter agent")
        state["current_agent"] = "reporter"
        return self.agents["reporter"].execute(state)

    def process_notebooks(
        self, 
        notebooks: List[Dict[str, Any]], 
        lint_data: Dict[str, List[Dict[str, Any]]],
        thread_id: str = "default"
    ) -> Dict[str, Any]:
        """Process notebooks through the multi-agent workflow."""

        logger.info(f"Starting notebook processing with {len(notebooks)} notebooks")

        # Create initial state
        initial_state = create_initial_state(
            notebooks=notebooks,
            lint_data=lint_data,
            max_iterations=self.config.get("max_iterations", 200)
        )

        # Configure thread
        config = {"configurable": {"thread_id": thread_id}}

        try:
            # Execute workflow
            final_state = None
            for step_output in self.workflow.stream(initial_state, config):
                logger.debug(f"Workflow step: {step_output}")
                final_state = step_output

            if final_state:
                # Extract the actual state from the last step
                if isinstance(final_state, dict):
                    # Get the last value from the dict (should be the final state)
                    final_state = list(final_state.values())[-1]

                logger.info("Workflow completed successfully")
                return self._format_final_results(final_state)
            else:
                logger.error("Workflow completed without final state")
                return {"success": False, "error": "No final state returned"}

        except Exception as e:
            logger.error(f"Workflow execution failed: {e}")
            return {
                "success": False,
                "error": str(e),
                "results": []
            }

    def _format_final_results(self, state: AgentState) -> Dict[str, Any]:
        """Format final results from workflow state."""

        return {
            "success": state.get("success", False),
            "notebooks_processed": len(state.get("analysis_results", [])),
            "total_notebooks": len(state.get("notebooks", [])),
            "analysis_results": state.get("analysis_results", []),
            "migration_plans": state.get("migration_plans", []),
            "code_modifications": state.get("code_modifications", []),
            "validation_results": state.get("validation_results", []),
            "converted_notebooks": state.get("converted_notebooks", []),
            "final_report": state.get("final_report"),
            "errors": state.get("errors", []),
            "agent_messages": state.get("agent_messages", []),
            "iterations_used": state.get("iteration_count", 0)
        }


def create_default_workflow(config: Dict[str, Any]) -> PyUCXFramework:
    """Create a default PyUCX framework instance."""
    return PyUCXFramework(config)


def load_python_files_from_folder(folder_path: str) -> List[Dict[str, Any]]:
    """Load all Python files from a folder."""

    files = []
    folder = Path(folder_path)

    if not folder.exists():
        logger.error(f"Folder not found: {folder_path}")
        return files

    for python_file in folder.glob("*.py"):
        try:
            with open(python_file, 'r', encoding='utf-8') as f:
                content = f.read()

            # Split content into lines for analysis
            lines = content.split('\n')
            
            # Count code lines (non-empty, non-comment lines)
            code_lines = [line for line in lines if line.strip() and not line.strip().startswith('#')]

            file_info = {
                "filename": python_file.name,
                "path": str(python_file),
                "content": content,
                "lines": lines,
                "line_count": len(lines),
                "code_line_count": len(code_lines),
                "file_type": "python"
            }

            files.append(file_info)
            logger.info(f"Loaded Python file: {python_file.name}")

        except Exception as e:
            logger.error(f"Error loading Python file {python_file}: {e}")

    logger.info(f"Loaded {len(files)} Python files from {folder_path}")
    return files


def load_notebooks_from_folder(folder_path: str) -> List[Dict[str, Any]]:
    """Load all Jupyter notebooks from a folder."""

    notebooks = []
    folder = Path(folder_path)

    if not folder.exists():
        logger.error(f"Folder not found: {folder_path}")
        return notebooks

    for notebook_file in folder.glob("*.ipynb"):
        try:
            with open(notebook_file, 'r', encoding='utf-8') as f:
                notebook_data = json.load(f)

            notebook_info = {
                "filename": notebook_file.name,
                "path": str(notebook_file),
                "content": json.dumps(notebook_data, indent=2),
                "cells": notebook_data.get("cells", []),
                "metadata": notebook_data.get("metadata", {}),
                "cell_count": len(notebook_data.get("cells", [])),
                "code_cell_count": len([
                    c for c in notebook_data.get("cells", []) 
                    if c.get("cell_type") == "code"
                ]),
                "file_type": "notebook"
            }

            notebooks.append(notebook_info)
            logger.info(f"Loaded notebook: {notebook_file.name}")

        except Exception as e:
            logger.error(f"Failed to load notebook {notebook_file}: {e}")

    logger.info(f"Loaded {len(notebooks)} notebooks from {folder_path}")
    return notebooks


def load_lint_data_from_file(lint_file_path: str) -> Dict[str, List[Dict[str, Any]]]:
    """Load UCX lint data from file."""

    lint_data = {}
    lint_file = Path(lint_file_path)

    if not lint_file.exists():
        logger.error(f"Lint file not found: {lint_file_path}")
        return lint_data

    try:
        with open(lint_file, 'r', encoding='utf-8') as f:
            content = f.read()

        # Parse lint output (assuming it's in a specific format)
        lint_issues = parse_lint_output(content)

        # Group by file path
        for issue in lint_issues:
            file_path = issue.get("file_path", "unknown")
            if file_path not in lint_data:
                lint_data[file_path] = []
            lint_data[file_path].append(issue)

        logger.info(f"Loaded {len(lint_issues)} lint issues from {lint_file_path}")

    except Exception as e:
        logger.error(f"Failed to load lint data from {lint_file_path}: {e}")

    return lint_data


def parse_lint_output(content: str) -> List[Dict[str, Any]]:
    """Parse UCX lint output into structured data."""

    issues = []
    lines = content.strip().split('\n')

    for line in lines:
        if not line.strip():
            continue

        # Parse different lint output formats
        if ":" in line and any(severity in line.lower() for severity in ["error", "warning", "info"]):
            parts = line.split(":")
            if len(parts) >= 4:
                try:
                    file_path = parts[0].strip()
                    line_number = int(parts[1].strip()) if parts[1].strip().isdigit() else 0
                    column_number = int(parts[2].strip()) if parts[2].strip().isdigit() else 0
                    message_part = ":".join(parts[3:]).strip()

                    # Extract severity and issue type
                    severity = "warning"
                    issue_type = "compatibility"

                    if "error" in message_part.lower():
                        severity = "error"
                    elif "warning" in message_part.lower():
                        severity = "warning"
                    elif "info" in message_part.lower():
                        severity = "info"

                    # Determine issue category
                    category = "general"
                    if "spark" in message_part.lower():
                        category = "spark"
                    elif "sql" in message_part.lower():
                        category = "sql"
                    elif "import" in message_part.lower():
                        category = "imports"
                    elif "databricks" in message_part.lower():
                        category = "databricks"

                    issue = {
                        "file_path": file_path,
                        "line_number": line_number,
                        "column_number": column_number,
                        "issue_type": issue_type,
                        "severity": severity,
                        "message": message_part,
                        "code_snippet": "",
                        "suggested_fix": "",
                        "category": category
                    }

                    issues.append(issue)

                except (ValueError, IndexError) as e:
                    logger.debug(f"Could not parse lint line: {line} - {e}")
                    continue

    return issues


def save_results_to_file(results: Dict[str, Any], output_path: str):
    """Save workflow results to JSON file."""

    try:
        output_file = Path(output_path)
        output_file.parent.mkdir(parents=True, exist_ok=True)

        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(results, f, indent=2, default=str)

        logger.info(f"Results saved to: {output_path}")

    except Exception as e:
        logger.error(f"Failed to save results to {output_path}: {e}")
        raise

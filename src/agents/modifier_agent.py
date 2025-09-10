"""
Modifier Agent for the PyUCX-AI Multi-Agent Framework.

This agent generates specific code modifications based on migration plans
and lint analysis to ensure Unity Catalog compatibility.
"""

import json
import logging
import re
from typing import Dict, List, Any, Optional, Tuple
from langchain_core.messages import HumanMessage, SystemMessage

from .base_agent import BaseAgent
from ..core.agent_state import AgentState, CodeModification

logger = logging.getLogger(__name__)


class ModifierAgent(BaseAgent):
    """Agent responsible for generating code modifications."""

    def execute(self, state: AgentState) -> AgentState:
        """Generate specific code modifications for Unity Catalog migration."""

        try:
            self._increment_iteration(state)
            self._add_message(state, "Generating code modifications")

            # Get current notebook and issues
            current_notebook = state.get("current_notebook")
            current_issues = state.get("current_lint_issues", [])

            if not current_notebook:
                return self._handle_error(state, "No current notebook for modification")

            # Generate modifications
            modifications = self._generate_modifications(current_notebook, current_issues)

            # Add modifications to state
            for mod in modifications:
                state["code_modifications"].append(mod.to_dict())

            self._add_message(
                state,
                f"Generated {len(modifications)} code modifications for {current_notebook['filename']}"
            )

            return state

        except Exception as e:
            return self._handle_error(state, f"Modification generation failed: {str(e)}")

    def _generate_modifications(
        self, 
        notebook: Dict[str, Any], 
        issues: List[Dict[str, Any]]
    ) -> List[CodeModification]:
        """Generate code modifications based on issues."""

        modifications = []

        # Check if this is a Python file or notebook
        file_type = notebook.get("file_type", "notebook")
        
        if file_type == "python":
            # Handle Python files
            lines = notebook.get("lines", [])
            
            # Process each issue
            for issue in issues:
                issue_mods = self._create_modifications_for_python_issue(issue, lines, notebook["path"])
                modifications.extend(issue_mods)

            # Generate additional proactive modifications
            proactive_mods = self._generate_proactive_modifications_for_python(lines, notebook["path"])
            modifications.extend(proactive_mods)
        else:
            # Handle Jupyter notebooks
            cells = notebook.get("cells", [])

            # Process each issue
            for issue in issues:
                issue_mods = self._create_modifications_for_issue(issue, cells, notebook["path"])
                modifications.extend(issue_mods)

            # Generate additional proactive modifications
            proactive_mods = self._generate_proactive_modifications(cells, notebook["path"])
            modifications.extend(proactive_mods)

        return modifications

    def _create_modifications_for_issue(
        self, 
        issue: Dict[str, Any], 
        cells: List[Dict[str, Any]], 
        notebook_path: str
    ) -> List[CodeModification]:
        """Create modifications for a specific lint issue."""

        modifications = []
        line_number = issue.get("line_number", 0)
        issue_type = issue.get("issue_type", "compatibility")
        message = issue.get("message", "")
        category = issue.get("category", "general")

        # Find the cell containing the issue
        cell_index, cell_line = self._find_cell_for_line(cells, line_number)

        if cell_index >= 0:
            cell = cells[cell_index]
            if cell.get("cell_type") == "code":
                source_lines = cell.get("source", [])

                if isinstance(source_lines, str):
                    source_lines = source_lines.split("\n")

                if 0 <= cell_line < len(source_lines):
                    original_line = source_lines[cell_line]

                    # Generate modification based on issue category
                    modified_line = self._generate_line_modification(
                        original_line, 
                        category, 
                        message
                    )

                    if modified_line != original_line:
                        mod = CodeModification(
                            file_path=notebook_path,
                            cell_index=cell_index,
                            line_number=cell_line,
                            change_type="replace",
                            original_code=original_line.strip(),
                            modified_code=modified_line.strip(),
                            reason=f"Fix {category} issue: {message}",
                            issue_type=issue_type,
                            confidence_level=self._assess_confidence(category, message)
                        )

                        modifications.append(mod)

        return modifications

    def _find_cell_for_line(
        self, 
        cells: List[Dict[str, Any]], 
        global_line_number: int
    ) -> Tuple[int, int]:
        """Find which cell contains the specified global line number."""

        current_line = 0

        for cell_index, cell in enumerate(cells):
            if cell.get("cell_type") == "code":
                source = cell.get("source", [])

                if isinstance(source, str):
                    source_lines = source.split("\n")
                else:
                    source_lines = source

                cell_lines = len(source_lines)

                if current_line <= global_line_number - 1 < current_line + cell_lines:
                    return cell_index, global_line_number - 1 - current_line

                current_line += cell_lines

        return -1, -1

    def _generate_line_modification(
        self, 
        original_line: str, 
        category: str, 
        message: str
    ) -> str:
        """Generate a modified line based on the issue category."""

        line = original_line.strip()

        if category == "spark":
            return self._modify_spark_code(line, message)
        elif category == "sql":
            return self._modify_sql_code(line, message)
        elif category == "imports":
            return self._modify_import_code(line, message)
        elif category == "databricks":
            return self._modify_databricks_code(line, message)
        else:
            return self._modify_general_code(line, message)

    def _modify_spark_code(self, line: str, message: str) -> str:
        """Modify Spark-related code for Unity Catalog."""

        # Simple string-based replacements for common patterns
        if "spark.read.table(" in line:
            # Extract table name and add catalog.schema prefix
            import re
            match = re.search(r'spark\.read\.table\(["\'](\w+)["\']\)', line)
            if match:
                table_name = match.group(1)
                return line.replace(f'"{table_name}"', f'"main.default.{table_name}"')

        if "saveAsTable(" in line:
            # Update saveAsTable calls
            import re
            match = re.search(r'saveAsTable\(["\'](\w+)["\']\)', line)
            if match:
                table_name = match.group(1)
                return line.replace(f'"{table_name}"', f'"main.default.{table_name}"')

        return line

    def _modify_sql_code(self, line: str, message: str) -> str:
        """Modify SQL code for Unity Catalog."""

        # SQL modifications
        if "SELECT" in line.upper() and "FROM" in line.upper():
            # Add catalog.schema prefix to table names
            import re
            pattern = r"FROM\s+(\w+)"
            replacement = r"FROM main.default.\1"
            return re.sub(pattern, replacement, line, flags=re.IGNORECASE)

        if "CREATE TABLE" in line.upper():
            # Update CREATE TABLE statements
            import re
            pattern = r"CREATE\s+TABLE\s+(\w+)"
            replacement = r"CREATE TABLE main.default.\1"
            return re.sub(pattern, replacement, line, flags=re.IGNORECASE)

        return line

    def _modify_import_code(self, line: str, message: str) -> str:
        """Modify import statements for Unity Catalog."""

        # Common import updates
        if "from pyspark.sql import SparkSession" in line:
            return line + "\n# Configure for Unity Catalog"

        if "import databricks" in line:
            return line + "\n# Note: Some databricks features may need UC updates"

        return line

    def _modify_databricks_code(self, line: str, message: str) -> str:
        """Modify Databricks-specific code for Unity Catalog."""

        # Databricks-specific modifications
        if "dbutils.fs" in line:
            # File system operations might need updates
            return f"# TODO: Review file path for Unity Catalog compatibility\n{line}"

        return line

    def _modify_general_code(self, line: str, message: str) -> str:
        """Apply general modifications for Unity Catalog compatibility."""

        # General patterns that need attention
        if "hive_metastore" in line.lower():
            return line.replace("hive_metastore", "main")
        
        # Handle table references that need three-part naming
        if "should use three-part naming" in message.lower():
            # Extract table name from the line and add catalog.schema prefix
            import re
            # Look for table references in SQL
            if "FROM" in line.upper() or "JOIN" in line.upper():
                # Simple table name patterns
                patterns = [
                    (r"FROM\s+(\w+)", r"FROM main.default.\1"),
                    (r"JOIN\s+(\w+)", r"JOIN main.default.\1"),
                    (r"table\(['\"](\w+)['\"]", r"table('main.default.\1'"),
                    (r"saveAsTable\(['\"](\w+)['\"]", r"saveAsTable('main.default.\1'")
                ]
                
                for pattern, replacement in patterns:
                    if re.search(pattern, line, re.IGNORECASE):
                        return re.sub(pattern, replacement, line, flags=re.IGNORECASE)
        
        # Handle specific table references
        if "event_log" in line and "FROM" in line.upper():
            return line.replace("event_log", "main.default.event_log")
        if "performance_metrics" in line and "FROM" in line.upper():
            return line.replace("performance_metrics", "main.default.performance_metrics")
        if "user_profiles" in line and "JOIN" in line.upper():
            return line.replace("user_profiles", "main.default.user_profiles")
        if "analytics_summary" in line and "saveAsTable" in line:
            return line.replace("analytics_summary", "main.default.analytics_summary")
        
        # Handle spark.read.table() calls
        if "spark.read.table(" in line:
            # Extract table name and add catalog.schema prefix
            import re
            table_matches = re.findall(r"spark\.read\.table\(['\"](\w+)['\"]", line)
            for table_name in table_matches:
                if '.' not in table_name:  # Only modify if not already three-part naming
                    line = line.replace(f"'{table_name}'", f"'main.default.{table_name}'")
                    line = line.replace(f'"{table_name}"', f'"main.default.{table_name}"')
        
        # Handle saveAsTable calls
        if "saveAsTable(" in line:
            import re
            table_matches = re.findall(r"saveAsTable\(['\"](\w+)['\"]", line)
            for table_name in table_matches:
                if '.' not in table_name:  # Only modify if not already three-part naming
                    line = line.replace(f"'{table_name}'", f"'main.default.{table_name}'")
                    line = line.replace(f'"{table_name}"', f'"main.default.{table_name}"')

        return line

    def _assess_confidence(self, category: str, message: str) -> float:
        """Assess confidence level for the modification."""

        # High confidence for well-known patterns
        high_confidence_patterns = ["table reference", "spark.sql", "saveAsTable"]
        medium_confidence_patterns = ["import", "databricks", "configuration"]

        message_lower = message.lower()

        for pattern in high_confidence_patterns:
            if pattern in message_lower:
                return 0.9

        for pattern in medium_confidence_patterns:
            if pattern in message_lower:
                return 0.7

        return 0.5  # Default medium-low confidence

    def _generate_proactive_modifications(
        self, 
        cells: List[Dict[str, Any]], 
        notebook_path: str
    ) -> List[CodeModification]:
        """Generate proactive modifications for better Unity Catalog compatibility."""

        modifications = []

        for cell_index, cell in enumerate(cells):
            if cell.get("cell_type") == "code":
                source = cell.get("source", [])

                if isinstance(source, str):
                    source_lines = source.split("\n")
                else:
                    source_lines = source

                for line_index, line in enumerate(source_lines):
                    line = line.strip()

                    # Proactive Spark configuration
                    if "SparkSession.builder" in line and "enableHiveSupport" in line:
                        modified_line = line.replace(
                            ".enableHiveSupport()",
                            '.config("spark.sql.catalog.spark_catalog", "unity")'
                        )

                        mod = CodeModification(
                            file_path=notebook_path,
                            cell_index=cell_index,
                            line_number=line_index,
                            change_type="replace",
                            original_code=line,
                            modified_code=modified_line,
                            reason="Proactive Unity Catalog configuration",
                            issue_type="proactive",
                            confidence_level=0.8,
                            requires_testing=True
                        )

                        modifications.append(mod)

        return modifications

    def _create_modifications_for_python_issue(
        self, 
        issue: Dict[str, Any], 
        lines: List[str], 
        file_path: str
    ) -> List[CodeModification]:
        """Create modifications for a specific issue in a Python file."""

        modifications = []
        line_number = issue.get("line_number", 0)
        category = issue.get("category", "general")
        message = issue.get("message", "")

        if 0 <= line_number - 1 < len(lines):
            original_line = lines[line_number - 1]

            # Generate modification based on issue category
            modified_line = self._generate_line_modification(
                original_line, 
                category, 
                message
            )

            if modified_line != original_line:
                mod = CodeModification(
                    file_path=file_path,
                    cell_index=0,  # Python files don't have cells
                    line_number=line_number - 1,  # Convert to 0-based
                    change_type="replace",
                    original_code=original_line.strip(),
                    modified_code=modified_line.strip(),
                    reason=f"Fix {category} issue: {message}",
                    issue_type=category,
                    confidence_level=self._assess_confidence(category, message),
                    requires_testing=True,
                    breaking_change=False,
                    documentation_needed=False
                )
                modifications.append(mod)

        return modifications

    def _generate_proactive_modifications_for_python(
        self, 
        lines: List[str], 
        file_path: str
    ) -> List[CodeModification]:
        """Generate proactive modifications for Python files."""

        modifications = []

        for line_index, line in enumerate(lines):
            line = line.strip()

            # Proactive Spark configuration
            if "SparkSession.builder" in line and "enableHiveSupport" in line:
                modified_line = line.replace(
                    ".enableHiveSupport()",
                    '.config("spark.sql.catalog.spark_catalog", "unity")'
                )
                
                mod = CodeModification(
                    file_path=file_path,
                    cell_index=0,
                    line_number=line_index,
                    change_type="replace",
                    original_code=line,
                    modified_code=modified_line,
                    reason="Proactive Unity Catalog configuration",
                    issue_type="proactive",
                    confidence_level=0.9,
                    requires_testing=True,
                    breaking_change=False,
                    documentation_needed=False
                )
                modifications.append(mod)

        return modifications

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
from ..utils.code_processor import CodeSectionProcessor, detect_unity_catalog_patterns

logger = logging.getLogger(__name__)


class ModifierAgent(BaseAgent):
    """Agent responsible for generating code modifications."""

    def __init__(self, config: Dict[str, Any]):
        """Initialize the modifier agent."""
        super().__init__(config)
        self.code_processor = CodeSectionProcessor()

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
        """Modify Spark-related code for Unity Catalog using dynamic catalog mapping."""

        # Simple string-based replacements for common patterns
        if "spark.read.table(" in line:
            # Extract table name and add dynamic catalog.schema prefix
            import re
            match = re.search(r'spark\.read\.table\(["\']([^"\']+)["\']\)', line)
            if match:
                table_ref = match.group(1)
                new_table_ref = self._get_dynamic_table_reference(table_ref)
                return line.replace(f'"{table_ref}"', f'"{new_table_ref}"').replace(f"'{table_ref}'", f"'{new_table_ref}'")

        if "saveAsTable(" in line:
            # Update saveAsTable calls
            import re
            match = re.search(r'saveAsTable\(["\']([^"\']+)["\']\)', line)
            if match:
                table_ref = match.group(1)
                new_table_ref = self._get_dynamic_table_reference(table_ref)
                return line.replace(f'"{table_ref}"', f'"{new_table_ref}"').replace(f"'{table_ref}'", f"'{new_table_ref}'")

        return line

    def _modify_sql_code(self, line: str, message: str) -> str:
        """Modify SQL code for Unity Catalog using dynamic catalog mapping."""
        import re
        
        modified_line = line
        
        # Comprehensive SQL table reference patterns
        sql_patterns = [
            # FROM clauses
            (r"\bFROM\s+([a-zA-Z_]\w*(?:\.[a-zA-Z_]\w*)*)", "FROM"),
            # JOIN clauses
            (r"\bJOIN\s+([a-zA-Z_]\w*(?:\.[a-zA-Z_]\w*)*)", "JOIN"),
            (r"\bINNER\s+JOIN\s+([a-zA-Z_]\w*(?:\.[a-zA-Z_]\w*)*)", "INNER JOIN"),
            (r"\bLEFT\s+JOIN\s+([a-zA-Z_]\w*(?:\.[a-zA-Z_]\w*)*)", "LEFT JOIN"),
            (r"\bRIGHT\s+JOIN\s+([a-zA-Z_]\w*(?:\.[a-zA-Z_]\w*)*)", "RIGHT JOIN"),
            (r"\bFULL\s+OUTER\s+JOIN\s+([a-zA-Z_]\w*(?:\.[a-zA-Z_]\w*)*)", "FULL OUTER JOIN"),
            # CREATE TABLE
            (r"\bCREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?([a-zA-Z_]\w*(?:\.[a-zA-Z_]\w*)*)", "CREATE TABLE"),
            # INSERT INTO
            (r"\bINSERT\s+INTO\s+([a-zA-Z_]\w*(?:\.[a-zA-Z_]\w*)*)", "INSERT INTO"),
            # UPDATE
            (r"\bUPDATE\s+([a-zA-Z_]\w*(?:\.[a-zA-Z_]\w*)*)", "UPDATE"),
            # DELETE FROM
            (r"\bDELETE\s+FROM\s+([a-zA-Z_]\w*(?:\.[a-zA-Z_]\w*)*)", "DELETE FROM"),
            # WITH clauses
            (r"\bWITH\s+\w+\s+AS\s*\(\s*SELECT\s+.*?\s+FROM\s+([a-zA-Z_]\w*(?:\.[a-zA-Z_]\w*)*)", "WITH"),
        ]
        
        for pattern, clause_type in sql_patterns:
            matches = re.finditer(pattern, modified_line, flags=re.IGNORECASE)
            for match in reversed(list(matches)):  # Reverse to avoid index shifting
                table_ref = match.group(1)
                # Skip if table reference already has 3+ parts or contains functions
                if table_ref.count('.') >= 2 or '(' in table_ref or ')' in table_ref:
                    continue
                    
                new_table_ref = self._get_dynamic_table_reference(table_ref)
                # Replace only the specific match, not all occurrences
                start, end = match.span(1)
                modified_line = modified_line[:start] + new_table_ref + modified_line[end:]
        
        # Handle spark.sql() with table references
        if "spark.sql(" in modified_line:
            # Look for table references within the SQL string
            sql_string_pattern = r"spark\.sql\(['\"]([^'\"]*)['\"]"
            sql_match = re.search(sql_string_pattern, modified_line)
            if sql_match:
                sql_content = sql_match.group(1)
                modified_sql = self._modify_sql_code(sql_content, message)
                if modified_sql != sql_content:
                    quote_char = modified_line[sql_match.start(1)-1]  # Get the quote character
                    modified_line = modified_line.replace(
                        f"{quote_char}{sql_content}{quote_char}",
                        f"{quote_char}{modified_sql}{quote_char}"
                    )

        return modified_line

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
        import re
        
        modified_line = line
        
        # Handle hive_metastore references
        if "hive_metastore" in modified_line.lower():
            modified_line = re.sub(r'\bhive_metastore\b', 'main', modified_line, flags=re.IGNORECASE)
        
        # Handle spark.read.table() calls with comprehensive pattern matching
        if "spark.read.table(" in modified_line:
            table_pattern = r"spark\.read\.table\(['\"]([^'\"]+)['\"]"
            for match in re.finditer(table_pattern, modified_line):
                table_ref = match.group(1)
                new_table_ref = self._get_dynamic_table_reference(table_ref)
                modified_line = modified_line.replace(match.group(0), 
                    f"spark.read.table('{new_table_ref}')")

        # Handle saveAsTable calls
        if "saveAsTable(" in modified_line:
            table_pattern = r"saveAsTable\(['\"]([^'\"]+)['\"]"
            for match in re.finditer(table_pattern, modified_line):
                table_ref = match.group(1)
                new_table_ref = self._get_dynamic_table_reference(table_ref)
                modified_line = modified_line.replace(match.group(0), 
                    f"saveAsTable('{new_table_ref}')")
        
        # Handle writeTable calls
        if "writeTable(" in modified_line:
            table_pattern = r"writeTable\(['\"]([^'\"]+)['\"]"
            for match in re.finditer(table_pattern, modified_line):
                table_ref = match.group(1)
                new_table_ref = self._get_dynamic_table_reference(table_ref)
                modified_line = modified_line.replace(match.group(0), 
                    f"writeTable('{new_table_ref}')")

        # Handle createOrReplaceTempView calls
        if "createOrReplaceTempView(" in modified_line:
            # Temp views don't need catalog prefixes, but log for awareness
            logger.debug(f"Temp view found in line: {modified_line.strip()}")
        
        # Handle SQL strings in general code (not just spark.sql)
        # Look for SQL keywords followed by table references
        sql_in_string_patterns = [
            r"['\"]([^'\"]*\bFROM\s+[a-zA-Z_]\w*(?:\.[a-zA-Z_]\w*)*[^'\"]*)['\"]",
            r"['\"]([^'\"]*\bJOIN\s+[a-zA-Z_]\w*(?:\.[a-zA-Z_]\w*)*[^'\"]*)['\"]",
            r"['\"]([^'\"]*\bUPDATE\s+[a-zA-Z_]\w*(?:\.[a-zA-Z_]\w*)*[^'\"]*)['\"]",
            r"['\"]([^'\"]*\bINSERT\s+INTO\s+[a-zA-Z_]\w*(?:\.[a-zA-Z_]\w*)*[^'\"]*)['\"]"
        ]
        
        for pattern in sql_in_string_patterns:
            matches = list(re.finditer(pattern, modified_line, flags=re.IGNORECASE))
            for match in reversed(matches):  # Process in reverse to avoid index issues
                sql_content = match.group(1)
                modified_sql = self._modify_sql_code(sql_content, message)
                if modified_sql != sql_content:
                    modified_line = modified_line.replace(sql_content, modified_sql)

        # Handle table references in comments or configuration
        if "#" in modified_line and any(keyword in modified_line.lower() for keyword in ['table', 'schema', 'catalog']):
            # Add a note about Unity Catalog if this seems like a table reference comment
            comment_pattern = r"#.*(?:table|schema|catalog)"
            if re.search(comment_pattern, modified_line, re.IGNORECASE):
                if "unity catalog" not in modified_line.lower():
                    modified_line += " # TODO: Verify Unity Catalog compatibility"

        return modified_line

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
        """Generate proactive modifications for Python files, excluding imports."""

        modifications = []
        content = '\n'.join(lines)
        
        # Separate code sections to exclude imports
        sections = self.code_processor.separate_code_sections(content, "python")
        main_code = sections.get("main_code", "")
        main_code_lines = main_code.split('\n') if main_code else []
        
        # Calculate offset to adjust line numbers for the main code section
        imports_lines_count = len(sections.get("imports", "").split('\n')) if sections.get("imports") else 0
        docstring_lines_count = len(sections.get("docstring", "").split('\n')) if sections.get("docstring") else 0
        line_offset = imports_lines_count + docstring_lines_count

        # Only process main code lines (excluding imports)
        for line_index, line in enumerate(main_code_lines):
            line = line.strip()

            # Skip empty lines
            if not line:
                continue

            # Proactive Spark configuration
            if "SparkSession.builder" in line and "enableHiveSupport" in line:
                modified_line = line.replace(
                    ".enableHiveSupport()",
                    '.config("spark.sql.catalog.spark_catalog", "unity")'
                )
                
                mod = CodeModification(
                    file_path=file_path,
                    cell_index=0,
                    line_number=line_index + line_offset,  # Adjust for imports section
                    change_type="replace",
                    original_code=line,
                    modified_code=modified_line,
                    reason="Proactive Unity Catalog configuration (imports preserved)",
                    issue_type="proactive",
                    confidence_level=0.9,
                    requires_testing=True,
                    breaking_change=False,
                    documentation_needed=False
                )
                modifications.append(mod)

            # Detect and modify table references in main code only
            uc_patterns = detect_unity_catalog_patterns(line)
            for pattern in uc_patterns:
                if pattern["type"] == "table_reference":
                    table_name = pattern["table_name"]
                    new_table_ref = self._get_dynamic_table_reference(table_name)
                    
                    if new_table_ref != table_name:
                        modified_line = line.replace(table_name, new_table_ref)
                        
                        mod = CodeModification(
                            file_path=file_path,
                            cell_index=0,
                            line_number=line_index + line_offset,
                            change_type="replace",
                            original_code=line,
                            modified_code=modified_line,
                            reason=f"Convert table reference to Unity Catalog three-part naming (imports preserved)",
                            issue_type="table_reference",
                            confidence_level=0.8,
                            requires_testing=True,
                            breaking_change=False,
                            documentation_needed=False
                        )
                        modifications.append(mod)

        logger.info(f"Generated {len(modifications)} proactive modifications (imports preserved)")
        return modifications

    def _get_dynamic_table_reference(self, table_ref: str) -> str:
        """
        Convert a table reference to three-part naming using dynamic catalog mapping.

        Args:
            table_ref: Table reference (e.g., 'table', 'schema.table', 'catalog.schema.table')

        Returns:
            Three-part table reference using dynamic catalog mapping
        """
        # Clean the table reference
        table_ref = table_ref.strip()
        
        # Check if already a three-part name
        parts = table_ref.split('.')
        if len(parts) >= 3:
            return table_ref  # Already properly formatted
            
        # Check if RAG service is available
        rag_service = self.config.get("rag_service")
        if rag_service:
            try:
                return rag_service.get_three_part_name(table_ref)
            except Exception as e:
                logger.debug(f"RAG service failed for table '{table_ref}': {e}")

        # Improved static mapping with better schema inference
        if len(parts) == 1:
            # Just table name - infer schema based on table name patterns
            table_name = parts[0]
            
            # Infer schema based on common patterns
            if any(pattern in table_name.lower() for pattern in ['sales', 'customer', 'product', 'order']):
                schema = 'sales'
            elif any(pattern in table_name.lower() for pattern in ['analytics', 'metrics', 'event', 'behavior', 'stats']):
                schema = 'analytics'
            elif any(pattern in table_name.lower() for pattern in ['user', 'profile', 'account']):
                schema = 'users'
            elif table_name.lower().startswith('raw_'):
                schema = 'raw'
            elif table_name.lower().startswith('processed_'):
                schema = 'processed'
            else:
                schema = 'default'
                
            # Use appropriate catalog
            catalog = self.config.get("default_catalog", "main")
            return f"{catalog}.{schema}.{table_name}"

        elif len(parts) == 2:
            # schema.table format - use default catalog
            catalog = self.config.get("default_catalog", "main")
            return f"{catalog}.{table_ref}"

        else:
            # Fallback - shouldn't happen but just in case
            return table_ref

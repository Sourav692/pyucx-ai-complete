"""
Validator Agent for the PyUCX-AI Multi-Agent Framework.

This agent validates code modifications and ensures they maintain
functionality while achieving Unity Catalog compatibility.
"""

import json
import logging
import re
from typing import Dict, List, Any, Optional
from langchain_core.messages import HumanMessage, SystemMessage

from .base_agent import BaseAgent
from ..core.agent_state import AgentState, ValidationResult

logger = logging.getLogger(__name__)


class ValidatorAgent(BaseAgent):
    """Agent responsible for validating code modifications."""

    def execute(self, state: AgentState) -> AgentState:
        """Validate the generated code modifications."""

        try:
            self._increment_iteration(state)
            self._add_message(state, "Validating code modifications")

            # Get current notebook and modifications
            current_notebook = state.get("current_notebook")
            current_modifications = self._get_current_modifications(state)

            if not current_notebook:
                return self._handle_error(state, "No current notebook for validation")

            # Perform validation
            validation_results = self._validate_modifications(
                current_notebook, 
                current_modifications
            )

            # Add validation results to state
            for result in validation_results:
                state["validation_results"].append(result.to_dict())

            # Summary of validation
            total_validations = len(validation_results)
            valid_count = len([r for r in validation_results if r.is_valid])

            self._add_message(
                state,
                f"Validation completed for {current_notebook['filename']}: "
                f"{valid_count}/{total_validations} modifications validated successfully"
            )

            return state

        except Exception as e:
            return self._handle_error(state, f"Validation failed: {str(e)}")

    def _get_current_modifications(self, state: AgentState) -> List[Dict[str, Any]]:
        """Get modifications for the current notebook."""

        current_notebook = state.get("current_notebook")
        if not current_notebook:
            return []

        notebook_path = current_notebook["path"]
        all_modifications = state.get("code_modifications", [])

        # Filter modifications for current notebook
        current_mods = [
            mod for mod in all_modifications 
            if mod.get("file_path") == notebook_path
        ]

        return current_mods

    def _validate_modifications(
        self, 
        notebook: Dict[str, Any], 
        modifications: List[Dict[str, Any]]
    ) -> List[ValidationResult]:
        """Validate all modifications for the notebook."""

        validation_results = []

        # Syntax validation
        syntax_result = self._validate_syntax(notebook, modifications)
        validation_results.append(syntax_result)

        # Compatibility validation
        compatibility_result = self._validate_compatibility(notebook, modifications)
        validation_results.append(compatibility_result)

        # Logic validation
        logic_result = self._validate_logic(notebook, modifications)
        validation_results.append(logic_result)

        return validation_results

    def _validate_syntax(
        self, 
        notebook: Dict[str, Any], 
        modifications: List[Dict[str, Any]]
    ) -> ValidationResult:
        """Validate syntax correctness of modifications."""

        result = ValidationResult(
            file_path=notebook["path"],
            validation_type="syntax"
        )

        syntax_errors = []

        for mod in modifications:
            modified_code = mod.get("modified_code", "")

            # Basic syntax checks
            if modified_code:
                syntax_issues = self._check_code_syntax(modified_code)
                syntax_errors.extend(syntax_issues)

        if syntax_errors:
            result.is_valid = False
            result.issues_found = syntax_errors
            result.syntax_errors = len(syntax_errors)
        else:
            result.is_valid = True
            result.syntax_errors = 0

        # Add recommendations
        if syntax_errors:
            result.recommendations = [
                "Review modified code for syntax errors",
                "Test code execution in development environment",
                "Consider using IDE with syntax highlighting"
            ]
        else:
            result.recommendations = [
                "Syntax validation passed",
                "Proceed with compatibility testing"
            ]

        return result

    def _check_code_syntax(self, code: str) -> List[str]:
        """Check basic Python syntax issues."""

        issues = []

        # Basic syntax checks
        try:
            # Try to compile the code
            compile(code, '<string>', 'exec')
        except SyntaxError as e:
            issues.append(f"Syntax error: {str(e)}")
        except Exception as e:
            issues.append(f"Code issue: {str(e)}")

        # Check for common patterns that might cause issues
        if code.count('(') != code.count(')'):
            issues.append("Mismatched parentheses")

        if code.count('[') != code.count(']'):
            issues.append("Mismatched brackets")

        if code.count('{') != code.count('}'):
            issues.append("Mismatched braces")

        # Check for incomplete strings
        single_quotes = code.count("'") - code.count("\'")
        double_quotes = code.count('"') - code.count('\"')

        if single_quotes % 2 != 0:
            issues.append("Incomplete single-quoted string")

        if double_quotes % 2 != 0:
            issues.append("Incomplete double-quoted string")

        return issues

    def _validate_compatibility(
        self, 
        notebook: Dict[str, Any], 
        modifications: List[Dict[str, Any]]
    ) -> ValidationResult:
        """Validate Unity Catalog compatibility of modifications."""

        result = ValidationResult(
            file_path=notebook["path"],
            validation_type="compatibility"
        )

        compatibility_issues = []
        warnings = []

        for mod in modifications:
            modified_code = mod.get("modified_code", "")

            # Check Unity Catalog compatibility
            uc_issues = self._check_unity_catalog_compatibility(modified_code)
            compatibility_issues.extend(uc_issues)

            # Check for potential warnings
            uc_warnings = self._check_unity_catalog_warnings(modified_code)
            warnings.extend(uc_warnings)

        result.compatibility_issues = len(compatibility_issues)
        result.issues_found = compatibility_issues
        result.warnings = warnings

        if compatibility_issues:
            result.is_valid = False
            result.recommendations = [
                "Review Unity Catalog naming conventions",
                "Ensure all table references use three-part names",
                "Validate data access permissions"
            ]
        else:
            result.is_valid = True
            result.recommendations = [
                "Unity Catalog compatibility validated",
                "Test with actual Unity Catalog environment"
            ]

        return result

    def _check_unity_catalog_compatibility(self, code: str) -> List[str]:
        """Check for Unity Catalog compatibility issues."""

        issues = []

        # Check for old-style table references
        if "spark.sql(" in code and "FROM " in code.upper():
            # Look for table names that don't have catalog.schema prefix
            from_matches = re.findall(r'FROM\s+(\w+)', code, re.IGNORECASE)
            for match in from_matches:
                if '.' not in match and match not in ['dual', 'values']:
                    issues.append(f"Table '{match}' should use three-part naming")

        # Check for direct table reads without catalog prefix
        if "spark.read.table(" in code:
            table_matches = re.findall(r'spark\.read\.table\(["\'](\w+)["\']\)', code)
            for match in table_matches:
                if '.' not in match:
                    issues.append(f"Table '{match}' should use catalog.schema.table format")

        # Check for deprecated Hive references
        if "hive_metastore" in code.lower():
            issues.append("Direct hive_metastore references should be updated")

        return issues

    def _check_unity_catalog_warnings(self, code: str) -> List[str]:
        """Check for potential Unity Catalog warnings."""

        warnings = []

        # Check for file system operations
        if "dbutils.fs" in code:
            warnings.append("File system operations may need path updates for Unity Catalog")

        # Check for legacy Spark configurations
        if "enableHiveSupport" in code:
            warnings.append("Consider updating Spark configuration for Unity Catalog")

        # Check for hardcoded database names
        if "USE " in code.upper():
            warnings.append("USE database statements may need catalog prefix")

        return warnings

    def _validate_logic(
        self, 
        notebook: Dict[str, Any], 
        modifications: List[Dict[str, Any]]
    ) -> ValidationResult:
        """Validate logical correctness of modifications."""

        result = ValidationResult(
            file_path=notebook["path"],
            validation_type="logic"
        )

        logic_issues = []

        # Check for logical consistency
        for mod in modifications:
            original_code = mod.get("original_code", "")
            modified_code = mod.get("modified_code", "")

            # Check if modification maintains original intent
            logic_checks = self._check_logic_preservation(original_code, modified_code)
            logic_issues.extend(logic_checks)

        if logic_issues:
            result.is_valid = False
            result.issues_found = logic_issues
            result.recommendations = [
                "Review modifications for logical consistency",
                "Ensure business logic is preserved",
                "Test with representative data"
            ]
        else:
            result.is_valid = True
            result.recommendations = [
                "Logic validation passed",
                "Modifications preserve original intent",
                "Ready for integration testing"
            ]

        return result

    def _check_logic_preservation(self, original: str, modified: str) -> List[str]:
        """Check if modification preserves original logic."""

        issues = []

        # Check if table operations are preserved
        if "read.table" in original and "read.table" in modified:
            # Extract table names to ensure they're consistently updated
            orig_tables = re.findall(r'read\.table\(["\'](\w+)["\']\)', original)
            mod_tables = re.findall(r'read\.table\(["\'](\w+\.\w+\.\w+)["\']\)', modified)

            if len(orig_tables) != len(mod_tables):
                issues.append("Table reference count mismatch between original and modified code")

        # Check if SQL query structure is preserved
        if "SELECT" in original.upper() and "SELECT" in modified.upper():
            # Ensure SELECT, FROM, WHERE clauses are consistently updated
            orig_keywords = len(re.findall(r'\b(SELECT|FROM|WHERE|GROUP|ORDER)\b', original, re.IGNORECASE))
            mod_keywords = len(re.findall(r'\b(SELECT|FROM|WHERE|GROUP|ORDER)\b', modified, re.IGNORECASE))

            if orig_keywords != mod_keywords:
                issues.append("SQL query structure may have been altered")

        return issues

    def _generate_validation_summary(
        self, 
        validation_results: List[ValidationResult]
    ) -> Dict[str, Any]:
        """Generate a summary of validation results."""

        summary = {
            "total_validations": len(validation_results),
            "passed_validations": len([r for r in validation_results if r.is_valid]),
            "failed_validations": len([r for r in validation_results if not r.is_valid]),
            "total_issues": sum(len(r.issues_found) for r in validation_results),
            "total_warnings": sum(len(r.warnings) for r in validation_results),
            "validation_types": [r.validation_type for r in validation_results],
            "overall_status": "passed" if all(r.is_valid for r in validation_results) else "failed"
        }

        return summary

    def _generate_validation_report(
        self, 
        notebook: Dict[str, Any], 
        validation_results: List[ValidationResult]
    ) -> str:
        """Generate a detailed validation report using LLM."""

        try:
            summary = self._generate_validation_summary(validation_results)

            # Prepare validation details
            details = []
            for result in validation_results:
                details.append(f"\n{result.validation_type.upper()} Validation:")
                details.append(f"  Status: {'PASSED' if result.is_valid else 'FAILED'}")
                details.append(f"  Issues: {len(result.issues_found)}")
                details.append(f"  Warnings: {len(result.warnings)}")

                if result.issues_found:
                    details.append("  Issues Found:")
                    for issue in result.issues_found[:3]:  # Show top 3
                        details.append(f"    - {issue}")

            validation_context = "\n".join(details)

            prompt = f"""
            Generate a validation report for Unity Catalog migration:

            Notebook: {notebook.get('filename', 'Unknown')}

            Validation Summary:
            - Total Validations: {summary['total_validations']}
            - Passed: {summary['passed_validations']}
            - Failed: {summary['failed_validations']}
            - Total Issues: {summary['total_issues']}
            - Overall Status: {summary['overall_status']}

            Validation Details:
            {validation_context}

            Provide a clear assessment and recommendations for next steps.
            """

            messages = [
                SystemMessage(content="You are a Unity Catalog migration expert. Provide clear validation assessments."),
                HumanMessage(content=prompt)
            ]

            response = self.llm.invoke(messages)
            return response.content

        except Exception as e:
            logger.error(f"Failed to generate validation report: {e}")
            return f"Validation completed with {summary['overall_status']} status. {summary['total_issues']} issues found."

"""
Code Generation Agent for the PyUCX-AI Multi-Agent Framework.

This agent applies code modifications to create converted Unity Catalog notebooks
and saves them to the output directory.
"""

import json
import logging
import os
from typing import Dict, List, Any, Optional
from pathlib import Path
from langchain_core.messages import HumanMessage, SystemMessage

from .base_agent import BaseAgent
from ..core.agent_state import AgentState, CodeModification
from ..utils.code_processor import CodeSectionProcessor

logger = logging.getLogger(__name__)


class CodeGenerationAgent(BaseAgent):
    """Agent responsible for generating converted Unity Catalog notebooks."""

    def __init__(self, config: Dict[str, Any]):
        """Initialize the code generation agent."""
        super().__init__(config)
        self.code_processor = CodeSectionProcessor()

    def execute(self, state: AgentState) -> AgentState:
        """Apply modifications and generate converted notebooks."""

        try:
            self._increment_iteration(state)
            self._add_message(state, "Generating converted Unity Catalog notebooks")

            # Get current notebook and modifications
            current_notebook = state.get("current_notebook")
            if not current_notebook:
                return self._handle_error(state, "No current notebook for code generation")

            # Get modifications for this notebook
            notebook_modifications = self._get_modifications_for_notebook(state, current_notebook)
            
            if not notebook_modifications:
                self._add_message(state, f"No modifications found for {current_notebook['filename']}")
                return state

            # Apply modifications to create converted notebook
            converted_notebook = self._apply_modifications(current_notebook, notebook_modifications)
            
            # Save converted notebook
            output_path = self._save_converted_notebook(converted_notebook, current_notebook)
            
            # Update state with converted notebook info
            converted_info = {
                "original_path": current_notebook["path"],
                "converted_path": output_path,
                "filename": current_notebook["filename"],
                "modifications_applied": len(notebook_modifications),
                "timestamp": self._get_timestamp()
            }
            
            state["converted_notebooks"] = state.get("converted_notebooks", [])
            state["converted_notebooks"].append(converted_info)

            self._add_message(
                state,
                f"Generated converted notebook: {current_notebook['filename']} "
                f"({len(notebook_modifications)} modifications applied)"
            )

            return state

        except Exception as e:
            return self._handle_error(state, f"Code generation failed: {str(e)}")

    def _get_modifications_for_notebook(
        self, 
        state: AgentState, 
        notebook: Dict[str, Any]
    ) -> List[CodeModification]:
        """Get all modifications for the current notebook."""

        notebook_path = notebook["path"]
        modifications = []
        
        for mod_dict in state.get("code_modifications", []):
            if mod_dict.get("file_path") == notebook_path:
                # Convert dict back to CodeModification object
                mod = CodeModification(
                    file_path=mod_dict.get("file_path", ""),
                    cell_index=mod_dict.get("cell_index", -1),
                    line_number=mod_dict.get("line_number", 0),
                    change_type=mod_dict.get("change_type", "replace"),
                    original_code=mod_dict.get("original_code", ""),
                    modified_code=mod_dict.get("modified_code", ""),
                    reason=mod_dict.get("reason", ""),
                    issue_type=mod_dict.get("issue_type", ""),
                    confidence_level=mod_dict.get("confidence_level", 0.8),
                    requires_testing=mod_dict.get("requires_testing", True),
                    breaking_change=mod_dict.get("breaking_change", False),
                    documentation_needed=mod_dict.get("documentation_needed", False)
                )
                modifications.append(mod)

        return modifications

    def _apply_modifications(
        self, 
        notebook: Dict[str, Any], 
        modifications: List[CodeModification]
    ) -> Dict[str, Any]:
        """Apply modifications to create converted notebook."""

        # Check if this is a Python file or notebook
        file_type = notebook.get("file_type", "notebook")
        
        if file_type == "python":
            return self._apply_python_modifications(notebook, modifications)
        else:
            return self._apply_notebook_modifications(notebook, modifications)

    def _apply_notebook_modifications(
        self, 
        notebook: Dict[str, Any], 
        modifications: List[CodeModification]
    ) -> Dict[str, Any]:
        """Apply modifications to create converted notebook."""

        # Create a deep copy of the notebook
        converted_notebook = json.loads(json.dumps(notebook))
        
        # Get cells
        cells = converted_notebook.get("cells", [])
        
        # Group modifications by cell index for efficient processing
        modifications_by_cell = {}
        for mod in modifications:
            cell_idx = mod.cell_index
            if cell_idx not in modifications_by_cell:
                modifications_by_cell[cell_idx] = []
            modifications_by_cell[cell_idx].append(mod)

        # Apply modifications to each cell
        for cell_idx, cell_mods in modifications_by_cell.items():
            if 0 <= cell_idx < len(cells):
                cell = cells[cell_idx]
                if cell.get("cell_type") == "code":
                    self._apply_cell_modifications(cell, cell_mods)

        # Add Unity Catalog metadata
        self._add_unity_catalog_metadata(converted_notebook)
        
        return converted_notebook

    def _apply_python_modifications(
        self, 
        python_file: Dict[str, Any], 
        modifications: List[CodeModification]
    ) -> Dict[str, Any]:
        """Apply modifications to create converted Python file, preserving imports."""

        # Create a deep copy of the Python file
        converted_file = json.loads(json.dumps(python_file))
        
        # Get original content
        original_content = converted_file.get("content", "")
        
        # Separate code sections using the code processor
        sections = self.code_processor.separate_code_sections(original_content, "python")
        
        # Only apply modifications to the main code section (preserve imports)
        main_code_lines = sections["main_code"].split('\n') if sections["main_code"] else []
        
        # Apply modifications only to main code lines
        for mod in modifications:
            # Skip modifications that would affect import sections
            if self._modification_affects_imports(mod, sections["imports"]):
                logger.info(f"Skipped modification to import section: {mod.original_code[:50]}...")
                continue
                
            # Find the line that matches the original code in main code section
            for i, line in enumerate(main_code_lines):
                if mod.original_code.strip() in line.strip():
                    if mod.change_type == "replace":
                        main_code_lines[i] = mod.modified_code
                    elif mod.change_type == "insert":
                        main_code_lines.insert(i, mod.modified_code)
                    elif mod.change_type == "delete":
                        main_code_lines.pop(i)
                    elif mod.change_type == "modify":
                        main_code_lines[i] = mod.modified_code
                    break

        # Update the main code section
        sections["main_code"] = '\n'.join(main_code_lines)
        
        # Reconstruct the complete file with preserved imports
        reconstructed_content = self.code_processor.reconstruct_file(sections, "python")
        
        # Update the converted file
        converted_file["lines"] = reconstructed_content.split('\n')
        converted_file["content"] = reconstructed_content
        converted_file["sections"] = sections  # Store section info for debugging

        # Add Unity Catalog metadata (this will be added to imports section)
        self._add_unity_catalog_metadata_to_python(converted_file, sections)
        
        return converted_file

    def _apply_cell_modifications(
        self, 
        cell: Dict[str, Any], 
        modifications: List[CodeModification]
    ) -> None:
        """Apply modifications to a specific cell."""

        source = cell.get("source", [])
        
        # Convert source to list if it's a string
        if isinstance(source, str):
            source_lines = source.split("\n")
        else:
            source_lines = source

        # Apply each modification by finding the matching line content
        for mod in modifications:
            # Find the line that matches the original code
            for i, line in enumerate(source_lines):
                if mod.original_code.strip() in line.strip():
                    if mod.change_type == "replace":
                        source_lines[i] = mod.modified_code
                    elif mod.change_type == "insert":
                        source_lines.insert(i, mod.modified_code)
                    elif mod.change_type == "delete":
                        source_lines.pop(i)
                    elif mod.change_type == "modify":
                        source_lines[i] = mod.modified_code
                    break

        # Update cell source
        cell["source"] = source_lines

    def _modification_affects_imports(self, modification: CodeModification, imports_section: str) -> bool:
        """Check if a modification would affect the imports section."""
        
        # If the original code appears in the imports section, skip it
        if modification.original_code.strip() in imports_section:
            return True
        
        # Check if the modification is specifically targeting import statements
        import_keywords = ['import ', 'from ', 'import\t', 'from\t']
        original_code_lower = modification.original_code.lower().strip()
        
        for keyword in import_keywords:
            if original_code_lower.startswith(keyword):
                return True
        
        return False

    def _add_unity_catalog_metadata(self, notebook: Dict[str, Any]) -> None:
        """Add Unity Catalog metadata to the notebook."""

        # Add metadata to notebook
        if "metadata" not in notebook:
            notebook["metadata"] = {}
        
        notebook["metadata"]["unity_catalog_converted"] = True
        notebook["metadata"]["conversion_timestamp"] = self._get_timestamp()
        notebook["metadata"]["conversion_tool"] = "PyUCX-AI Multi-Agent Framework"
        
        # Add a markdown cell at the beginning with conversion info
        conversion_cell = {
            "cell_type": "markdown",
            "metadata": {},
            "source": [
                "# Unity Catalog Converted Notebook\n",
                "\n",
                "This notebook has been automatically converted for Unity Catalog compatibility using the PyUCX-AI Multi-Agent Framework.\n",
                "\n",
                "## Changes Made\n",
                "- Updated table references to use three-part naming (catalog.schema.table)\n",
                "- Modified Spark configurations for Unity Catalog\n",
                "- Updated SQL queries for UC compatibility\n",
                "- Added Unity Catalog-specific configurations\n",
                "\n",
                "## Next Steps\n",
                "1. Review the converted code for accuracy\n",
                "2. Test the notebook in your Unity Catalog environment\n",
                "3. Update any hardcoded catalog/schema names as needed\n",
                "4. Verify data access permissions\n"
            ]
        }
        
        # Insert at the beginning
        notebook["cells"].insert(0, conversion_cell)

    def _add_unity_catalog_metadata_to_python(self, python_file: Dict[str, Any], sections: Dict[str, Any] = None) -> None:
        """Add Unity Catalog metadata to the Python file."""

        # Add header comment
        header_comment = [
            "# Unity Catalog Converted Python File",
            "# This file has been automatically converted for Unity Catalog compatibility using the PyUCX-AI Multi-Agent Framework.",
            "#",
            "# Changes Made:",
            "# - Updated table references to use three-part naming (catalog.schema.table)",
            "# - Modified Spark configurations for Unity Catalog", 
            "# - Updated SQL queries for UC compatibility",
            "# - Added Unity Catalog-specific configurations",
            "# - Preserved original import statements (imports are not modified during UC conversion)",
            "#",
            "# Next Steps:",
            "# 1. Review the converted code for accuracy",
            "# 2. Test the file in your Unity Catalog environment", 
            "# 3. Update any hardcoded catalog/schema names as needed",
            "# 4. Verify data access permissions",
            "#"
        ]
        
        if sections:
            # If we have sections, add header to the beginning and reconstruct
            header_content = '\n'.join(header_comment) + '\n'
            
            # Add header at the very beginning, before docstring and imports
            reconstructed_sections = {
                "imports": sections.get("imports", ""),
                "docstring": sections.get("docstring", ""),
                "main_code": sections.get("main_code", ""),
                "metadata": sections.get("metadata", {})
            }
            
            # Prepend header
            if reconstructed_sections["docstring"]:
                content = header_content + reconstructed_sections["docstring"] + '\n' + reconstructed_sections["imports"] + '\n' + reconstructed_sections["main_code"]
            else:
                content = header_content + reconstructed_sections["imports"] + '\n' + reconstructed_sections["main_code"]
            
            python_file["content"] = content
            python_file["lines"] = content.split('\n')
        else:
            # Fallback to original behavior
            lines = python_file.get("lines", [])
            python_file["lines"] = header_comment + lines
            python_file["content"] = "\n".join(python_file["lines"])

    def _save_converted_notebook(
        self, 
        converted_notebook: Dict[str, Any], 
        original_notebook: Dict[str, Any]
    ) -> str:
        """Save the converted notebook to the output directory."""

        file_type = converted_notebook.get("file_type", "notebook")
        
        if file_type == "python":
            return self._save_converted_python_file(converted_notebook, original_notebook)
        else:
            return self._save_converted_notebook_file(converted_notebook, original_notebook)

    def _save_converted_notebook_file(
        self, 
        converted_notebook: Dict[str, Any], 
        original_notebook: Dict[str, Any]
    ) -> str:
        """Save the converted notebook to the output directory."""

        # Create output directory
        output_dir = Path("output/converted_notebooks")
        output_dir.mkdir(parents=True, exist_ok=True)
        
        # Generate output filename
        original_filename = original_notebook["filename"]
        base_name = original_filename.replace(".ipynb", "")
        converted_filename = f"{base_name}_unity_catalog.ipynb"
        output_path = output_dir / converted_filename
        
        # Save the notebook
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(converted_notebook, f, indent=2, ensure_ascii=False)
        
        logger.info(f"Saved converted notebook: {output_path}")
        return str(output_path)

    def _save_converted_python_file(
        self, 
        converted_file: Dict[str, Any], 
        original_file: Dict[str, Any]
    ) -> str:
        """Save the converted Python file to the output directory."""

        # Create output directory
        output_dir = Path("output/converted_python_files")
        output_dir.mkdir(parents=True, exist_ok=True)
        
        # Generate output filename
        original_filename = original_file["filename"]
        base_name = original_filename.replace(".py", "")
        converted_filename = f"{base_name}_unity_catalog.py"
        output_path = output_dir / converted_filename
        
        # Save the Python file
        with open(output_path, 'w', encoding='utf-8') as f:
            f.write(converted_file["content"])
        
        logger.info(f"Saved converted Python file: {output_path}")
        return str(output_path)

    def _get_timestamp(self) -> str:
        """Get current timestamp in ISO format."""
        from datetime import datetime
        return datetime.now().isoformat()

    def _add_message(self, state: AgentState, message: str) -> None:
        """Add a message to the agent messages."""
        from datetime import datetime
        
        state["agent_messages"] = state.get("agent_messages", [])
        state["agent_messages"].append({
            "agent": "code_generation",
            "message": message,
            "type": "info",
            "timestamp": datetime.now().isoformat()
        })


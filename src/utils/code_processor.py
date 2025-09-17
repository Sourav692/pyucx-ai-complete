"""
Code Processing Utilities for PyUCX-AI Framework.

This module provides utilities for processing code files while preserving
import sections during Unity Catalog conversion.
"""

import re
import ast
from typing import List, Dict, Tuple, Any
import logging

logger = logging.getLogger(__name__)


class CodeSectionProcessor:
    """Processor for handling different sections of code files."""
    
    def __init__(self):
        self.import_patterns = [
            r'^import\s+\w+',
            r'^from\s+\w+.*import',
            r'^import\s+\w+.*as\s+\w+',
            r'^\s*import\s+\w+',
            r'^\s*from\s+\w+.*import'
        ]
    
    def separate_code_sections(self, content: str, file_type: str = "python") -> Dict[str, Any]:
        """
        Separate code into different sections: imports, main code, etc.
        
        Args:
            content: The complete file content
            file_type: Type of file ('python', 'notebook', etc.)
            
        Returns:
            Dictionary with separated sections
        """
        if file_type == "python":
            return self._separate_python_sections(content)
        elif file_type == "notebook":
            return self._separate_notebook_sections(content)
        else:
            # Default processing
            return {
                "imports": "",
                "main_code": content,
                "docstring": "",
                "metadata": {}
            }
    
    def _separate_python_sections(self, content: str) -> Dict[str, Any]:
        """Separate Python file into import section and main code."""
        lines = content.split('\n')
        
        # First, find the docstring boundaries if it exists
        docstring_start = -1
        docstring_end = -1
        
        for i, line in enumerate(lines):
            stripped = line.strip()
            if stripped.startswith('"""') or stripped.startswith("'''"):
                if docstring_start == -1:
                    docstring_start = i
                    # Check if it's a single-line docstring
                    quotes = stripped[:3]
                    if stripped.count(quotes) >= 2 and len(stripped) > 3:
                        docstring_end = i
                        break
                else:
                    docstring_end = i
                    break
        
        # Separate the content into sections
        header_lines = []
        docstring_lines = []
        remaining_lines = []
        
        if docstring_start >= 0:
            header_lines = lines[:docstring_start]
            if docstring_end >= 0:
                docstring_lines = lines[docstring_start:docstring_end + 1]
                remaining_lines = lines[docstring_end + 1:]
            else:
                # Docstring not properly closed, treat as regular code
                remaining_lines = lines[docstring_start:]
        else:
            remaining_lines = lines
        
        # Now separate remaining lines into imports and main code
        import_lines = []
        main_code_lines = []
        imports_ended = False
        
        # Add header (initial comments) to imports
        import_lines.extend(header_lines)
        
        for line in remaining_lines:
            stripped = line.strip()
            
            if not imports_ended:
                # Check if this line should be in imports section
                if (self._is_import_line(line) or 
                    stripped.startswith('#') or 
                    not stripped):
                    import_lines.append(line)
                else:
                    # First non-import line
                    imports_ended = True
                    main_code_lines.append(line)
            else:
                main_code_lines.append(line)
        
        return {
            "imports": '\n'.join(import_lines),
            "docstring": '\n'.join(docstring_lines),
            "main_code": '\n'.join(main_code_lines),
            "metadata": {
                "import_count": len([l for l in import_lines if self._is_import_line(l)]),
                "has_docstring": len(docstring_lines) > 0
            }
        }
    
    def _separate_notebook_sections(self, content: str) -> Dict[str, Any]:
        """Separate notebook cells into import cells and code cells."""
        try:
            import json
            notebook = json.loads(content)
        except json.JSONDecodeError:
            logger.warning("Could not parse notebook JSON, treating as plain text")
            return self._separate_python_sections(content)
        
        import_cells = []
        code_cells = []
        other_cells = []
        
        for cell in notebook.get('cells', []):
            if cell.get('cell_type') == 'code':
                source = ''.join(cell.get('source', []))
                if self._cell_contains_only_imports(source):
                    import_cells.append(cell)
                else:
                    code_cells.append(cell)
            else:
                other_cells.append(cell)
        
        return {
            "import_cells": import_cells,
            "code_cells": code_cells,
            "other_cells": other_cells,
            "metadata": {
                "total_cells": len(notebook.get('cells', [])),
                "import_cells_count": len(import_cells),
                "code_cells_count": len(code_cells)
            }
        }
    
    def _is_import_line(self, line: str) -> bool:
        """Check if a line is an import statement."""
        stripped = line.strip()
        
        # Empty lines or comments are not imports
        if not stripped or stripped.startswith('#'):
            return False
        
        # Check against import patterns
        for pattern in self.import_patterns:
            if re.match(pattern, stripped):
                return True
        
        return False
    
    def _cell_contains_only_imports(self, source: str) -> bool:
        """Check if a notebook cell contains only import statements."""
        lines = source.strip().split('\n')
        
        for line in lines:
            stripped = line.strip()
            # Skip empty lines and comments
            if not stripped or stripped.startswith('#'):
                continue
            # If we find a non-import line, this cell has more than imports
            if not self._is_import_line(line):
                return False
        
        return True
    
    def reconstruct_file(self, sections: Dict[str, Any], file_type: str = "python") -> str:
        """
        Reconstruct the complete file from separated sections.
        
        Args:
            sections: Dictionary with file sections
            file_type: Type of file to reconstruct
            
        Returns:
            Complete file content
        """
        if file_type == "python":
            return self._reconstruct_python_file(sections)
        elif file_type == "notebook":
            return self._reconstruct_notebook_file(sections)
        else:
            # Default reconstruction
            return sections.get("main_code", "")
    
    def _reconstruct_python_file(self, sections: Dict[str, Any]) -> str:
        """Reconstruct Python file from sections."""
        parts = []
        
        # Add docstring if present
        if sections.get("docstring"):
            parts.append(sections["docstring"])
        
        # Add imports (unchanged)
        if sections.get("imports"):
            parts.append(sections["imports"])
        
        # Add main code (potentially modified)
        if sections.get("main_code"):
            parts.append(sections["main_code"])
        
        return '\n'.join(parts)
    
    def _reconstruct_notebook_file(self, sections: Dict[str, Any]) -> str:
        """Reconstruct notebook file from sections."""
        try:
            import json
            
            # Reconstruct the notebook structure
            notebook = {
                "cells": [],
                "metadata": sections.get("original_metadata", {}),
                "nbformat": 4,
                "nbformat_minor": 2
            }
            
            # Add cells in order: other cells, import cells, then code cells
            notebook["cells"].extend(sections.get("other_cells", []))
            notebook["cells"].extend(sections.get("import_cells", []))
            notebook["cells"].extend(sections.get("code_cells", []))
            
            return json.dumps(notebook, indent=2)
        except Exception as e:
            logger.error(f"Error reconstructing notebook: {e}")
            return ""


def detect_unity_catalog_patterns(code: str) -> List[Dict[str, Any]]:
    """
    Detect patterns in code that need Unity Catalog conversion.
    
    Args:
        code: Code content to analyze
        
    Returns:
        List of patterns that need conversion
    """
    patterns = []
    
    # Table reference patterns (excluding imports)
    table_patterns = [
        r'\.table\s*\(\s*["\']([^"\']+)["\']\s*\)',  # .table("table_name")
        r'FROM\s+([a-zA-Z_][a-zA-Z0-9_]*)',  # SQL FROM clauses
        r'JOIN\s+([a-zA-Z_][a-zA-Z0-9_]*)',  # SQL JOIN clauses
        r'INSERT\s+INTO\s+([a-zA-Z_][a-zA-Z0-9_]*)',  # SQL INSERT statements
    ]
    
    for pattern in table_patterns:
        matches = re.finditer(pattern, code, re.IGNORECASE)
        for match in matches:
            patterns.append({
                "type": "table_reference",
                "pattern": match.group(0),
                "table_name": match.group(1),
                "start": match.start(),
                "end": match.end()
            })
    
    return patterns

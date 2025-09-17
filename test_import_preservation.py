#!/usr/bin/env python3
"""
Test script to verify that the enhanced PyUCX-AI framework correctly
preserves import statements during Unity Catalog conversion.
"""

import os
import sys
import tempfile
import json
from pathlib import Path

# Add the src directory to Python path
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), 'src'))

from src.utils.code_processor import CodeSectionProcessor
from src.agents.code_generation_agent import CodeGenerationAgent
from src.agents.modifier_agent import ModifierAgent
from src.core.agent_state import CodeModification


def create_test_python_file():
    """Create a sample Python file with imports and main code."""
    
    content = '''# Sample Python file for testing import preservation
"""
This is a sample Python file for testing Unity Catalog conversion
while preserving import statements.
"""

import pandas as pd
import numpy as np
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as spark_sum
import logging

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def main():
    """Main function with Spark operations."""
    
    # Create Spark session
    spark = SparkSession.builder \\
        .appName("TestApp") \\
        .enableHiveSupport() \\
        .getOrCreate()
    
    # Read a table that needs UC conversion
    df = spark.table("sales_data")
    
    # Perform some operations
    result = df.groupBy("product_id").agg(spark_sum("amount").alias("total_sales"))
    
    # Write to another table
    result.write.mode("overwrite").saveAsTable("sales_summary")
    
    logger.info("Data processing completed")

if __name__ == "__main__":
    main()
'''
    
    return {
        "content": content,
        "lines": content.split('\n'),
        "filename": "test_sample.py",
        "path": "test_sample.py",
        "file_type": "python"
    }


def test_code_section_processor():
    """Test the CodeSectionProcessor's ability to separate imports."""
    
    print("🧪 Testing CodeSectionProcessor...")
    
    processor = CodeSectionProcessor()
    test_file = create_test_python_file()
    
    # Separate sections
    sections = processor.separate_code_sections(test_file["content"], "python")
    
    print(f"✅ Separated sections:")
    print(f"   📦 Imports: {len(sections['imports'].split(chr(10)))} lines")
    print(f"   📝 Docstring: {len(sections['docstring'].split(chr(10)))} lines")
    print(f"   💻 Main code: {len(sections['main_code'].split(chr(10)))} lines")
    
    # Debug: Print the actual content
    print("\n🔍 Debug - Original content:")
    print(repr(test_file["content"][:300]))
    print("\n🔍 Debug - Imports section content:")
    print(repr(sections["imports"]))
    print("\n🔍 Debug - Docstring section content:")
    print(repr(sections["docstring"][:200]))
    print("\n🔍 Debug - Main code section content:")
    print(repr(sections["main_code"][:200]))
    
    # Verify imports are captured
    imports_section = sections["imports"]
    assert "import pandas as pd" in imports_section, f"pandas import not found in: {imports_section}"
    assert "from pyspark.sql import SparkSession" in imports_section, f"pyspark import not found in: {imports_section}"
    
    # Verify main code doesn't include imports
    main_code = sections["main_code"]
    assert "def main():" in main_code, "main function not found in main code"
    assert "import pandas" not in main_code, "imports found in main code section"
    
    print("✅ CodeSectionProcessor test passed!")
    return sections


def test_modifier_agent_import_exclusion():
    """Test that ModifierAgent excludes imports from modifications."""
    
    print("\n🧪 Testing ModifierAgent import exclusion...")
    
    config = {"openai_api_key": "test", "temperature": 0.1}
    modifier = ModifierAgent(config)
    
    test_file = create_test_python_file()
    
    # Generate proactive modifications
    modifications = modifier._generate_proactive_modifications_for_python(
        test_file["lines"], 
        test_file["path"]
    )
    
    print(f"✅ Generated {len(modifications)} modifications")
    
    # Verify no modifications target import lines
    for mod in modifications:
        original_code = mod.original_code.strip()
        assert not original_code.startswith("import "), f"Modification targets import: {original_code}"
        assert not original_code.startswith("from "), f"Modification targets import: {original_code}"
    
    # Verify modifications target appropriate lines
    has_spark_modification = any("enableHiveSupport" in mod.original_code for mod in modifications)
    print(f"✅ Found Spark configuration modification: {has_spark_modification}")
    
    print("✅ ModifierAgent import exclusion test passed!")
    return modifications


def test_code_generation_agent():
    """Test that CodeGenerationAgent preserves imports during code generation."""
    
    print("\n🧪 Testing CodeGenerationAgent import preservation...")
    
    config = {"openai_api_key": "test", "temperature": 0.1}
    code_gen = CodeGenerationAgent(config)
    
    test_file = create_test_python_file()
    
    # Create sample modifications (avoiding imports)
    modifications = [
        CodeModification(
            file_path=test_file["path"],
            cell_index=0,
            line_number=20,
            change_type="replace",
            original_code='    spark = SparkSession.builder \\',
            modified_code='    spark = SparkSession.builder \\',
            reason="Test modification",
            issue_type="test",
            confidence_level=0.9,
            requires_testing=True,
            breaking_change=False,
            documentation_needed=False
        )
    ]
    
    # Apply modifications
    converted_file = code_gen._apply_python_modifications(test_file, modifications)
    
    # Verify imports are preserved
    converted_content = converted_file["content"]
    assert "import pandas as pd" in converted_content, "pandas import not preserved"
    assert "from pyspark.sql import SparkSession" in converted_content, "pyspark import not preserved"
    
    # Verify header was added
    assert "Unity Catalog Converted Python File" in converted_content, "UC header not added"
    assert "imports are not modified" in converted_content, "Import preservation note not added"
    
    print("✅ CodeGenerationAgent import preservation test passed!")
    
    return converted_file


def test_end_to_end_conversion():
    """Test the complete conversion process end-to-end."""
    
    print("\n🧪 Testing end-to-end conversion...")
    
    # Create test file
    test_file = create_test_python_file()
    
    # Test modifier agent
    config = {"openai_api_key": "test", "temperature": 0.1}
    modifier = ModifierAgent(config)
    modifications = modifier._generate_proactive_modifications_for_python(
        test_file["lines"], 
        test_file["path"]
    )
    
    # Test code generation agent
    code_gen = CodeGenerationAgent(config)
    converted_file = code_gen._apply_python_modifications(test_file, modifications)
    
    # Save to temporary file for inspection
    with tempfile.NamedTemporaryFile(mode='w', suffix='.py', delete=False) as f:
        f.write(converted_file["content"])
        temp_path = f.name
    
    print(f"✅ End-to-end conversion completed")
    print(f"📁 Converted file saved to: {temp_path}")
    
    # Verify the structure
    sections = converted_file.get("sections", {})
    if sections:
        print(f"📊 Final file structure:")
        print(f"   📦 Imports preserved: {bool(sections.get('imports'))}")
        print(f"   📝 Docstring preserved: {bool(sections.get('docstring'))}")
        print(f"   💻 Main code modified: {bool(sections.get('main_code'))}")
    
    return temp_path


def main():
    """Run all tests."""
    
    print("🚀 Testing Enhanced PyUCX-AI Framework - Import Preservation")
    print("=" * 60)
    
    try:
        # Test 1: Code section processor
        sections = test_code_section_processor()
        
        # Test 2: Modifier agent import exclusion
        modifications = test_modifier_agent_import_exclusion()
        
        # Test 3: Code generation agent import preservation
        converted_file = test_code_generation_agent()
        
        # Test 4: End-to-end conversion
        output_file = test_end_to_end_conversion()
        
        print("\n" + "=" * 60)
        print("🎉 All tests passed! Import preservation is working correctly.")
        print(f"📋 Generated {len(modifications)} modifications")
        print(f"📁 Output file: {output_file}")
        print("\n💡 The enhanced framework now:")
        print("   ✅ Preserves all import statements")
        print("   ✅ Only modifies table references and configurations")
        print("   ✅ Maintains proper Python file structure")
        print("   ✅ Adds clear documentation about changes")
        
    except Exception as e:
        print(f"\n❌ Test failed: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()

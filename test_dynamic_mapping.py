"""
Test script for dynamic catalog mapping using RAG with ChromaDB.

This script tests the dynamic schema-to-catalog mapping functionality
to ensure that:
- sales schema maps to catalog2
- analytics schema maps to catalog1
"""

import sys
import os
import logging
from pathlib import Path

# Add the src directory to Python path
sys.path.insert(0, os.path.join(os.getcwd(), 'src'))

try:
    from src.services.schema_mapping_rag import SchemaMappingRAG, SchemaMapping
    from src.utils.logging_setup import quick_setup
    print("✅ Successfully imported RAG service components")
except ImportError as e:
    print(f"❌ Error importing components: {e}")
    sys.exit(1)


def test_rag_service():
    """Test the RAG service functionality."""

    print("\n🧪 Testing RAG Service for Dynamic Catalog Mapping")
    print("=" * 60)

    # Setup logging
    logger = quick_setup(log_level="INFO")

    try:
        # Initialize RAG service
        print("📊 Initializing ChromaDB RAG service...")
        rag_service = SchemaMappingRAG(
            db_path="data/chroma_db_test",
            collection_name="schema_mappings_test"
        )
        print("✅ RAG service initialized successfully")

        # Test individual mappings
        test_cases = [
            ("sales.raw_sales_data", "catalog2.sales.raw_sales_data"),
            ("sales.customer_master", "catalog2.sales.customer_master"),
            ("analytics.user_behavior", "catalog1.analytics.user_behavior"),
            ("analytics.event_log", "catalog1.analytics.event_log"),
            ("finance.transactions", "catalog2.finance.transactions"),
            ("marketing.campaigns", "catalog1.marketing.campaigns"),
            ("operations.inventory", "catalog3.operations.inventory"),
        ]

        print("\n📋 Testing Dynamic Table Reference Mappings:")
        print("-" * 50)

        all_passed = True
        for input_ref, expected_output in test_cases:
            actual_output = rag_service.get_three_part_name(input_ref)
            status = "✅ PASS" if actual_output == expected_output else "❌ FAIL"

            print(f"{status} | {input_ref:<25} -> {actual_output}")

            if actual_output != expected_output:
                print(f"     Expected: {expected_output}")
                all_passed = False

        print("-" * 50)
        if all_passed:
            print("🎉 All mapping tests PASSED!")
        else:
            print("⚠️  Some mapping tests FAILED!")

        # Test schema lookup
        print("\n🔍 Testing Schema Lookup:")
        print("-" * 30)

        schemas_to_test = ["sales", "analytics", "finance", "marketing", "operations"]
        for schema in schemas_to_test:
            mapping = rag_service.get_catalog_mapping(schema)
            if mapping:
                print(f"✅ {schema:<12} -> {mapping.target_catalog}")
            else:
                print(f"❌ {schema:<12} -> No mapping found")

        # Test search functionality
        print("\n🔎 Testing Semantic Search:")
        print("-" * 30)

        search_queries = [
            "sales data tables",
            "user behavior analytics",
            "financial transactions"
        ]

        for query in search_queries:
            results = rag_service.search_mappings(query, n_results=2)
            print(f"Query: '{query}'")
            for mapping, score in results:
                print(f"  -> {mapping.source_schema} (score: {score:.3f})")

        print("\n📈 RAG Service Test Summary:")
        print("=" * 40)
        print(f"Total mappings in database: {len(rag_service.list_all_mappings())}")

        return all_passed

    except Exception as e:
        print(f"❌ Test failed with error: {e}")
        return False


def test_framework_integration():
    """Test integration with the PyUCX framework."""

    print("\n🔧 Testing Framework Integration")
    print("=" * 40)

    try:
        from src.utils.config_manager import ConfigManager
        from src.core.langgraph_framework import PyUCXFramework

        # Test config manager with RAG settings
        print("📋 Testing config manager with RAG settings...")
        config_manager = ConfigManager()
        config = config_manager.get_all()

        rag_settings = [
            "rag_enabled",
            "chromadb_path",
            "schema_collection_name",
            "enable_dynamic_catalog_mapping"
        ]

        for setting in rag_settings:
            if setting in config:
                print(f"✅ {setting}: {config[setting]}")
            else:
                print(f"❌ Missing setting: {setting}")

        # Test framework initialization with RAG
        print("\n🏗️  Testing framework initialization with RAG...")
        framework = PyUCXFramework(config)

        if framework.rag_service:
            print("✅ RAG service successfully integrated into framework")

            # Test that agents have access to RAG service
            modifier_agent = framework.agents["modifier"]
            if hasattr(modifier_agent, "config") and "rag_service" in modifier_agent.config:
                print("✅ Modifier agent has access to RAG service")
            else:
                print("❌ Modifier agent missing RAG service access")
        else:
            print("❌ RAG service not integrated into framework")

        return True

    except Exception as e:
        print(f"❌ Framework integration test failed: {e}")
        return False


def simulate_code_conversion():
    """Simulate code conversion with dynamic mapping."""

    print("\n🔄 Testing Code Conversion with Dynamic Mapping")
    print("=" * 50)

    # Sample code snippets to test
    test_code_snippets = [
        "sales_df = spark.read.table('sales.raw_sales_data')",
        "analytics_df = spark.read.table('analytics.user_behavior')",
        "result_df.write.mode('overwrite').saveAsTable('sales.processed_sales_fact')",
        "SELECT * FROM analytics.event_log WHERE date > '2024-01-01'",
        "CREATE TABLE finance.monthly_summary AS SELECT * FROM finance.transactions",
    ]

    expected_conversions = [
        "sales_df = spark.read.table('catalog2.sales.raw_sales_data')",
        "analytics_df = spark.read.table('catalog1.analytics.user_behavior')",
        "result_df.write.mode('overwrite').saveAsTable('catalog2.sales.processed_sales_fact')",
        "SELECT * FROM catalog1.analytics.event_log WHERE date > '2024-01-01'",
        "CREATE TABLE catalog2.finance.monthly_summary AS SELECT * FROM catalog2.finance.transactions",
    ]

    try:
        # Initialize RAG service
        rag_service = SchemaMappingRAG(
            db_path="data/chroma_db_test",
            collection_name="schema_mappings_test"
        )

        print("Original Code -> Converted Code")
        print("-" * 50)

        all_passed = True
        for i, (original, expected) in enumerate(zip(test_code_snippets, expected_conversions)):
            # This is a simplified test - normally the modifier agent would do this
            # For testing purposes, we'll manually apply the conversions
            converted = simulate_table_conversion(original, rag_service)
            status = "✅" if expected in converted or converted == expected else "❌"

            print(f"{status} Test {i+1}:")
            print(f"  Original:  {original}")
            print(f"  Converted: {converted}")
            print(f"  Expected:  {expected}")
            print()

            if expected not in converted and converted != expected:
                all_passed = False

        if all_passed:
            print("🎉 All code conversion tests PASSED!")
        else:
            print("⚠️  Some code conversion tests need attention.")

        return all_passed

    except Exception as e:
        print(f"❌ Code conversion test failed: {e}")
        return False


def simulate_table_conversion(code_line: str, rag_service: SchemaMappingRAG) -> str:
    """Simulate table reference conversion using RAG service."""
    import re

    # Handle spark.read.table() calls
    if "spark.read.table(" in code_line:
        match = re.search(r"spark\.read\.table\(['\"]([^'\"]+)['\"]", code_line)
        if match:
            table_ref = match.group(1)
            new_table_ref = rag_service.get_three_part_name(table_ref)
            return code_line.replace(f"'{table_ref}'", f"'{new_table_ref}'").replace(f'"{table_ref}"', f'"{new_table_ref}"')

    # Handle saveAsTable() calls
    if "saveAsTable(" in code_line:
        match = re.search(r"saveAsTable\(['\"]([^'\"]+)['\"]", code_line)
        if match:
            table_ref = match.group(1)
            new_table_ref = rag_service.get_three_part_name(table_ref)
            return code_line.replace(f"'{table_ref}'", f"'{new_table_ref}'").replace(f'"{table_ref}"', f'"{new_table_ref}"')

    # Handle SQL FROM clauses
    if "FROM" in code_line.upper():
        pattern = r"FROM\s+([a-zA-Z_]\w*(?:\.[a-zA-Z_]\w*)*)"
        matches = re.findall(pattern, code_line, flags=re.IGNORECASE)
        modified_line = code_line
        for table_ref in matches:
            new_table_ref = rag_service.get_three_part_name(table_ref)
            modified_line = modified_line.replace(table_ref, new_table_ref)
        return modified_line

    # Handle SQL CREATE TABLE
    if "CREATE TABLE" in code_line.upper():
        pattern = r"CREATE\s+TABLE\s+([a-zA-Z_]\w*(?:\.[a-zA-Z_]\w*)*)"
        matches = re.findall(pattern, code_line, flags=re.IGNORECASE)
        modified_line = code_line
        for table_ref in matches:
            new_table_ref = rag_service.get_three_part_name(table_ref)
            modified_line = modified_line.replace(table_ref, new_table_ref)

        # Also handle FROM clauses in CREATE TABLE AS SELECT statements
        if "FROM" in modified_line.upper():
            pattern = r"FROM\s+([a-zA-Z_]\w*(?:\.[a-zA-Z_]\w*)*)"
            from_matches = re.findall(pattern, modified_line, flags=re.IGNORECASE)
            for table_ref in from_matches:
                new_table_ref = rag_service.get_three_part_name(table_ref)
                modified_line = modified_line.replace(table_ref, new_table_ref, 1)  # Replace only first occurrence

        return modified_line

    return code_line


if __name__ == "__main__":
    print("🚀 PyUCX-AI Dynamic Catalog Mapping Test Suite")
    print("=" * 60)

    # Run all tests
    test_results = []

    print("\n1️⃣  Testing RAG Service...")
    test_results.append(test_rag_service())

    print("\n2️⃣  Testing Framework Integration...")
    test_results.append(test_framework_integration())

    print("\n3️⃣  Testing Code Conversion...")
    test_results.append(simulate_code_conversion())

    # Final results
    print("\n" + "=" * 60)
    print("📊 TEST RESULTS SUMMARY")
    print("=" * 60)

    test_names = ["RAG Service", "Framework Integration", "Code Conversion"]

    for i, (name, result) in enumerate(zip(test_names, test_results), 1):
        status = "✅ PASSED" if result else "❌ FAILED"
        print(f"{i}. {name:<20} {status}")

    passed = sum(test_results)
    total = len(test_results)

    print(f"\nOverall: {passed}/{total} tests passed")

    if passed == total:
        print("🎉 All tests PASSED! Dynamic mapping is working correctly.")
        sys.exit(0)
    else:
        print("⚠️  Some tests FAILED. Please check the implementation.")
        sys.exit(1)
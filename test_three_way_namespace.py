#!/usr/bin/env python3
"""
Test script to verify the three-way namespace conversion functionality.
"""

import sys
import os
sys.path.insert(0, os.path.join(os.getcwd(), 'src'))

from src.agents.modifier_agent import ModifierAgent

def test_table_reference_conversion():
    """Test the table reference conversion logic."""
    
    # Create a config with default values
    config = {
        "default_catalog": "main",
        "default_schema": "default",
        "rag_service": None  # Test without RAG service first
    }
    
    # Create modifier agent
    modifier = ModifierAgent(config)
    
    # Test cases for table reference conversion
    test_cases = [
        # (input, expected_pattern)
        ("user_behavior", "main.analytics.user_behavior"),
        ("sales.raw_sales_data", "main.sales.raw_sales_data"),
        ("analytics.event_log", "main.analytics.event_log"),
        ("main.sales.customer_master", "main.sales.customer_master"),  # Already three-part
        ("product_catalog", "main.sales.product_catalog"),  # Should infer sales schema
        ("performance_metrics", "main.analytics.performance_metrics"),  # Should infer analytics schema
        ("raw_data", "main.raw.raw_data"),  # Should infer raw schema
        ("processed_sales", "main.processed.processed_sales"),  # Should infer processed schema
        ("customer_profiles", "main.users.customer_profiles"),  # Should infer users schema
        ("some_table", "main.default.some_table"),  # Default schema
    ]
    
    print("Testing three-way namespace conversion:")
    print("=" * 60)
    
    for input_ref, expected in test_cases:
        result = modifier._get_dynamic_table_reference(input_ref)
        status = "✅ PASS" if result == expected else "❌ FAIL"
        print(f"{status} '{input_ref}' -> '{result}' (expected: '{expected}')")
    
    print("\nTesting SQL code modifications:")
    print("=" * 60)
    
    # Test SQL modifications
    sql_test_cases = [
        ("SELECT * FROM user_behavior", "main.analytics.user_behavior"),
        ("JOIN sales.customer_master ON", "main.sales.customer_master"),
        ("INSERT INTO analytics.summary", "main.analytics.summary"),
        ("CREATE TABLE sales_fact AS", "main.sales.sales_fact"),
    ]
    
    for sql_line, expected_table in sql_test_cases:
        result = modifier._modify_sql_code(sql_line, "test message")
        contains_expected = expected_table in result
        status = "✅ PASS" if contains_expected else "❌ FAIL"
        print(f"{status} '{sql_line}'")
        print(f"     -> '{result}'")
        print(f"     Expected to contain: '{expected_table}'")
        print()
    
    print("Testing Spark code modifications:")
    print("=" * 60)
    
    # Test Spark modifications
    spark_test_cases = [
        ("spark.read.table('sales.raw_data')", "main.sales.raw_data"),
        ("df.write.saveAsTable('analytics_summary')", "main.analytics.analytics_summary"),
        ("spark.read.table('user_behavior')", "main.analytics.user_behavior"),
    ]
    
    for spark_line, expected_table in spark_test_cases:
        result = modifier._modify_spark_code(spark_line, "test message")
        contains_expected = expected_table in result
        status = "✅ PASS" if contains_expected else "❌ FAIL"
        print(f"{status} '{spark_line}'")
        print(f"     -> '{result}'")
        print(f"     Expected to contain: '{expected_table}'")
        print()

if __name__ == "__main__":
    test_table_reference_conversion()

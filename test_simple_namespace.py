#!/usr/bin/env python3
"""
Simple test script to verify the three-way namespace conversion logic.
"""

import re

def get_dynamic_table_reference(table_ref: str, config: dict = None) -> str:
    """
    Convert a table reference to three-part naming using dynamic catalog mapping.
    Simplified version of the logic from ModifierAgent.
    """
    if config is None:
        config = {"default_catalog": "main", "default_schema": "default"}
        
    # Clean the table reference
    table_ref = table_ref.strip()
    
    # Check if already a three-part name
    parts = table_ref.split('.')
    if len(parts) >= 3:
        return table_ref  # Already properly formatted
        
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
        catalog = config.get("default_catalog", "main")
        return f"{catalog}.{schema}.{table_name}"

    elif len(parts) == 2:
        # schema.table format - use default catalog
        catalog = config.get("default_catalog", "main")
        return f"{catalog}.{table_ref}"

    else:
        # Fallback - shouldn't happen but just in case
        return table_ref

def modify_sql_code(line: str) -> str:
    """Simplified SQL modification logic."""
    modified_line = line
    
    # Comprehensive SQL table reference patterns
    sql_patterns = [
        # FROM clauses
        (r"\bFROM\s+([a-zA-Z_]\w*(?:\.[a-zA-Z_]\w*)*)", "FROM"),
        # JOIN clauses
        (r"\bJOIN\s+([a-zA-Z_]\w*(?:\.[a-zA-Z_]\w*)*)", "JOIN"),
        (r"\bINNER\s+JOIN\s+([a-zA-Z_]\w*(?:\.[a-zA-Z_]\w*)*)", "INNER JOIN"),
        (r"\bLEFT\s+JOIN\s+([a-zA-Z_]\w*(?:\.[a-zA-Z_]\w*)*)", "LEFT JOIN"),
        # CREATE TABLE
        (r"\bCREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?([a-zA-Z_]\w*(?:\.[a-zA-Z_]\w*)*)", "CREATE TABLE"),
        # INSERT INTO
        (r"\bINSERT\s+INTO\s+([a-zA-Z_]\w*(?:\.[a-zA-Z_]\w*)*)", "INSERT INTO"),
    ]
    
    for pattern, clause_type in sql_patterns:
        matches = re.finditer(pattern, modified_line, flags=re.IGNORECASE)
        for match in reversed(list(matches)):  # Reverse to avoid index shifting
            table_ref = match.group(1)
            # Skip if table reference already has 3+ parts or contains functions
            if table_ref.count('.') >= 2 or '(' in table_ref or ')' in table_ref:
                continue
                
            new_table_ref = get_dynamic_table_reference(table_ref)
            # Replace only the specific match, not all occurrences
            start, end = match.span(1)
            modified_line = modified_line[:start] + new_table_ref + modified_line[end:]
    
    return modified_line

def modify_spark_code(line: str) -> str:
    """Simplified Spark modification logic."""
    # Handle spark.read.table() calls with comprehensive pattern matching
    if "spark.read.table(" in line:
        table_pattern = r"spark\.read\.table\(['\"]([^'\"]+)['\"]"
        for match in re.finditer(table_pattern, line):
            table_ref = match.group(1)
            new_table_ref = get_dynamic_table_reference(table_ref)
            line = line.replace(match.group(0), 
                f"spark.read.table('{new_table_ref}')")

    # Handle saveAsTable calls
    if "saveAsTable(" in line:
        table_pattern = r"saveAsTable\(['\"]([^'\"]+)['\"]"
        for match in re.finditer(table_pattern, line):
            table_ref = match.group(1)
            new_table_ref = get_dynamic_table_reference(table_ref)
            line = line.replace(match.group(0), 
                f"saveAsTable('{new_table_ref}')")
    
    return line

def test_table_reference_conversion():
    """Test the table reference conversion logic."""
    
    print("Testing three-way namespace conversion:")
    print("=" * 60)
    
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
    
    all_passed = True
    for input_ref, expected in test_cases:
        result = get_dynamic_table_reference(input_ref)
        passed = result == expected
        all_passed = all_passed and passed
        status = "✅ PASS" if passed else "❌ FAIL"
        print(f"{status} '{input_ref}' -> '{result}' (expected: '{expected}')")
    
    print(f"\nTable Reference Tests: {'✅ ALL PASSED' if all_passed else '❌ SOME FAILED'}")
    
    print("\nTesting SQL code modifications:")
    print("=" * 60)
    
    # Test SQL modifications
    sql_test_cases = [
        ("SELECT * FROM user_behavior", "main.analytics.user_behavior"),
        ("JOIN sales.customer_master ON", "main.sales.customer_master"),
        ("INSERT INTO analytics.summary", "main.analytics.summary"),
        ("CREATE TABLE sales_fact AS", "main.sales.sales_fact"),
        ("SELECT * FROM raw_sales_data r JOIN product_catalog p", ["main.raw.raw_sales_data", "main.sales.product_catalog"]),
    ]
    
    sql_passed = True
    for test_case in sql_test_cases:
        if len(test_case) == 2:
            sql_line, expected_table = test_case
            expected_tables = [expected_table]
        else:
            sql_line, expected_tables = test_case
            
        result = modify_sql_code(sql_line)
        
        all_expected_found = all(expected in result for expected in expected_tables)
        sql_passed = sql_passed and all_expected_found
        status = "✅ PASS" if all_expected_found else "❌ FAIL"
        print(f"{status} '{sql_line}'")
        print(f"     -> '{result}'")
        print(f"     Expected to contain: {expected_tables}")
        print()
    
    print(f"SQL Modification Tests: {'✅ ALL PASSED' if sql_passed else '❌ SOME FAILED'}")
    
    print("\nTesting Spark code modifications:")
    print("=" * 60)
    
    # Test Spark modifications
    spark_test_cases = [
        ("spark.read.table('sales.raw_data')", "main.sales.raw_data"),
        ("df.write.saveAsTable('analytics_summary')", "main.analytics.analytics_summary"),
        ("spark.read.table('user_behavior')", "main.analytics.user_behavior"),
    ]
    
    spark_passed = True
    for spark_line, expected_table in spark_test_cases:
        result = modify_spark_code(spark_line)
        contains_expected = expected_table in result
        spark_passed = spark_passed and contains_expected
        status = "✅ PASS" if contains_expected else "❌ FAIL"
        print(f"{status} '{spark_line}'")
        print(f"     -> '{result}'")
        print(f"     Expected to contain: '{expected_table}'")
        print()

    print(f"Spark Modification Tests: {'✅ ALL PASSED' if spark_passed else '❌ SOME FAILED'}")
    
    # Overall summary
    overall_passed = all_passed and sql_passed and spark_passed
    print("\n" + "=" * 60)
    print(f"OVERALL RESULT: {'✅ ALL TESTS PASSED' if overall_passed else '❌ SOME TESTS FAILED'}")
    print("=" * 60)
    
    return overall_passed

if __name__ == "__main__":
    test_table_reference_conversion()

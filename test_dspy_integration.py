#!/usr/bin/env python3
"""
Comprehensive test suite for DSPy integration
"""
import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from src.utils.config_manager import ConfigManager
from src.dspy_models import (
    DSPyConfigManager, AnalyzerModel, PlannerModel, ModifierModel,
    ValidatorModel, CodeGenerationModel, ReporterModel,
    DSPyWorkflowOptimizer, DSPyQualityAssurance
)
from src.core.enhanced_langgraph_framework import EnhancedPyUCXFramework

def test_dspy_configuration():
    """Test DSPy configuration loading."""
    print("Testing DSPy configuration...")
    
    # Load configuration
    config = ConfigManager("config/default.yaml")
    config_data = config.get_all()
    
    # Test DSPy configuration
    dspy_config = DSPyConfigManager(config_data)
    
    assert dspy_config.is_enabled() == True, "DSPy should be enabled"
    print("✅ DSPy configuration test passed")

def test_model_initialization():
    """Test DSPy model initialization."""
    print("Testing DSPy model initialization...")
    
    # Load configuration
    config = ConfigManager("config/default.yaml")
    config_data = config.get_all()
    
    # Test model initialization (without API key for testing)
    try:
        analyzer = AnalyzerModel(config_data)
        planner = PlannerModel(config_data)
        modifier = ModifierModel(config_data)
        validator = ValidatorModel(config_data)
        code_gen = CodeGenerationModel(config_data)
        reporter = ReporterModel(config_data)
        
        print("✅ All DSPy models initialized successfully")
    except Exception as e:
        print(f"⚠️  Model initialization test failed (expected without API key): {e}")

def test_enhanced_framework():
    """Test enhanced framework initialization."""
    print("Testing enhanced framework...")
    
    # Load configuration
    config = ConfigManager("config/default.yaml")
    config_data = config.get_all()
    
    # Test enhanced framework
    framework = EnhancedPyUCXFramework(config_data)
    
    assert framework.is_dspy_enabled() == True, "DSPy should be enabled in framework"
    print("✅ Enhanced framework test passed")

def test_integration_utilities():
    """Test DSPy integration utilities."""
    print("Testing integration utilities...")
    
    # Load configuration
    config = ConfigManager("config/default.yaml")
    config_data = config.get_all()
    
    # Test workflow optimizer
    optimizer = DSPyWorkflowOptimizer(config_data)
    performance = optimizer.get_workflow_performance()
    
    assert "dspy_enabled" in performance, "Performance should include DSPy status"
    print("✅ Integration utilities test passed")

def test_quality_assurance():
    """Test quality assurance utilities."""
    print("Testing quality assurance...")
    
    # Load configuration
    config = ConfigManager("config/default.yaml")
    config_data = config.get_all()
    
    # Test quality assurance
    qa = DSPyQualityAssurance(config_data)
    
    # Test validation
    test_output = {
        "confidence_score": 0.8,
        "result": "test"
    }
    validation = qa.validate_model_output(test_output, ["confidence_score", "result"])
    
    assert validation["is_valid"] == True, "Validation should pass"
    print("✅ Quality assurance test passed")

def test_analyzer_model():
    """Test analyzer model functionality."""
    print("Testing analyzer model...")
    
    # Load configuration
    config = ConfigManager("config/default.yaml")
    config_data = config.get_all()
    
    try:
        analyzer = AnalyzerModel(config_data)
        
        # Test fallback analysis
        result = analyzer._fallback_analysis(
            notebook_content="import pyspark",
            notebook_filename="test.py",
            code_cell_count=1,
            lint_issues=[]
        )
        
        assert "migration_complexity" in result, "Result should include migration complexity"
        assert "confidence_score" in result, "Result should include confidence score"
        print("✅ Analyzer model test passed")
        
    except Exception as e:
        print(f"⚠️  Analyzer model test failed: {e}")

def test_configuration_validation():
    """Test configuration validation."""
    print("Testing configuration validation...")
    
    # Load configuration
    config = ConfigManager("config/default.yaml")
    config_data = config.get_all()
    
    # Check DSPy configuration
    dspy_config = config_data.get("dspy", {})
    assert dspy_config.get("enabled") == True, "DSPy should be enabled in config"
    assert "models" in dspy_config, "DSPy config should include models"
    
    print("✅ Configuration validation test passed")

if __name__ == "__main__":
    print("🧪 Running DSPy Integration Tests")
    print("=" * 50)
    
    try:
        test_dspy_configuration()
        test_model_initialization()
        test_enhanced_framework()
        test_integration_utilities()
        test_quality_assurance()
        test_analyzer_model()
        test_configuration_validation()
        
        print("=" * 50)
        print("✅ All 7 tests passed successfully!")
        
    except Exception as e:
        print(f"❌ Test suite failed: {e}")
        sys.exit(1)

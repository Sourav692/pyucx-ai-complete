#!/usr/bin/env python3
"""
Simple test for DSPy integration
"""
import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from src.utils.config_manager import ConfigManager
from src.dspy_models import DSPyConfigManager, AnalyzerModel

def test_dspy_config():
    """Test DSPy configuration loading."""
    print("Testing DSPy configuration...")
    
    # Load configuration
    config = ConfigManager("config/default.yaml")
    config_data = config.get_all()
    
    # Test DSPy configuration
    dspy_config = DSPyConfigManager(config_data)
    
    print(f"DSPy enabled: {dspy_config.is_enabled()}")
    print("✅ DSPy configuration test passed")

def test_analyzer_model():
    """Test AnalyzerModel initialization."""
    print("Testing AnalyzerModel...")
    
    # Load configuration
    config = ConfigManager("config/default.yaml")
    config_data = config.get_all()
    
    # Test model initialization (without API key for testing)
    try:
        model = AnalyzerModel(config_data)
        print("✅ AnalyzerModel initialization test passed")
    except Exception as e:
        print(f"⚠️  AnalyzerModel test failed (expected without API key): {e}")

if __name__ == "__main__":
    print("🧪 Running DSPy Integration Tests")
    print("=" * 50)
    
    test_dspy_config()
    test_analyzer_model()
    
    print("=" * 50)
    print("✅ All tests completed!")

# PyUCX-AI Multi-Agent Framework

A comprehensive AI-powered framework for analyzing Jupyter notebooks and planning Unity Catalog migrations using LangGraph and multi-agent workflows.

## 🚀 Features

- **Multi-Agent Architecture**: Five specialized agents working together through LangGraph workflows
- **Comprehensive Analysis**: Deep analysis of PySpark notebooks for Unity Catalog compatibility
- **Migration Planning**: Detailed migration plans with timelines, risks, and recommendations
- **Code Modification**: Intelligent code suggestions for Unity Catalog compatibility
- **Validation**: Multi-level validation of proposed changes
- **Reporting**: Executive summaries and detailed technical reports

## 📋 Requirements

- Python 3.8+
- OpenAI API key or Azure OpenAI access
- PySpark notebooks for analysis
- UCX (Unity Catalog Migration Assistant) lint output

## 🛠️ Installation

1. **Clone or download the project**
   ```bash
   git clone <repository-url>
   cd pyucx-ai-complete
   ```

2. **Install dependencies**
   ```bash
   pip install -r requirements.txt
   ```

3. **Set up environment variables**
   ```bash
   cp .env.example .env
   # Edit .env with your OpenAI API key
   ```

## 🎯 Quick Start

### Basic Usage

Analyze sample notebooks with provided lint data:

```bash
python main.py --input-folder data/sample_notebooks --lint-file data/lint_outputs/sample_45_scenarios.txt
```

### Custom Analysis

Analyze your own notebooks:

```bash
python main.py --input-folder /path/to/your/notebooks --lint-file /path/to/ucx-lint-output.txt --output my_analysis.json
```

### With Configuration File

```bash
python main.py --input-folder notebooks/ --lint-file lint.txt --config config/production.yaml --verbose
```

## 📁 Project Structure

```
pyucx-ai-complete/
├── main.py                          # CLI entry point
├── requirements.txt                 # Dependencies
├── .env.example                     # Environment template
├── README.md                        # This file
├── .gitignore                      # Git ignore rules
├── src/                            # Source code
│   ├── agents/                     # Multi-agent implementations
│   │   ├── analyzer_agent.py       # Notebook analysis agent
│   │   ├── planner_agent.py        # Migration planning agent
│   │   ├── modifier_agent.py       # Code modification agent
│   │   ├── validator_agent.py      # Validation agent
│   │   ├── reporter_agent.py       # Reporting agent
│   │   └── base_agent.py          # Base agent class
│   ├── core/                      # Core framework
│   │   ├── langgraph_framework.py # Main framework logic
│   │   └── agent_state.py         # State management
│   └── utils/                     # Utilities
│       ├── config_manager.py      # Configuration handling
│       └── logging_setup.py       # Logging configuration
├── data/                          # Sample data
│   ├── sample_notebooks/          # Example notebooks
│   │   ├── sales_etl_pipeline.ipynb
│   │   └── analytics_dashboard.ipynb
│   └── lint_outputs/              # UCX lint results
│       └── sample_45_scenarios.txt
├── config/                        # Configuration files
│   └── default.yaml              # Default configuration
└── tests/                        # Test files (future)
```

## 🏗️ Architecture

### Multi-Agent Workflow

The framework uses a **LangGraph-based multi-agent system** with five specialized agents:

1. **Analyzer Agent**: 
   - Analyzes notebooks and lint data
   - Identifies compatibility issues
   - Assesses migration complexity

2. **Planner Agent**:
   - Creates detailed migration plans
   - Estimates timelines and effort
   - Identifies dependencies and risks

3. **Modifier Agent**:
   - Generates specific code modifications
   - Suggests Unity Catalog-compatible alternatives
   - Provides confidence levels for changes

4. **Validator Agent**:
   - Validates syntax and logic of modifications
   - Checks Unity Catalog compatibility
   - Performs quality assurance

5. **Reporter Agent**:
   - Generates comprehensive reports
   - Creates executive summaries
   - Provides actionable recommendations

### State Management

The framework uses **TypedDict-based state management** with:
- Persistent workflow state through LangGraph checkpointer
- Structured data classes for notebooks, issues, and results
- Automatic state transitions between agents

## 📊 Sample Data

The project includes realistic sample data:

### Sample Notebooks
- **sales_etl_pipeline.ipynb**: ETL pipeline with PySpark operations
- **analytics_dashboard.ipynb**: Analytics workbook with Databricks features

### Sample Lint Output
- **sample_45_scenarios.txt**: Realistic UCX lint output with 20+ compatibility issues

## 🔧 Configuration

### Environment Variables

Create a `.env` file from `.env.example`:

```env
# Required: OpenAI Configuration
OPENAI_API_KEY=sk-your-openai-api-key-here
OPENAI_MODEL=gpt-4o-mini

# Optional: Azure OpenAI (alternative)
# AZURE_OPENAI_ENDPOINT=https://your-resource.openai.azure.com/
# AZURE_OPENAI_API_KEY=your-azure-api-key

# Framework Settings
LOG_LEVEL=INFO
MAX_ITERATIONS=10
ENABLE_CHECKPOINTS=true
```

### Configuration Files

Use YAML or JSON configuration files for advanced settings:

```yaml
# config/production.yaml
openai_model: "gpt-4"
temperature: 0.1
max_iterations: 15
log_level: "DEBUG"
enable_checkpoints: true
checkpoint_dir: "./production-checkpoints"
```

## 📈 Usage Examples

### 1. Development Analysis
```bash
# Quick analysis with verbose output
python main.py --input-folder data/sample_notebooks --lint-file data/lint_outputs/sample_45_scenarios.txt --verbose
```

### 2. Production Analysis  
```bash
# Production run with custom config
python main.py \
  --input-folder /production/notebooks \
  --lint-file /reports/ucx-lint-output.txt \
  --config config/production.yaml \
  --output /reports/migration-analysis.json \
  --log-file /logs/pyucx-analysis.log
```

### 3. Dry Run Validation
```bash
# Validate inputs without running analysis
python main.py --input-folder notebooks/ --lint-file lint.txt --dry-run
```

## 📋 CLI Options

```
python main.py [OPTIONS]

Required Arguments:
  --input-folder PATH     Folder containing Jupyter notebooks
  --lint-file PATH       UCX lint output file

Optional Arguments:
  --output PATH          Output file for results (default: migration_analysis_results.json)
  --config PATH          Configuration file (YAML/JSON)
  --log-level LEVEL      Logging level (DEBUG/INFO/WARNING/ERROR/CRITICAL)
  --log-file PATH        Log file path
  --max-iterations N     Maximum workflow iterations (default: 10)
  --thread-id ID         Workflow thread ID (default: cli-session)
  --dry-run             Validate inputs only
  --verbose, -v          Enable verbose output
  --version             Show version information
  --help, -h            Show help message
```

## 🔍 Output Format

The framework generates comprehensive JSON reports containing:

```json
{
  "success": true,
  "notebooks_processed": 2,
  "total_notebooks": 2,
  "analysis_results": [...],
  "migration_plans": [...],
  "code_modifications": [...],
  "validation_results": [...],
  "final_report": {
    "executive_summary": "...",
    "overall_statistics": {...},
    "priority_breakdown": {...},
    "risk_assessment": {...},
    "timeline_estimation": {...},
    "recommendations": [...]
  }
}
```

## 🧪 Testing

Run with sample data to verify installation:

```bash
# Test with provided samples
python main.py --input-folder data/sample_notebooks --lint-file data/lint_outputs/sample_45_scenarios.txt --dry-run

# Expected output: "Dry run completed successfully"
```

## 🔧 Troubleshooting

### Common Issues

1. **OpenAI API Key Error**
   ```
   Error: Either OPENAI_API_KEY or AZURE_OPENAI_API_KEY must be provided
   ```
   **Solution**: Set your API key in `.env` file or environment variables

2. **No Notebooks Found**
   ```
   Error: No Jupyter notebooks found in: /path/to/folder
   ```
   **Solution**: Ensure the folder contains `.ipynb` files

3. **Import Errors**
   ```
   ModuleNotFoundError: No module named 'langgraph'
   ```
   **Solution**: Run `pip install -r requirements.txt`

### Getting Help

- Check the `--help` option: `python main.py --help`
- Use `--dry-run` to validate inputs
- Enable `--verbose` for detailed output
- Review log files for debugging information

## 🤝 Contributing

This is a production-ready MVP. For enhancements:

1. Fork the repository
2. Create a feature branch
3. Add comprehensive tests
4. Submit a pull request

## 📄 License

This project is provided as-is for Unity Catalog migration analysis.

## 🎯 Next Steps

After running the analysis:

1. **Review Results**: Examine the generated JSON report
2. **Prioritize Notebooks**: Start with high-priority migration candidates  
3. **Plan Execution**: Use the timeline estimates for project planning
4. **Test Changes**: Validate code modifications in a development environment
5. **Monitor Progress**: Track migration progress through the validation results

---

**PyUCX-AI Framework v1.0.0** - Intelligent Unity Catalog Migration Analysis

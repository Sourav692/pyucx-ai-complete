#!/usr/bin/env python3
"""
PyUCX-AI Multi-Agent Framework - Main CLI Entry Point

This script provides the command-line interface for running the PyUCX-AI framework
to analyze Jupyter notebooks and plan Unity Catalog migrations.

Usage:
    python main.py --input-folder data/sample_notebooks --lint-file data/lint_outputs/sample_45_scenarios.txt

For more options:
    python main.py --help
"""

import sys
import os
import argparse
import logging
from pathlib import Path
from typing import List, Dict, Any, Optional

# Add the src directory to Python path
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), 'src'))

from src.utils.logging_setup import quick_setup
from src.utils.config_manager import ConfigManager
from src.core.langgraph_framework import (
    PyUCXFramework, 
    load_notebooks_from_folder,
    load_python_files_from_folder, 
    load_lint_data_from_file,
    save_results_to_file
)


def parse_arguments() -> argparse.Namespace:
    """Parse command-line arguments."""

    parser = argparse.ArgumentParser(
        description="PyUCX-AI Multi-Agent Framework for Unity Catalog Migration Analysis",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Basic usage with sample data
  python main.py --input-folder data/sample_notebooks --lint-file data/lint_outputs/sample_45_scenarios.txt

  # Custom configuration
  python main.py --input-folder /path/to/notebooks --lint-file lint_results.txt --config config/custom.yaml

  # Verbose output with custom output location
  python main.py --input-folder notebooks/ --lint-file lint.txt --output results.json --verbose

  # Dry run to validate inputs
  python main.py --input-folder notebooks/ --lint-file lint.txt --dry-run
        """
    )

    # Required arguments
    parser.add_argument(
        "--input-folder", 
        required=True,
        help="Path to folder containing Python files to analyze"
    )

    parser.add_argument(
        "--lint-file",
        required=True, 
        help="Path to UCX lint output file"
    )

    # Optional arguments
    parser.add_argument(
        "--output",
        default="migration_analysis_results.json",
        help="Output file path for results (default: migration_analysis_results.json)"
    )

    parser.add_argument(
        "--config",
        help="Path to configuration file (YAML or JSON)"
    )

    parser.add_argument(
        "--log-level",
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
        default="INFO",
        help="Logging level (default: INFO)"
    )

    parser.add_argument(
        "--log-file",
        help="Path to log file (default: console only)"
    )

    parser.add_argument(
        "--max-iterations",
        type=int,
        default=200,
        help="Maximum workflow iterations (default: 200)"
    )

    parser.add_argument(
        "--thread-id",
        default="cli-session",
        help="Thread ID for workflow execution (default: cli-session)"
    )

    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Validate inputs and configuration without running analysis"
    )

    parser.add_argument(
        "--verbose", "-v",
        action="store_true",
        help="Enable verbose output"
    )

    parser.add_argument(
        "--version",
        action="version",
        version="PyUCX-AI Framework 1.0.0"
    )

    return parser.parse_args()


def validate_inputs(args: argparse.Namespace) -> bool:
    """Validate command-line arguments and inputs."""

    errors = []

    # Check input folder
    input_folder = Path(args.input_folder)
    if not input_folder.exists():
        errors.append(f"Input folder does not exist: {args.input_folder}")
    elif not input_folder.is_dir():
        errors.append(f"Input path is not a directory: {args.input_folder}")
    else:
        # Check for Python files
        python_files = list(input_folder.glob("*.py"))
        if not python_files:
            errors.append(f"No Python files found in: {args.input_folder}")

    # Check lint file
    lint_file = Path(args.lint_file)
    if not lint_file.exists():
        errors.append(f"Lint file does not exist: {args.lint_file}")
    elif not lint_file.is_file():
        errors.append(f"Lint path is not a file: {args.lint_file}")

    # Check config file if provided
    if args.config:
        config_file = Path(args.config)
        if not config_file.exists():
            errors.append(f"Config file does not exist: {args.config}")
        elif not config_file.suffix in ['.yaml', '.yml', '.json']:
            errors.append(f"Config file must be YAML or JSON: {args.config}")

    # Check output directory
    output_path = Path(args.output)
    try:
        output_path.parent.mkdir(parents=True, exist_ok=True)
    except Exception as e:
        errors.append(f"Cannot create output directory: {e}")

    if errors:
        print("Validation Errors:")
        for error in errors:
            print(f"  - {error}")
        return False

    return True


def setup_configuration(args: argparse.Namespace) -> ConfigManager:
    """Setup configuration from arguments and files."""

    # Load configuration
    if args.config:
        config = ConfigManager(config_path=args.config)
    else:
        config = ConfigManager()

    # Override with command-line arguments
    config.update({
        "log_level": args.log_level,
        "log_file": args.log_file,
        "max_iterations": args.max_iterations
    })

    return config


def print_summary(notebooks: List[Dict[str, Any]], lint_data: Dict[str, List[Dict[str, Any]]]):
    """Print summary of inputs."""

    total_lint_issues = sum(len(issues) for issues in lint_data.values())

    print("\n" + "="*60)
    print("PyUCX-AI Multi-Agent Framework - Analysis Summary")
    print("="*60)
    print(f"Python files found: {len(notebooks)}")
    print(f"Total lint issues: {total_lint_issues}")
    print(f"Files with issues: {len(lint_data)}")

    if notebooks:
        print("\nPython files to analyze:")
        for nb in notebooks:
            code_lines = nb.get('code_line_count', 0)
            print(f"  - {nb['filename']} ({code_lines} code lines)")

    if lint_data:
        print("\nLint issues by file:")
        for file_path, issues in lint_data.items():
            print(f"  - {file_path}: {len(issues)} issues")

    print("="*60)


def run_analysis(
    notebooks: List[Dict[str, Any]], 
    lint_data: Dict[str, List[Dict[str, Any]]], 
    config: ConfigManager,
    thread_id: str
) -> Dict[str, Any]:
    """Run the main analysis workflow."""

    logger = logging.getLogger(__name__)
    logger.info("Starting PyUCX-AI analysis workflow")

    try:
        # Initialize framework
        framework = PyUCXFramework(config.get_all())

        # Run analysis
        results = framework.process_notebooks(
            notebooks=notebooks,
            lint_data=lint_data,
            thread_id=thread_id
        )

        # logger.info(f"Results: {results}")

        logger.info(f"Analysis completed - Success: {results.get('success', False)}")
        return results

    except Exception as e:
        logger.error(f"Analysis failed: {str(e)}", exc_info=True)
        return {
            "success": False,
            "error": str(e),
            "notebooks_processed": 0,
            "total_notebooks": len(notebooks)
        }


def print_results_summary(results: Dict[str, Any]):
    """Print summary of analysis results."""

    success = results.get("success", False)
    notebooks_processed = results.get("notebooks_processed", 0)
    total_notebooks = results.get("total_notebooks", 0)

    print("\n" + "="*60)
    print("Analysis Results Summary")
    print("="*60)
    print(f"Status: {'SUCCESS' if success else 'FAILED'}")
    print(f"Notebooks processed: {notebooks_processed}/{total_notebooks}")

    if results.get("analysis_results"):
        print(f"Analysis results: {len(results['analysis_results'])}")

    if results.get("migration_plans"):
        print(f"Migration plans created: {len(results['migration_plans'])}")

    if results.get("code_modifications"):
        print(f"Code modifications suggested: {len(results['code_modifications'])}")

    if results.get("validation_results"):
        print(f"Validation checks performed: {len(results['validation_results'])}")

    if results.get("errors"):
        print(f"\nErrors encountered: {len(results['errors'])}")
        for error in results["errors"][:3]:  # Show first 3 errors
            print(f"  - {error}")
        if len(results["errors"]) > 3:
            print(f"  ... and {len(results['errors']) - 3} more errors")

    print(f"\nIterations used: {results.get('iterations_used', 0)}")
    print("="*60)


def main():
    """Main entry point for the CLI."""

    try:
        # Parse arguments
        args = parse_arguments()

        # Setup logging
        if args.verbose:
            log_level = "DEBUG"
        else:
            log_level = args.log_level

        logger = quick_setup(log_level=log_level, log_file=args.log_file)
        logger.info("PyUCX-AI Framework starting...")

        # Validate inputs
        if not validate_inputs(args):
            sys.exit(1)

        # Setup configuration
        config = setup_configuration(args)

        # Validate configuration
        if not config.validate_config():
            print("Configuration validation failed. Please check your settings.")
            sys.exit(1)

        # Load data
        print("Loading Python files and lint data...")
        notebooks = load_python_files_from_folder(args.input_folder)
        lint_data = load_lint_data_from_file(args.lint_file)

        if not notebooks:
            print(f"No Python files found in {args.input_folder}")
            sys.exit(1)

        # Print summary
        print_summary(notebooks, lint_data)

        # Dry run check
        if args.dry_run:
            print("\nDry run completed successfully. All inputs are valid.")
            print("Remove --dry-run flag to execute the analysis.")
            sys.exit(0)

        # # Confirm execution
        # if not args.verbose:
        #     response = input("\nProceed with analysis? (y/N): ")
        #     if response.lower() not in ['y', 'yes']:
        #         print("Analysis cancelled.")
        #         sys.exit(0)

        # Run analysis
        print("\nStarting analysis...")
        results = run_analysis(notebooks, lint_data, config, args.thread_id)

        # Save results
        save_results_to_file(results, args.output)

        # Print summary
        print_results_summary(results)
        print(f"\nDetailed results saved to: {args.output}")

        # Exit with appropriate code
        if results.get("success", False):
            print("\nAnalysis completed successfully!")
            sys.exit(0)
        else:
            print("\nAnalysis completed with errors.")
            sys.exit(1)

    except KeyboardInterrupt:
        print("\nAnalysis interrupted by user.")
        sys.exit(130)
    except Exception as e:
        print(f"\nUnexpected error: {str(e)}")
        logging.exception("Unexpected error in main")
        sys.exit(1)


if __name__ == "__main__":
    main()

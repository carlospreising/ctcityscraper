#!/bin/bash
# Test runner script for CT City Scraper

set -e  # Exit on error

echo "================================"
echo "CT City Scraper - Test Suite"
echo "================================"
echo ""

# Check if virtual environment is activated
if [ -z "$VIRTUAL_ENV" ]; then
    echo "Activating virtual environment..."
    source .venv/bin/activate
fi

# Parse command line arguments
COVERAGE=false
VERBOSE=false
SPECIFIC_TEST=""

while [[ $# -gt 0 ]]; do
    case $1 in
        --coverage|-c)
            COVERAGE=true
            shift
            ;;
        --verbose|-v)
            VERBOSE=true
            shift
            ;;
        --test|-t)
            SPECIFIC_TEST="$2"
            shift 2
            ;;
        --help|-h)
            echo "Usage: ./run_tests.sh [OPTIONS]"
            echo ""
            echo "Options:"
            echo "  -c, --coverage     Generate coverage report"
            echo "  -v, --verbose      Verbose output"
            echo "  -t, --test FILE    Run specific test file"
            echo "  -h, --help         Show this help message"
            echo ""
            echo "Examples:"
            echo "  ./run_tests.sh                    # Run all tests"
            echo "  ./run_tests.sh -c                 # Run with coverage"
            echo "  ./run_tests.sh -t test_database   # Run specific test"
            echo "  ./run_tests.sh -c -v              # Coverage + verbose"
            exit 0
            ;;
        *)
            echo "Unknown option: $1"
            echo "Use --help for usage information"
            exit 1
            ;;
    esac
done

# Build pytest command
CMD="python -m pytest"

if [ -n "$SPECIFIC_TEST" ]; then
    # Run specific test file
    if [[ "$SPECIFIC_TEST" != tests/* ]]; then
        SPECIFIC_TEST="tests/$SPECIFIC_TEST"
    fi
    if [[ "$SPECIFIC_TEST" != *.py ]]; then
        SPECIFIC_TEST="$SPECIFIC_TEST.py"
    fi
    CMD="$CMD $SPECIFIC_TEST"
else
    CMD="$CMD tests/"
fi

if [ "$VERBOSE" = true ]; then
    CMD="$CMD -v"
fi

if [ "$COVERAGE" = true ]; then
    CMD="$CMD --cov=src --cov-report=term-missing --cov-report=html"
fi

# Run tests
echo "Running: $CMD"
echo ""
eval $CMD

# Show coverage report location if generated
if [ "$COVERAGE" = true ]; then
    echo ""
    echo "================================"
    echo "Coverage report generated!"
    echo "HTML report: htmlcov/index.html"
    echo ""
    echo "Open with: open htmlcov/index.html"
    echo "================================"
fi

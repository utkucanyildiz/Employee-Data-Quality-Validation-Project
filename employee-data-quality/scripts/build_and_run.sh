#!/bin/bash

# Employee Data Quality Validation - Build and Run Script
# This script builds and runs the complete data quality validation pipeline

set -e  # Exit on any error

echo "🚀 Employee Data Quality Validation Pipeline"
echo "============================================="

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Function to print colored output
print_status() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

print_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Check prerequisites
print_status "Checking prerequisites..."

if ! command -v docker &> /dev/null; then
    print_error "Docker is not installed. Please install Docker and try again."
    exit 1
fi

if ! command -v docker-compose &> /dev/null; then
    print_error "Docker Compose is not installed. Please install Docker Compose and try again."
    exit 1
fi

# Check Docker daemon
if ! docker info &> /dev/null; then
    print_error "Docker daemon is not running. Please start Docker and try again."
    exit 1
fi

print_success "Prerequisites check passed"

# Create necessary directories
print_status "Creating output directories..."
mkdir -p output/ge_results
mkdir -p output/deequ_results
mkdir -p docs

# Set permissions
chmod 777 output/ge_results
chmod 777 output/deequ_results

print_success "Directories created"

# Function to run validation modes
run_individual_validations() {
    print_status "Running individual validations..."
    
    # Build images first
    print_status "Building Docker images..."
    docker-compose build
    
    # Run Great Expectations
    print_status "Running Great Expectations validation..."
    if docker-compose up --exit-code-from ge_service ge_service; then
        print_success "Great Expectations validation completed"
    else
        print_warning "Great Expectations validation had issues"
    fi
    
    # Run Deequ
    print_status "Running Deequ validation..."
    if docker-compose up --exit-code-from deequ_service deequ_service; then
        print_success "Deequ validation completed"
    else
        print_warning "Deequ validation had issues"
    fi
}

run_combined_pipeline() {
    print_status "Running combined pipeline..."
    
    # Ensure Deequ has run first
    print_status "Ensuring Deequ metrics are available..."
    docker-compose up --exit-code-from deequ_service deequ_service
    
    # Run combined pipeline
    print_status "Running Deequ → Great Expectations combined pipeline..."
    if docker-compose --profile combined up --exit-code-from combined_pipeline combined_pipeline; then
        print_success "Combined pipeline completed"
    else
        print_warning "Combined pipeline had issues"
    fi
}

display_results() {
    print_status "Validation Results Summary"
    echo "=========================="
    
    # Count result files
    ge_files=$(find output/ge_results -name "*.json" 2>/dev/null | wc -l)
    deequ_files=$(find output/deequ_results -name "*.json" 2>/dev/null | wc -l)
    
    echo "Great Expectations results: $ge_files files"
    echo "Deequ results: $deequ_files files"
    
    # Show file listings
    if [ "$ge_files" -gt 0 ]; then
        echo ""
        echo "Great Expectations files:"
        ls -la output/ge_results/
    fi
    
    if [ "$deequ_files" -gt 0 ]; then
        echo ""
        echo "Deequ files:"
        ls -la output/deequ_results/
    fi
    
    # Check for combined pipeline results
    if [ -f "output/combined_pipeline_summary.json" ]; then
        echo ""
        echo "Combined pipeline summary available: output/combined_pipeline_summary.json"
    fi
}

cleanup() {
    print_status "Cleaning up containers..."
    docker-compose down --remove-orphans
    print_success "Cleanup completed"
}

# Main execution based on arguments
case "${1:-all}" in
    "individual")
        run_individual_validations
        ;;
    "combined")
        run_combined_pipeline
        ;;
    "all")
        run_individual_validations
        echo ""
        run_combined_pipeline
        ;;
    "clean")
        cleanup
        print_status "Removing output files..."
        rm -rf output/ge_results/*
        rm -rf output/deequ_results/*
        rm -f output/combined_pipeline_summary.json
        print_success "Clean completed"
        exit 0
        ;;
    "help"|"-h"|"--help")
        echo "Usage: $0 [mode]"
        echo ""
        echo "Modes:"
        echo "  individual  - Run GE and Deequ validations separately"
        echo "  combined    - Run combined Deequ → GE pipeline"
        echo "  all         - Run both individual and combined (default)"
        echo "  clean       - Stop containers and clean output files"
        echo "  help        - Show this help message"
        echo ""
        echo "Examples:"
        echo "  $0                    # Run everything"
        echo "  $0 individual         # Run only individual validations"
        echo "  $0 combined           # Run only combined pipeline"
        echo "  $0 clean              # Clean up"
        exit 0
        ;;
    *)
        print_error "Unknown mode: $1"
        echo "Use '$0 help' for usage information"
        exit 1
        ;;
esac

# Display results
echo ""
display_results

# Cleanup
echo ""
cleanup

print_success "Pipeline execution completed!"
echo ""
echo "📊 Check the output/ directory for validation results"
echo "📚 Read docs/docker_in_data_pipelines.md for Docker analysis"
echo "🐳 Use 'docker-compose logs [service]' to view detailed logs"
#!/bin/bash

# Setup script for Employee Data Quality Validation Project
# This script ensures all necessary directories and permissions are set up correctly

set -e

echo "🔧 Setting up Employee Data Quality Validation Project"
echo "====================================================="

# Colors for output
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

print_status() {
    echo -e "${BLUE}[SETUP]${NC} $1"
}

print_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

# Create directory structure
print_status "Creating directory structure..."

directories=(
    "dataset"
    "ge_project"
    "deequ_project/src/main/scala"
    "deequ_project/project"
    "output/ge_results"
    "output/deequ_results"
    "docs"
    "scripts"
)

for dir in "${directories[@]}"; do
    if [ ! -d "$dir" ]; then
        mkdir -p "$dir"
        print_status "Created directory: $dir"
    else
        print_status "Directory already exists: $dir"
    fi
done

# Set permissions for output directories
print_status "Setting permissions..."
chmod 755 output/
chmod 777 output/ge_results/
chmod 777 output/deequ_results/

# Make scripts executable
print_status "Making scripts executable..."
if [ -f "scripts/build_and_run.sh" ]; then
    chmod +x scripts/build_and_run.sh
fi

if [ -f "deequ_project/run_deequ_validation.sh" ]; then
    chmod +x deequ_project/run_deequ_validation.sh
fi

# Check if sample data exists
print_status "Checking sample data..."
sample_files=(
    "dataset/employees.csv"
    "dataset/salaries.csv"
    "dataset/titles.csv"
    "dataset/departments.csv"
    "dataset/dept_emp.csv"
    "dataset/dept_manager.csv"
)

missing_files=0
for file in "${sample_files[@]}"; do
    if [ ! -f "$file" ]; then
        print_warning "Missing sample file: $file"
        missing_files=$((missing_files + 1))
    fi
done

if [ $missing_files -eq 0 ]; then
    print_success "All sample data files present"
else
    print_warning "$missing_files sample data files are missing"
    echo "Please ensure all CSV files are in the dataset/ directory"
fi

# Check Docker requirements
print_status "Checking Docker requirements..."

if command -v docker &> /dev/null; then
    print_success "Docker is installed"
    
    # Check if Docker daemon is running
    if docker info &> /dev/null; then
        print_success "Docker daemon is running"
    else
        print_warning "Docker daemon is not running"
        echo "Please start Docker before running the validation pipeline"
    fi
else
    print_warning "Docker is not installed"
    echo "Please install Docker to run the validation pipeline"
fi

if command -v docker-compose &> /dev/null; then
    print_success "Docker Compose is installed"
else
    print_warning "Docker Compose is not installed"
    echo "Please install Docker Compose to run the validation pipeline"
fi

# Check available system resources
print_status "Checking system resources..."

# Check available memory (Linux/Mac)
if command -v free &> /dev/null; then
    available_mem=$(free -g | awk '/^Mem:/{print $7}')
    if [ "$available_mem" -ge 4 ]; then
        print_success "Sufficient memory available (${available_mem}GB)"
    else
        print_warning "Low memory available (${available_mem}GB). Recommend 4GB+ for Spark containers"
    fi
elif command -v vm_stat &> /dev/null; then
    # macOS memory check
    print_status "macOS detected - ensure Docker Desktop has 4GB+ memory allocated"
fi

# Check disk space
available_space=$(df -h . | awk 'NR==2{print $4}')
print_status "Available disk space: $available_space"

# Create .gitignore if it doesn't exist
print_status "Creating .gitignore..."
if [ ! -f ".gitignore" ]; then
    cat > .gitignore << EOF
# Output directories
output/ge_results/*
output/deequ_results/*
output/combined_pipeline_summary.json

# Docker
.docker/

# IDE files
.vscode/
.idea/
*.swp
*.swo

# OS files
.DS_Store
Thumbs.db

# Logs
*.log

# Temporary files
*.tmp
*.temp

# Compiled Scala
deequ_project/target/
deequ_project/project/target/
deequ_project/project/project/

# Python
__pycache__/
*.pyc
*.pyo
*.pyd
.Python
.pytest_cache/

# Keep output directories but ignore contents
!output/ge_results/.gitkeep
!output/deequ_results/.gitkeep
EOF

    # Create .gitkeep files
    touch output/ge_results/.gitkeep
    touch output/deequ_results/.gitkeep
    
    print_success "Created .gitignore with appropriate exclusions"
else
    print_status ".gitignore already exists"
fi

# Summary
echo ""
echo "🎉 Setup completed!"
echo "=================="
echo ""
echo "Next steps:"
echo "1. Ensure Docker is running"
echo "2. Run: ./scripts/build_and_run.sh"
echo "3. Check results in output/ directories"
echo ""
echo "For help: ./scripts/build_and_run.sh help"

print_success "Setup script completed successfully"
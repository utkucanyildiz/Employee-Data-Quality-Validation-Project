# Employee Data Quality Validation Project

A comprehensive dockerized data quality comparison system using Great Expectations and Apache Deequ for automated validation of employee datasets.

## 🎯 Project Overview

This project demonstrates enterprise-grade data quality validation by comparing two industry-leading tools:
- **Great Expectations (GE)**: Python-based data validation with rich reporting
- **Apache Deequ**: Scala-based data profiling built on Apache Spark

The system automatically detects table types from CSV filenames and applies appropriate validation rules, showcasing both individual tool capabilities and combined pipeline integration.

## 📁 Project Structure

```
employee-data-quality/
├── dataset/                    # Sample employee CSV files
│   ├── employees.csv          # Employee master data
│   ├── salaries.csv           # Salary history
│   ├── titles.csv             # Job titles
│   ├── departments.csv        # Department definitions
│   ├── dept_emp.csv           # Employee-department relationships
│   └── dept_manager.csv       # Department managers
├── ge_project/                # Great Expectations implementation
│   ├── Dockerfile
│   ├── requirements.txt
│   ├── auto_validator.py      # Main GE validation logic
│   └── combined_pipeline.py   # Combined Deequ→GE pipeline
├── deequ_project/             # Apache Deequ implementation
│   ├── Dockerfile
│   ├── build.sbt
│   ├── run_deequ_validation.sh
│   └── src/main/scala/DataQualityValidator.scala
├── output/                    # Validation results
│   ├── ge_results/           # Great Expectations outputs
│   └── deequ_results/        # Deequ JSON metrics
├── docs/                     # Documentation
│   └── docker_in_data_pipelines.md
├── docker-compose.yml        # Service orchestration
└── README.md
```

## 🚀 Quick Start

### Prerequisites
- Docker Engine 20.0+
- Docker Compose 2.0+
- 4GB+ available RAM (for Spark containers)

### Running Individual Validations

1. **Clone and Navigate**
   ```bash
   git clone <repository-url>
   cd employee-data-quality
   ```

2. **Run Great Expectations Validation**
   ```bash
   docker-compose up ge_service
   ```

3. **Run Deequ Validation**
   ```bash
   docker-compose up deequ_service
   ```

4. **View Results**
   ```bash
   # Great Expectations results
   ls output/ge_results/
   
   # Deequ metrics
   ls output/deequ_results/
   ```

### Running Combined Pipeline

The combined pipeline uses Deequ metrics to auto-generate Great Expectations rules:

```bash
# Run Deequ first, then combined pipeline
docker-compose up deequ_service
docker-compose --profile combined up combined_pipeline
```

### Running Everything Together

```bash
# Complete validation workflow
docker-compose up --build
```

## 🔍 Validation Features

### Great Expectations Capabilities

**Automated Table Detection**: Automatically maps CSV files to validation suites:
- `employees.csv` → Employee-specific validations (unique emp_no, valid gender)
- `salaries.csv` → Salary range and completeness checks
- `departments.csv` → Department uniqueness validations
- And more...

**Rich Reporting**: Generates detailed HTML reports with:
- Expectation success/failure details
- Data profiling summaries
- Interactive validation results

**Example Expectations**:
```python
# Automatically applied based on table detection
expect_column_values_to_be_unique(column="emp_no")
expect_column_values_to_be_in_set(column="gender", value_set=["M", "F"])
expect_column_values_to_be_between(column="salary", min_value=30000, max_value=200000)
```

### Deequ Capabilities

**Statistical Profiling**: Calculates comprehensive metrics:
- Completeness, uniqueness, distinctness
- Min/max values for numeric columns
- String length statistics
- Custom constraint validations

**Scalable Processing**: Leverages Apache Spark for:
- Large dataset processing
- Distributed computation
- Memory-efficient analysis

**Structured Output**: Generates JSON metrics for:
- Programmatic consumption
- Integration with other tools
- Automated threshold monitoring

### Combined Pipeline Innovation

**Dynamic Expectation Generation**: 
1. Deequ analyzes data and calculates metrics
2. Python script reads Deequ JSON outputs
3. Automatically generates Great Expectations based on metrics
4. Runs validation with auto-generated expectations

**Benefits**:
- Data-driven validation rules
- Reduced manual configuration
- Automatic threshold discovery
- Cross-tool validation consistency

## 📊 Output Examples

### Great Expectations Results
```json
{
  "table_name": "employees",
  "validation_success": true,
  "data_shape": {"rows": 10, "columns": 6},
  "results": [
    {
      "expectation_type": "expect_column_values_to_be_unique",
      "success": true,
      "kwargs": {"column": "emp_no"}
    }
  ]
}
```

### Deequ Metrics
```json
{
  "table_name": "employees",
  "metrics": [
    {
      "analyzer": "Completeness(emp_no)",
      "value": "1.0",
      "metric_type": "DoubleMetric"
    },
    {
      "analyzer": "Uniqueness(emp_no)", 
      "value": "1.0",
      "metric_type": "DoubleMetric"
    }
  ]
}
```

### Combined Pipeline Results
```json
{
  "pipeline_type": "combined_deequ_to_ge",
  "expectations_from_deequ": true,
  "total_expectations": 8,
  "successful_expectations": 7,
  "failed_expectations": 1
}
```

## 🛠 Customization Guide

### Adding New Tables

1. **Add CSV file** to `dataset/` directory
2. **Update validation rules**:
   
   **For Great Expectations** (`ge_project/auto_validator.py`):
   ```python
   # Add to expectation_suite_mappings
   'new_table': 'new_table_suite'
   
   # Create new expectation suite in create_expectation_suites()
   new_table_suite = self.context.create_expectation_suite(
       expectation_suite_name="new_table_suite"
   )
   ```

   **For Deequ** (`deequ_project/src/main/scala/DataQualityValidator.scala`):
   ```scala
   // Add case in runValidationChecks()
   case "new_table" =>
     List(
       Check(CheckLevel.Error, "new_table_checks")
         .hasSize(_ > 0)
         .isComplete("key_column")
     )
   ```

### Modifying Validation Rules

**Great Expectations**: Edit expectation definitions in `create_expectation_suites()`
**Deequ**: Modify check definitions in `runValidationChecks()`

### Environment Configuration

**Memory Settings** (for large datasets):
```yaml
# docker-compose.yml
services:
  deequ_service:
    environment:
      - SPARK_DRIVER_MEMORY=4g
      - SPARK_EXECUTOR_MEMORY=4g
```

**Volume Paths**:
```yaml
volumes:
  - ./your-dataset:/app/dataset:ro
  - ./your-output:/app/output
```

## 🐛 Troubleshooting

### Common Issues

**Deequ Container Memory Issues**:
```bash
# Increase Docker Desktop memory to 6GB+
# Or modify docker-compose.yml:
deploy:
  resources:
    limits:
      memory: 4G
```

**Permission Errors**:
```bash
# Fix output directory permissions
sudo chmod 777 output/
# Or run with current user
docker-compose run --user $(id -u):$(id -g) ge_service
```

**Spark Issues**:
```bash
# Check Spark logs
docker-compose logs deequ_service

# Verify Java installation in container
docker-compose exec deequ_service java -version
```

### Debugging Commands

```bash
# View container logs
docker-compose logs ge_service
docker-compose logs deequ_service

# Access running containers
docker-compose exec ge_service bash
docker-compose exec deequ_service bash

# Check output files
docker-compose exec ge_service ls -la /app/output
```

## 📈 Performance Considerations

**Dataset Size Guidelines**:
- **Small** (< 1MB): All validation modes work efficiently
- **Medium** (1MB-100MB): Consider increasing container memory
- **Large** (100MB+): Optimize Spark settings, consider data sampling

**Resource Optimization**:
```yaml
# For large datasets
services:
  deequ_service:
    environment:
      - SPARK_DRIVER_MEMORY=8g
      - SPARK_EXECUTOR_MEMORY=8g
      - SPARK_DRIVER_MAXRESULTSIZE=2g
```

## 🔒 Security Considerations

- Input data is mounted read-only
- Containers run with minimal privileges
- No external network access required
- Results are written to isolated output volumes

## 📚 Additional Resources

- [Great Expectations Documentation](https://greatexpectations.io/)
- [Apache Deequ Documentation](https://github.com/awslabs/deequ)
- [Docker Analysis Report](docs/docker_in_data_pipelines.md)

## 🤝 Contributing

1. Fork the repository
2. Create feature branch (`git checkout -b feature/new-validation`)
3. Commit changes (`git commit -am 'Add new validation rules'`)
4. Push to branch (`git push origin feature/new-validation`)
5. Create Pull Request

## 📄 License

This project is licensed under the MIT License - see the LICENSE file for details.

---

**Built with**: Docker 🐳 | Great Expectations 📊 | Apache Deequ ⚡ | Apache Spark 🔥
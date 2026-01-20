#!/usr/bin/env python3
"""
Great Expectations Auto Validator
Automatically detects CSV files and runs table-specific validation rules.
"""

import os
import json
import pandas as pd
import great_expectations as gx
from great_expectations.core.batch import RuntimeBatchRequest
from great_expectations.checkpoint import SimpleCheckpoint
from pathlib import Path
import logging
from datetime import datetime

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class GEAutoValidator:
    def __init__(self, dataset_path="/app/dataset", output_path="/app/output"):
        self.dataset_path = Path(dataset_path)
        self.output_path = Path(output_path)
        self.context = None
        self.datasource_name = "employee_datasource"
        self.expectation_suite_mappings = {
            'employees': 'employees_suite',
            'salaries': 'salaries_suite', 
            'titles': 'titles_suite',
            'departments': 'departments_suite',
            'dept_emp': 'dept_emp_suite',
            'dept_manager': 'dept_manager_suite'
        }
        
    def initialize_context(self):
        """Initialize Great Expectations context"""
        try:
            # Create context with in-memory configuration
            self.context = gx.get_context(mode="ephemeral")
            logger.info("Great Expectations context initialized successfully")
            
            # Add pandas datasource
            datasource_config = {
                "name": self.datasource_name,
                "class_name": "Datasource",
                "execution_engine": {
                    "class_name": "PandasExecutionEngine"
                },
                "data_connectors": {
                    "default_runtime_data_connector": {
                        "class_name": "RuntimeDataConnector",
                        "batch_identifiers": ["default_identifier_name"]
                    }
                }
            }
            
            self.context.add_datasource(**datasource_config)
            logger.info(f"Added datasource: {self.datasource_name}")
            
        except Exception as e:
            logger.error(f"Failed to initialize context: {e}")
            raise
    
    def create_expectation_suites(self):
        """Create expectation suites for each table type"""
        
        # Employees expectation suite
        employees_suite = self.context.create_expectation_suite(
            expectation_suite_name="employees_suite",
            overwrite_existing=True
        )
        
        employees_expectations = [
            {"expectation_type": "expect_table_columns_to_match_ordered_list", 
             "kwargs": {"column_list": ["emp_no", "birth_date", "first_name", "last_name", "gender", "hire_date"]}},
            {"expectation_type": "expect_column_values_to_be_unique", "kwargs": {"column": "emp_no"}},
            {"expectation_type": "expect_column_values_to_not_be_null", "kwargs": {"column": "emp_no"}},
            {"expectation_type": "expect_column_values_to_not_be_null", "kwargs": {"column": "first_name"}},
            {"expectation_type": "expect_column_values_to_not_be_null", "kwargs": {"column": "last_name"}},
            {"expectation_type": "expect_column_values_to_be_in_set", 
             "kwargs": {"column": "gender", "value_set": ["M", "F"]}},
            {"expectation_type": "expect_column_values_to_be_between", 
             "kwargs": {"column": "emp_no", "min_value": 10001, "max_value": 999999}},
        ]
        
        for exp in employees_expectations:
            employees_suite.add_expectation(**exp)
        
        # Salaries expectation suite
        salaries_suite = self.context.create_expectation_suite(
            expectation_suite_name="salaries_suite",
            overwrite_existing=True
        )
        
        salaries_expectations = [
            {"expectation_type": "expect_table_columns_to_match_ordered_list",
             "kwargs": {"column_list": ["emp_no", "salary", "from_date", "to_date"]}},
            {"expectation_type": "expect_column_values_to_not_be_null", "kwargs": {"column": "emp_no"}},
            {"expectation_type": "expect_column_values_to_not_be_null", "kwargs": {"column": "salary"}},
            {"expectation_type": "expect_column_values_to_be_between",
             "kwargs": {"column": "salary", "min_value": 30000, "max_value": 200000}},
        ]
        
        for exp in salaries_expectations:
            salaries_suite.add_expectation(**exp)
            
        # Titles expectation suite
        titles_suite = self.context.create_expectation_suite(
            expectation_suite_name="titles_suite", 
            overwrite_existing=True
        )
        
        titles_expectations = [
            {"expectation_type": "expect_table_columns_to_match_ordered_list",
             "kwargs": {"column_list": ["emp_no", "title", "from_date", "to_date"]}},
            {"expectation_type": "expect_column_values_to_not_be_null", "kwargs": {"column": "emp_no"}},
            {"expectation_type": "expect_column_values_to_not_be_null", "kwargs": {"column": "title"}},
        ]
        
        for exp in titles_expectations:
            titles_suite.add_expectation(**exp)
            
        # Departments expectation suite  
        departments_suite = self.context.create_expectation_suite(
            expectation_suite_name="departments_suite",
            overwrite_existing=True
        )
        
        departments_expectations = [
            {"expectation_type": "expect_table_columns_to_match_ordered_list",
             "kwargs": {"column_list": ["dept_no", "dept_name"]}},
            {"expectation_type": "expect_column_values_to_be_unique", "kwargs": {"column": "dept_no"}},
            {"expectation_type": "expect_column_values_to_be_unique", "kwargs": {"column": "dept_name"}},
            {"expectation_type": "expect_column_values_to_not_be_null", "kwargs": {"column": "dept_no"}},
            {"expectation_type": "expect_column_values_to_not_be_null", "kwargs": {"column": "dept_name"}},
        ]
        
        for exp in departments_expectations:
            departments_suite.add_expectation(**exp)
            
        # Dept_emp expectation suite
        dept_emp_suite = self.context.create_expectation_suite(
            expectation_suite_name="dept_emp_suite",
            overwrite_existing=True
        )
        
        dept_emp_expectations = [
            {"expectation_type": "expect_table_columns_to_match_ordered_list",
             "kwargs": {"column_list": ["emp_no", "dept_no", "from_date", "to_date"]}},
            {"expectation_type": "expect_column_values_to_not_be_null", "kwargs": {"column": "emp_no"}},
            {"expectation_type": "expect_column_values_to_not_be_null", "kwargs": {"column": "dept_no"}},
        ]
        
        for exp in dept_emp_expectations:
            dept_emp_suite.add_expectation(**exp)
            
        # Dept_manager expectation suite
        dept_manager_suite = self.context.create_expectation_suite(
            expectation_suite_name="dept_manager_suite",
            overwrite_existing=True
        )
        
        dept_manager_expectations = [
            {"expectation_type": "expect_table_columns_to_match_ordered_list",
             "kwargs": {"column_list": ["emp_no", "dept_no", "from_date", "to_date"]}},
            {"expectation_type": "expect_column_values_to_not_be_null", "kwargs": {"column": "emp_no"}},
            {"expectation_type": "expect_column_values_to_not_be_null", "kwargs": {"column": "dept_no"}},
            {"expectation_type": "expect_column_values_to_be_unique", "kwargs": {"column": "emp_no"}},
        ]
        
        for exp in dept_manager_expectations:
            dept_manager_suite.add_expectation(**exp)
            
        logger.info("All expectation suites created successfully")
    
    def detect_table_name(self, filename):
        """Extract table name from CSV filename"""
        table_name = filename.replace('.csv', '').lower()
        return table_name
    
    def validate_file(self, file_path):
        """Validate a single CSV file using appropriate expectation suite"""
        filename = file_path.name
        table_name = self.detect_table_name(filename)
        
        logger.info(f"Processing file: {filename}, detected table: {table_name}")
        
        # Check if we have an expectation suite for this table
        suite_name = self.expectation_suite_mappings.get(table_name)
        if not suite_name:
            logger.warning(f"No expectation suite found for table: {table_name}")
            return None
            
        try:
            # Read the CSV file
            df = pd.read_csv(file_path)
            logger.info(f"Loaded {len(df)} rows from {filename}")
            
            # Create runtime batch request
            batch_request = RuntimeBatchRequest(
                datasource_name=self.datasource_name,
                data_connector_name="default_runtime_data_connector",
                data_asset_name=table_name,
                runtime_parameters={"batch_data": df},
                batch_identifiers={"default_identifier_name": table_name}
            )
            
            # Create and run checkpoint
            checkpoint_config = {
                "name": f"{table_name}_checkpoint",
                "config_version": 1.0,
                "template_name": None,
                "module_name": "great_expectations.checkpoint",
                "class_name": "SimpleCheckpoint",
                "run_name_template": f"{table_name}_%Y%m%d-%H%M%S",
                "expectation_suite_name": suite_name,
                "batch_request": batch_request,
                "action_list": [
                    {
                        "name": "store_validation_result",
                        "action": {
                            "class_name": "StoreValidationResultAction"
                        }
                    },
                    {
                        "name": "update_data_docs",
                        "action": {
                            "class_name": "UpdateDataDocsAction"
                        }
                    }
                ]
            }
            
            checkpoint = SimpleCheckpoint(
                name=f"{table_name}_checkpoint",
                data_context=self.context,
                **checkpoint_config
            )
            
            # Run validation
            result = checkpoint.run()
            
            # Save results
            self.save_validation_results(table_name, result, df.shape)
            
            logger.info(f"Validation completed for {filename}")
            return result
            
        except Exception as e:
            logger.error(f"Error validating {filename}: {e}")
            return None
    
    def save_validation_results(self, table_name, result, data_shape):
        """Save validation results to output directory"""
        try:
            # Create output directory if it doesn't exist
            self.output_path.mkdir(parents=True, exist_ok=True)
            
            # Extract validation results
            validation_result = result.list_validation_results()[0]
            success = validation_result.success
            
            # Create summary
            summary = {
                "table_name": table_name,
                "timestamp": datetime.now().isoformat(),
                "data_shape": {"rows": data_shape[0], "columns": data_shape[1]},
                "validation_success": success,
                "results": []
            }
            
            # Process each expectation result
            for result_obj in validation_result.results:
                expectation_result = {
                    "expectation_type": result_obj.expectation_config.expectation_type,
                    "success": result_obj.success,
                    "kwargs": dict(result_obj.expectation_config.kwargs),
                    "result": result_obj.result
                }
                summary["results"].append(expectation_result)
            
            # Save JSON summary
            json_file = self.output_path / f"{table_name}_validation_results.json"
            with open(json_file, 'w') as f:
                json.dump(summary, f, indent=2, default=str)
            
            logger.info(f"Results saved to {json_file}")
            
        except Exception as e:
            logger.error(f"Error saving results for {table_name}: {e}")
    
    def run_validation(self):
        """Main method to run validation on all CSV files"""
        logger.info("Starting Great Expectations validation")
        
        # Initialize context and create expectation suites
        self.initialize_context()
        self.create_expectation_suites()
        
        # Find all CSV files
        csv_files = list(self.dataset_path.glob("*.csv"))
        
        if not csv_files:
            logger.warning(f"No CSV files found in {self.dataset_path}")
            return
        
        logger.info(f"Found {len(csv_files)} CSV files to process")
        
        # Validate each file
        results = {}
        for csv_file in csv_files:
            result = self.validate_file(csv_file)
            if result:
                results[csv_file.name] = result
        
        # Generate data docs
        try:
            self.context.build_data_docs()
            logger.info("Data docs built successfully")
        except Exception as e:
            logger.error(f"Error building data docs: {e}")
        
        logger.info("Validation completed for all files")
        return results

def main():
    """Main entry point"""
    validator = GEAutoValidator()
    validator.run_validation()

if __name__ == "__main__":
    main()
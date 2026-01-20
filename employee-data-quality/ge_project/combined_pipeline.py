#!/usr/bin/env python3
"""
Combined Pipeline: Deequ Metrics → Great Expectations Auto-Generated Expectations
Uses Deequ metrics to dynamically create GE expectations and re-run validation.
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

class CombinedPipeline:
    def __init__(self, dataset_path="/app/dataset", output_path="/app/output"):
        self.dataset_path = Path(dataset_path)
        self.output_path = Path(output_path)
        self.deequ_results_path = Path(output_path) / "deequ_results"
        self.ge_results_path = Path(output_path) / "ge_results" 
        self.context = None
        self.datasource_name = "combined_datasource"
        
    def initialize_context(self):
        """Initialize Great Expectations context"""
        try:
            self.context = gx.get_context(mode="ephemeral")
            logger.info("Great Expectations context initialized for combined pipeline")
            
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
    
    def load_deequ_metrics(self, table_name):
        """Load Deequ metrics from JSON file"""
        metrics_file = self.deequ_results_path / f"{table_name}_deequ_metrics.json"
        
        if not metrics_file.exists():
            logger.warning(f"Deequ metrics file not found: {metrics_file}")
            return None
            
        try:
            with open(metrics_file, 'r') as f:
                metrics_data = json.load(f)
            logger.info(f"Loaded Deequ metrics for {table_name}")
            return metrics_data
        except Exception as e:
            logger.error(f"Error loading Deequ metrics for {table_name}: {e}")
            return None
    
    def generate_expectations_from_metrics(self, table_name, metrics_data, df):
        """Generate GE expectations based on Deequ metrics"""
        
        suite_name = f"{table_name}_auto_generated_suite"
        
        # Create new expectation suite
        suite = self.context.create_expectation_suite(
            expectation_suite_name=suite_name,
            overwrite_existing=True
        )
        
        logger.info(f"Generating expectations for {table_name} based on Deequ metrics")
        
        # Parse metrics and create expectations
        expectations_added = 0
        
        for metric in metrics_data.get("metrics", []):
            analyzer = metric.get("analyzer", "")
            value = metric.get("value", "")
            
            try:
                # Size expectation
                if "Size()" in analyzer:
                    row_count = int(float(value))
                    suite.add_expectation({
                        "expectation_type": "expect_table_row_count_to_be_between",
                        "kwargs": {
                            "min_value": max(0, int(row_count * 0.9)),  # Allow 10% variance
                            "max_value": int(row_count * 1.1)
                        }
                    })
                    expectations_added += 1
                
                # Completeness expectations
                elif "Completeness(" in analyzer and "Completeness(*)" not in analyzer:
                    column_name = analyzer.split("Completeness(")[1].split(")")[0]
                    completeness_ratio = float(value)
                    
                    if completeness_ratio >= 0.95:  # High completeness
                        suite.add_expectation({
                            "expectation_type": "expect_column_values_to_not_be_null",
                            "kwargs": {"column": column_name}
                        })
                        expectations_added += 1
                    elif completeness_ratio >= 0.5:  # Moderate completeness
                        suite.add_expectation({
                            "expectation_type": "expect_column_values_to_not_be_null",
                            "kwargs": {
                                "column": column_name,
                                "mostly": completeness_ratio * 0.9  # Allow some tolerance
                            }
                        })
                        expectations_added += 1
                
                # Uniqueness expectations
                elif "Uniqueness(" in analyzer:
                    column_name = analyzer.split("Uniqueness(")[1].split(")")[0]
                    uniqueness_ratio = float(value)
                    
                    if uniqueness_ratio >= 0.99:  # High uniqueness suggests unique column
                        suite.add_expectation({
                            "expectation_type": "expect_column_values_to_be_unique",
                            "kwargs": {"column": column_name}
                        })
                        expectations_added += 1
                
                # Minimum/Maximum value expectations for numeric columns
                elif "Minimum(" in analyzer:
                    column_name = analyzer.split("Minimum(")[1].split(")")[0]
                    min_value = float(value)
                    
                    # Add to a temporary store for later combination with maximum
                    if not hasattr(self, '_min_max_store'):
                        self._min_max_store = {}
                    if column_name not in self._min_max_store:
                        self._min_max_store[column_name] = {}
                    self._min_max_store[column_name]['min'] = min_value
                
                elif "Maximum(" in analyzer:
                    column_name = analyzer.split("Maximum(")[1].split(")")[0]
                    max_value = float(value)
                    
                    if not hasattr(self, '_min_max_store'):
                        self._min_max_store = {}
                    if column_name not in self._min_max_store:
                        self._min_max_store[column_name] = {}
                    self._min_max_store[column_name]['max'] = max_value
                
                # String length expectations
                elif "MinLength(" in analyzer:
                    column_name = analyzer.split("MinLength(")[1].split(")")[0]
                    min_length = int(float(value))
                    
                    if min_length > 0:
                        suite.add_expectation({
                            "expectation_type": "expect_column_value_lengths_to_be_between",
                            "kwargs": {
                                "column": column_name,
                                "min_value": min_length,
                                "max_value": None
                            }
                        })
                        expectations_added += 1
                        
            except Exception as e:
                logger.warning(f"Error processing metric {analyzer}: {e}")
                continue
        
        # Add min/max range expectations
        if hasattr(self, '_min_max_store'):
            for column_name, bounds in self._min_max_store.items():
                if 'min' in bounds and 'max' in bounds:
                    suite.add_expectation({
                        "expectation_type": "expect_column_values_to_be_between",
                        "kwargs": {
                            "column": column_name,
                            "min_value": bounds['min'],
                            "max_value": bounds['max']
                        }
                    })
                    expectations_added += 1
        
        # Add basic structural expectations
        suite.add_expectation({
            "expectation_type": "expect_table_columns_to_match_ordered_list",
            "kwargs": {"column_list": list(df.columns)}
        })
        expectations_added += 1
        
        logger.info(f"Generated {expectations_added} expectations for {table_name}")
        return suite_name
    
    def validate_with_generated_expectations(self, table_name, suite_name, df):
        """Run validation using auto-generated expectations"""
        try:
            # Create runtime batch request
            batch_request = RuntimeBatchRequest(
                datasource_name=self.datasource_name,
                data_connector_name="default_runtime_data_connector",
                data_asset_name=table_name,
                runtime_parameters={"batch_data": df},
                batch_identifiers={"default_identifier_name": f"{table_name}_combined"}
            )
            
            # Create and run checkpoint
            checkpoint_config = {
                "name": f"{table_name}_combined_checkpoint",
                "config_version": 1.0,
                "template_name": None,
                "module_name": "great_expectations.checkpoint",
                "class_name": "SimpleCheckpoint",
                "run_name_template": f"{table_name}_combined_%Y%m%d-%H%M%S",
                "expectation_suite_name": suite_name,
                "batch_request": batch_request,
                "action_list": [
                    {
                        "name": "store_validation_result",
                        "action": {
                            "class_name": "StoreValidationResultAction"
                        }
                    }
                ]
            }
            
            checkpoint = SimpleCheckpoint(
                name=f"{table_name}_combined_checkpoint",
                data_context=self.context,
                **checkpoint_config
            )
            
            # Run validation
            result = checkpoint.run()
            
            # Save results
            self.save_combined_results(table_name, result, df.shape)
            
            logger.info(f"Combined validation completed for {table_name}")
            return result
            
        except Exception as e:
            logger.error(f"Error in combined validation for {table_name}: {e}")
            return None
    
    def save_combined_results(self, table_name, result, data_shape):
        """Save combined validation results"""
        try:
            # Create output directory
            self.ge_results_path.mkdir(parents=True, exist_ok=True)
            
            # Extract validation results
            validation_result = result.list_validation_results()[0]
            success = validation_result.success
            
            # Create summary
            summary = {
                "table_name": table_name,
                "pipeline_type": "combined_deequ_to_ge",
                "timestamp": datetime.now().isoformat(),
                "data_shape": {"rows": data_shape[0], "columns": data_shape[1]},
                "validation_success": success,
                "expectations_from_deequ": True,
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
            json_file = self.ge_results_path / f"{table_name}_combined_validation_results.json"
            with open(json_file, 'w') as f:
                json.dump(summary, f, indent=2, default=str)
            
            logger.info(f"Combined results saved to {json_file}")
            
        except Exception as e:
            logger.error(f"Error saving combined results for {table_name}: {e}")
    
    def process_table(self, csv_file):
        """Process a single table through the combined pipeline"""
        filename = csv_file.name
        table_name = filename.replace('.csv', '').lower()
        
        logger.info(f"Processing {table_name} in combined pipeline")
        
        try:
            # Load CSV data
            df = pd.read_csv(csv_file)
            logger.info(f"Loaded {len(df)} rows from {filename}")
            
            # Load Deequ metrics
            metrics_data = self.load_deequ_metrics(table_name)
            if not metrics_data:
                logger.warning(f"Skipping {table_name} - no Deequ metrics found")
                return None
            
            # Generate expectations from Deequ metrics
            suite_name = self.generate_expectations_from_metrics(table_name, metrics_data, df)
            
            # Run validation with generated expectations
            result = self.validate_with_generated_expectations(table_name, suite_name, df)
            
            return result
            
        except Exception as e:
            logger.error(f"Error processing {table_name} in combined pipeline: {e}")
            return None
    
    def run_combined_pipeline(self):
        """Main method to run the combined pipeline"""
        logger.info("Starting combined Deequ → Great Expectations pipeline")
        
        # Wait for Deequ results to be available
        logger.info("Checking for Deequ results...")
        if not self.deequ_results_path.exists():
            logger.error("Deequ results directory not found. Run Deequ validation first.")
            return
        
        # Initialize GE context
        self.initialize_context()
        
        # Find all CSV files
        csv_files = list(self.dataset_path.glob("*.csv"))
        if not csv_files:
            logger.warning(f"No CSV files found in {self.dataset_path}")
            return
        
        logger.info(f"Found {len(csv_files)} CSV files to process in combined pipeline")
        
        # Process each file
        results = {}
        for csv_file in csv_files:
            result = self.process_table(csv_file)
            if result:
                results[csv_file.name] = result
        
        # Generate summary report
        self.generate_pipeline_summary(results)
        
        logger.info("Combined pipeline completed successfully")
        return results
    
    def generate_pipeline_summary(self, results):
        """Generate a summary report of the combined pipeline"""
        try:
            summary = {
                "pipeline_type": "combined_deequ_to_ge",
                "timestamp": datetime.now().isoformat(),
                "total_tables_processed": len(results),
                "table_results": {}
            }
            
            for filename, result in results.items():
                table_name = filename.replace('.csv', '').lower()
                validation_result = result.list_validation_results()[0]
                
                summary["table_results"][table_name] = {
                    "validation_success": validation_result.success,
                    "total_expectations": len(validation_result.results),
                    "successful_expectations": sum(1 for r in validation_result.results if r.success),
                    "failed_expectations": sum(1 for r in validation_result.results if not r.success)
                }
            
            # Save summary
            summary_file = self.output_path / "combined_pipeline_summary.json"
            with open(summary_file, 'w') as f:
                json.dump(summary, f, indent=2, default=str)
            
            logger.info(f"Pipeline summary saved to {summary_file}")
            
        except Exception as e:
            logger.error(f"Error generating pipeline summary: {e}")

def main():
    """Main entry point for combined pipeline"""
    pipeline = CombinedPipeline()
    pipeline.run_combined_pipeline()

if __name__ == "__main__":
    main()
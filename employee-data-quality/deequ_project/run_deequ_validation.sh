#!/bin/bash

echo "Starting Deequ validation process..."

# Set environment variables
export SPARK_HOME=/opt/spark
export PATH=$SPARK_HOME/bin:$PATH

# Create output directory
mkdir -p /app/output

# Compile Scala code
echo "Compiling Scala code..."
cd /app/deequ_project

# Use spark-submit to run the application with Deequ jar
echo "Running Deequ validation..."
spark-submit \
  --class DataQualityValidator \
  --master local[*] \
  --jars /app/lib/deequ.jar \
  --conf spark.sql.adaptive.enabled=false \
  --conf spark.sql.adaptive.coalescePartitions.enabled=false \
  /app/deequ_project/target/scala-2.12/employee-data-quality-deequ_2.12-1.0.jar

echo "Deequ validation completed."
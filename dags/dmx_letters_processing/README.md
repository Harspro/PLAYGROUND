# DMX Letters Processing

## Overview

The DMX Letters Processing module provides an Airflow DAG builder for processing DMX RRDF files containing customer letter printing data. This pipeline transforms JSON-formatted input files into structured data and loads them into the BigQuery MAIL_PRINT table for downstream processing by printing and mailing systems.

*Note: This is a temporary solution.*

## Key Features

- **End-to-End Processing**: Complete pipeline from raw DMX RRDF files to BigQuery tables
- **Spark-Based Transformation**: Leverages Google Dataproc clusters for scalable data processing
- **BigQuery Integration**: Seamless loading with schema transformations and column operations
- **Configuration-Driven**: YAML-based configuration for flexible deployment across environments
- **Unique Run Isolation**: Prevents data conflicts through unique folder paths per DAG run
- **Robust Error Handling**: Comprehensive exception handling with proper Airflow task failures


## Configuration

### File Structure
```
dmx_letters_processing/
├── dmx_letters_processing_dag.py    # Main DAG builder implementation
├── dmx_letters_config.yaml          # Configuration file
├── folder_trigger_config.yaml       # Folder trigger configuration file
└── README.md                        # This documentation
```

## Usage

### DAG Triggering

The DAG is designed to be triggered automatically when a file is dropped in the specified bucket and folder mentioned in `folder_trigger_config.yaml`

## Data Processing Details

### Input Data Format
- **Source**: DMX RRDF JSON files containing letter printing instructions
- **Location**: Configurable GCS bucket and folder path
- **Format**: JSON with letter metadata and printing parameters

### Output Data Schema
The processed data is loaded into the `domain_communication.MAIL_PRINT` table with the following transformations:
- **source**: Static value 'DMX RRDF'
- **mailType**: Static value 'LETTER'
- **printingMode**: Static value 'SIMPLEX'
- **parameters**: Parsed JSON from parametersString field
- **timestamp**: Current timestamp in America/Toronto timezone
- **parametersString**: Dropped after transformation

### Processing Steps
1. **File Parsing**: Spark job reads and validates JSON input files
2. **Data Transformation**: Maps JSON fields to target schema
3. **Parquet Generation**: Outputs structured Parquet files to staging GCS bucket
4. **BigQuery Loading**: Creates external table and inserts data with transformations

## Dependencies

### Google Cloud Services
- **Dataproc**: For Spark cluster provisioning and job execution
- **Cloud Storage**: For intermediate file storage and data staging
- **BigQuery**: For final data storage and querying

### Spark Application
- **JAR File**: `dmx-letters-spark-batch-2.0.0.jar`
- **Main Class**: `com.pcb.batch.spark.dmx.letters.DmxLettersSparkBatchApplication`
- **Processing**: JSON parsing, data mapping, and Parquet output generation

### Airflow Utilities
- `util.bq_utils`: BigQuery operations (external tables, column transformations)
- `util.miscutils`: Cluster management and configuration utilities
- `util.logging_utils`: Spark logging configuration
- `dag_factory`: Dynamic DAG creation framework

## Support and Contacts

- **Team**: Team Telegraphers Alerts
- **Capability**: Communication
- **Sub-Capability**: Mail Processing
- **Severity**: P2 (High Priority)
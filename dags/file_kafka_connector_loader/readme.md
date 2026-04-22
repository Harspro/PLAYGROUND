# File Kafka Connector Loader

The file kafka connector loader is responsible for utilizing configuration files to dynamically create DAGs responsible
for publishing the contents of files within Terminus to DP Kafka topics. Currently, we're able to support two source 
file formats:

- CSV 
- PARQUET

Along with, having the ability to publish to the following Kafka schema types:
- AVRO
- JSON

The DAGS are created with the following tasks (run in order):
1. Start task - Dummy Operator. 
2. Dataproc cluster creation, unique to each dag name.
3. Dataproc submit job operator, submits dataproc job based on configuration provided.
4. End task - Dummy Operator.

When BQ is the source, the DAGS are created with the following tasks (run in order):
1. Start task - Dummy Operator.
2. Dataproc cluster creation, unique to each dag name.
3. Delete GCS processing objects in staging folder.
4. Execute BigQuery query to extract records and create parquet staging files. 
5. Dataproc submit job operator, submits dataproc job based on configuration provided. 
6. End task - Dummy Operator.

Lastly, during execution, the kafka writer dags are typically run as part of a separate proceeding dag. To do so, please 
ensure to use the TriggerDagRunOperator in that dag.

## Getting Started
Before creating configuration files, with the aim of using the file-kafka-connector, follow these steps:

1. Ensure the necessary dependencies are setup:
    1. Kafka topic is created on DP.
    2. Kafka topic schema is available within the schema registry.
    3. Kafka Service Account role binding is completed.
    4. Desired storage bucket is available on GCP.
2. Ensure all spark configuration files and items are in place.

## Usage
To use the file kafka connector loader, configuration files will need to be provided with the following:

**Default args**
- Common default args required by other dags: owner, capability, severity, sub capability, business impact, and 
customer impact.

**File Uris**
- File uris of spark configuration files, will include the following:
  - application.yaml (required): includes details on topic, producer properties, vault, etc.
  - schema_mapping.yaml (optional): mapping field names from file to topic.
  - _key.avsc (required typically): schema of kafka topic key.
  - _value.avsc (required): schema of kafka topic message.
  - file_schema.avsc (optional but highly encouraged): an avro representation of file schema.

**Arguments**
- Dataproc spark arguments which can be passed in as part of running the job:
  - **pcb.file.kafka.connector.avro.key.schema.path**: OPTIONAL  - Avro key schema file path under the resource's folder.
  - **pcb.file.kafka.connector.avro.value.schema.path**: OPTIONAL - Avro value schema file path under the resource's folder.
  - **pcb.file.kafka.connector.csv.comment.character**: OPTIONAL - CSV comment character, sets the single character used for skipping lines beginning with this character. By default, it is disabled.
  - **pcb.file.kafka.connector.csv.delimiter**: OPTIONAL - CSV delimiter to use when reading csv files.
  - **pcb.file.kafka.connector.csv.header**: OPTIONAL - CSV header, whether the source file contains a header or not.
  - **pcb.file.kafka.connector.csv.ignore.leading.white.space**: OPTIONAL - defines whether leading whitespaces from values being read should be skipped.
  - **pcb.file.kafka.connector.csv.ignore.trailing.white.space**: OPTIONAL - defines whether trailing whitespaces from values being read should be skipped.
  - **pcb.file.kafka.connector.dataset.reparation.count**: OPTIONAL - Reparation count on dataset, the number of new partitions the dataset will be broken down into.
  - **pcb.file.kafka.connector.dataset.reparation.criteria**: OPTIONAL - Criteria to reparation dataset on.
  - **pcb.file.kafka.connector.drop.array.source.fields**: Optional - Drop source fields used in the creation of array field/s.
  - **pcb.file.kafka.connector.executor.row.limit**: OPTIONAL - Limit of rows to load into executor memory at any given time for each partition.
  - **pcb.file.kafka.connector.file.kafka.schema.mapping.path**: OPTIONAL - File Kafka Schema mapping path, path to schema mapping file used for mapping dataset columns to kafka schema columns.
  - **pcb.file.kafka.connector.file.type**: REQUIRED - Type of file to read data from.
  - **pcb.file.kafka.connector.file.path**: REQUIRED - Location of data file.
  - **pcb.file.kafka.connector.filtered.fields**: OPTIONAL - Filter data file to only include these fields.
  - **pcb.file.kafka.connector.filtered.records.criteria**: OPTIONAL - Filter data from file to match that of the provided criteria.
  - **pcb.file.kafka.connector.group.by.fields**: OPTIONAL - Group by fields to use when grouping the dataset.
  - **pcb.file.kafka.connector.kafka.key**: OPTIONAL - Kafka key to use when publishing records.
  - **pcb.file.kafka.connector.kafka.key.part.of.dataset**: OPTIONAL - Is the kafka key part of the contents within the file. Expecting true or false here, enforced downstream.
  - **pcb.file.kafka.connector.pivot.field**: OPTIONAL - Pivot field to use when pivoting the dataset.
  - **pcb.file.kafka.connector.result.array.field.name**: OPTIONAL - Result array field name/s. 
  - **pcb.file.kafka.connector.schema.file.path**: OPTIONAL - Schema file path, path to schema used in data file.
  - **pcb.file.kafka.connector.source.array.fields**: OPTIONAL - Source fields to use when creating an array field.
  - **pcb.file.kafka.connector.source.array.fields.keyword**: OPTIONAL - Keyword to look for in source fields when creating an array field/s.
  - **pcb.file.kafka.connector.struct.to.map.field**: OPTIONAL - Struct field to convert to a Map field.
  - **pcb.file.kafka.connector.timestamp.format**: OPTIONAL - Timestamp format to be used in source data file.
  - **pcb.file.kafka.connector.transpose.fields.suffix**: OPTIONAL - Suffix for transpose fields.
  - **pcb.file.kafka.connector.transpose.join.type**: OPTIONAL - Join type to use if joining several transposed datasets.

**Dag**
- Dag related information:
  - key: kafka_writer_task - name to use for kafka writer task (submitting dataproc job).
  - key: read_pause_deploy_config - whether to pause dag in any given environment.
  - key: job_size - cluster size (in the scenario where the dataproc cluster is not available).

If needed, feel free to also reference any existing configuration in the **file_kafka_connector_writer_configs** folder.

## Example
Here's an example of a configuration file that could be created:
<pre>
glp_pcl_lcl_merch_list_kafka_writer:
  default_args:
    owner: "team-convergence-alerts"
    capability: "Loyalty"
    severity: "P2"
    sub_capability: "Rewards/Loyalty"
    business_impact: "This will impact the loading of LCL/SDM merchants into DP Kafka topic: dp-core-{env}-global-payment-merchant. Campaign service merchant list will be out of sync and all new LCL/ SDM merchants will not be updated in campaign service."
    customer_impact: "All transactions in new LCL/SDM stores will be considered out of network store transaction. Customer will have a lower points earn rate."
  file_uris:
    - gs://pcb-{env}-staging-artifacts/resources/file-kafka-connector/loyalty/global_payment_merchant/application.yaml
    - gs://pcb-{env}-staging-artifacts/resources/file-kafka-connector/loyalty/global_payment_merchant/schema_mapping.yaml
    - gs://pcb-{env}-staging-artifacts/resources/file-kafka-connector/loyalty/global_payment_merchant/global_payment_merchant_key.avsc
    - gs://pcb-{env}-staging-artifacts/resources/file-kafka-connector/loyalty/global_payment_merchant/global_payment_merchant_value.avsc
    - gs://pcb-{env}-staging-artifacts/resources/file-kafka-connector/loyalty/global_payment_merchant/glp_pcl_lcl_merch_list_file_schema.avsc
  application_config: application.yaml
  args:
    pcb.file.kafka.connector.avro.key.schema.path: global_payment_merchant_key.avsc
    pcb.file.kafka.connector.avro.value.schema.path: global_payment_merchant_value.avsc
    pcb.file.kafka.connector.csv.delimiter: '|'
    pcb.file.kafka.connector.csv.ignore.leading.white.space: true
    pcb.file.kafka.connector.csv.ignore.trailing.white.space: true
    pcb.file.kafka.connector.file.kafka.schema.mapping.path: schema_mapping.yaml
    pcb.file.kafka.connector.file.type: CSV
    pcb.file.kafka.connector.file.path:
      file_path_from_preceding_dag: true
    pcb.file.kafka.connector.kafka.key: merchantId
    pcb.file.kafka.connector.kafka.key.part.of.dataset: true
    pcb.file.kafka.connector.schema.file.path: glp_pcl_lcl_merch_list_file_schema.avsc
  dag:
    kafka_writer_task: kafka_writer_glp_pcl_lcl_merch_list
    read_pause_deploy_config: True
    job_size: extra_small
</pre>

## Contributing
Contributions to the file kafka writer class are welcome.
If you find any issues or have suggestions for improvements, please submit a pull request or open an issue.

## Additional Info:
Feel free to customize the README file to include additional sections or any other relevant information.

Author: [Sharif Mansour]
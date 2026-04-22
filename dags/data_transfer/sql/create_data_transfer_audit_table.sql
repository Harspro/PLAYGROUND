CREATE TABLE IF NOT EXISTS `pcb-{env}-landing.domain_technical.DATA_TRANSFER_AUDIT`(
    Source_Project_Id STRING,
    Source_Dag_Id STRING,
    Source_Dag_Run_Id STRING,
    Source_Type STRING,
    Source_Location STRING,
    Filename STRING,
    Destination_Project_Id STRING,
    Destination_Location STRING,
    Processing_Dag_Id STRING,
    Processing_Dag_Run_Id STRING,
    Execution_Type STRING,
    Trigger_Type STRING,
    Requested_By STRING,
    Insert_Time TIMESTAMP,
    Status STRING
)

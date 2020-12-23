importing Glue operator
```python
from airflow.operators.glue_plugin import AWSGlueJobOperator
```


starting using it
```python
csv_to_parquet = AWSGlueJobOperator(
    task_id='test_glue_operator_with_new_airflow_task',
    job_name='test_glue_operator_with_new_airflow',
    aws_conn_id='aws_default',
    region_name='eu-west-1',
    script_location=f"s3://bucket-name-xxx/test_spark.py",
    script_arguments={},
    glue_version='2.0',
    time_out=10,
    worker_type='Standard',
    max_capacity=2,
    job_command_type='glueetl',
    iam_role_name='role-name-xxx',
    trigger_rule=TriggerRule.ALL_DONE,
    dag=main_dag,
)
```

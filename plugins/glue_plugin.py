from airflow.plugins_manager import AirflowPlugin
from hooks.aws_glue_job_hook import *
from operators.glue_job_operator import *


class GluePlugin(AirflowPlugin):
    name = 'glue_plugin'

    hooks = [AwsGlueJobHook]
    operators = [AWSGlueJobOperator]
    #sensors = [MySensor]
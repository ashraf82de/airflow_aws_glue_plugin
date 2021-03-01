from __future__ import unicode_literals
from hooks.aws_glue_job_hook import AwsGlueJobHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class AWSGlueJobOperator(BaseOperator):

    template_fields = ()
    template_ext = ()
    ui_color = '#ededed'

    """
    https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/glue.html#Glue.Client.create_job
    read in this link create_job function to know about the parameters
    """
    @apply_defaults
    def __init__(self,
                 region_name: str,
                 job_name: str,
                 iam_role_name: str,
                 script_location: str,
                 script_arguments: dict = {},
                 aws_conn_id: str = 'aws_default',
                 glue_version: str = '2.0',
                 time_out: int = 60,
                 worker_type: str = 'Standard',
                 max_capacity: float = 2,
                 job_command_type: str = 'glueetl',
                 bookmark_enabled: bool = False,
                 connections: list = [],
                 *args, **kwargs
                 ):
        super(AWSGlueJobOperator, self).__init__(*args, **kwargs)
        self.job_name = job_name
        self.script_location = script_location
        self.script_arguments = script_arguments
        self.region_name = region_name
        self.iam_role_name = iam_role_name
        self.glue_version = glue_version
        self.time_out = time_out
        self.aws_conn_id = aws_conn_id
        self.worker_type = worker_type
        self.max_capacity = max_capacity
        self.job_command_type = job_command_type
        self.bookmark_enabled = bookmark_enabled
        self.connections = connections

    def execute(self, context):
        """
        Executes AWS Glue Job from Airflow
        :return:
        """
        glue_job = AwsGlueJobHook(job_name=self.job_name,
                                  script_location=self.script_location,
                                  region_name=self.region_name,
                                  script_arguments=self.script_arguments,
                                  iam_role_name=self.iam_role_name,
                                  aws_conn_id=self.aws_conn_id,
                                  glue_version=self.glue_version,
                                  time_out=self.time_out,
                                  worker_type=self.worker_type,
                                  max_capacity=self.max_capacity,
                                  job_command_type=self.job_command_type,
                                  bookmark_enabled=self.bookmark_enabled,
                                  connections=self.connections
                                  )

        self.log.info("Initializing AWS Glue Job: {}".format(self.job_name))
        glue_job.start_job()
        self.log.info('Done.')



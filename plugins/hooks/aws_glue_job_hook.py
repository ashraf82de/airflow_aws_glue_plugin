from airflow.contrib.hooks.aws_hook import AwsHook
import time


class AwsGlueJobHook(AwsHook):

    def __init__(self,
                 region_name: str,
                 job_name: str,
                 iam_role_name: str,
                 script_location: str,
                 script_arguments: str,
                 aws_conn_id: str = 'aws_default',
                 glue_version: str = '2.0',
                 time_out: int = 60,  # The job timeout in minute
                 worker_type: str = 'Standard',  # 'Standard' | 'G.1X' | 'G.2X'
                 max_capacity: float = 2,
                 job_command_type: str = 'glueetl', # 'pythonshell' | 'gluestreaming' | 'glueetl'
                 bookmark_enabled: bool = False,
                 connections: list = [],
                 *args,
                 **kwargs):
        super().__init__(aws_conn_id=aws_conn_id, *args, **kwargs)

        self.job_name = job_name
        self.role = iam_role_name
        self.script_location = script_location
        self.script_arguments = script_arguments
        self.glue = self.get_client_type('glue',
                                         region_name=region_name)  # self.glue = boto3.client(service_name='glue', region_name=region_name, endpoint_url=f"https://glue.{region_name}.amazonaws.com")
        self.glue_version = glue_version
        self.time_out = time_out
        self.worker_type = worker_type
        self.max_capacity = max_capacity
        self.job_command_type = job_command_type
        self.bookmark_enabled= bookmark_enabled
        self.connections = connections

    def create_job(self):
        """
        check if the glue job does not exists then create a new one
        :return:
        """
        try:
            self.glue.get_job(
                JobName=self.job_name
            )
        except:
            bookmark_option = 'job-bookmark-disable'
            if self.bookmark_enabled:
                bookmark_option = 'job-bookmark-enable'

            self.script_arguments['--job-bookmark-option'] = bookmark_option
            self.glue.create_job(Name=self.job_name, Role=self.role, GlueVersion=self.glue_version,
                                 Command={'Name': self.job_command_type,
                                          'ScriptLocation': self.script_location,
                                          'PythonVersion': "3"
                                          },
                                 Timeout=self.time_out,
                                 WorkerType=self.worker_type,
                                 NumberOfWorkers=self.max_capacity,
                                 DefaultArguments=self.script_arguments,
                                 Connections={'Connections': self.connections}
                                 )

    def start_job(self):
        # check if a job exists with that name or create it
        self.create_job()

        # run the job
        running_job = self.glue.start_job_run(JobName=self.job_name, Arguments=self.script_arguments)

        # wait for the job to finish
        while not self.is_job_stopped(running_job['JobRunId']):
            time.sleep(5)

    def get_job_status(self, job_id: str):
        status = self.glue.get_job_run(JobName=self.job_name, RunId=job_id)
        self.log.info(f"Job {job_id} status is {status}")

        return status['JobRun']['JobRunState']

    def is_job_stopped(self, job_id: str):
        status = self.get_job_status(job_id)

        if status == 'FAILED' or status == 'TIMEOUT':
            raise ValueError(f" Job {job_id} failed")

        return status == 'STOPPED' or status == 'SUCCEEDED'


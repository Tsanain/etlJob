from aws_cdk import (
    
    Stack,
    aws_s3 as s3,
    aws_iam as iam,
    aws_glue as glue,

)
from constructs import Construct

class EtlJobStack(Stack):

    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        bucket_dion = s3.Bucket.from_bucket_arn(self, id = "tsan-data-bucket", bucket_arn="arn:aws:s3:::tsan-bucket-trial")

        role = iam.Role(self, "Glue-role", assumed_by=iam.ServicePrincipal("glue.amazonaws.com"))
        role.add_managed_policy(iam.ManagedPolicy.from_aws_managed_policy_name("service-role/AWSGlueServiceRole"))

        PYTHON_VERSION = "3"
        COMMAND_NAME = "pythonshell"
        jobName = 'etljob'

        etljob = glue.CfnJob(self,id=jobName,name=jobName,role=role.role_arn,
                             max_capacity=1,
                             command=glue.CfnJob.JobCommandProperty(
                             name=COMMAND_NAME,
                             python_version=PYTHON_VERSION,
                             script_location="s3://bucket-1-tsan/Scripts/job.py"),
                             )        

            
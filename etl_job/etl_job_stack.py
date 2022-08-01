from aws_cdk import (

    Stack,
    aws_s3 as s3,
    aws_iam as iam,
    aws_glue as glue,

)
from constructs import Construct
import boto3

s3_client = boto3.client('s3')


class EtlJobStack(Stack):

    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)


        bucket_src = s3.Bucket.from_bucket_arn(
            self, id="tsan-etljob-src", bucket_arn="arn:aws:s3:::tsan-etljob-src")

        dest_bucket = s3.Bucket(
            self, id="tsan-eltJob-dest", bucket_name="tsan-etljob-dest", versioned=False, block_public_access=s3.BlockPublicAccess.BLOCK_ALL) #if resource creation fails for s3:PutObject, delete the s3 bucket, remove the parameter "block_public_access", run cdk deploy, then add "block_public_access" to code and run cdk deploy, it does not work at once as blocking public access blocks the permission to edit bucket policy.

        policy_doc = iam.PolicyStatement(actions=[
                        "s3:GetObject",
                        "s3:GetObjectVersion",
                        "s3:PutObject"
                    ],
                    principals=[iam.AnyPrincipal()],
                    resources=[dest_bucket.bucket_arn, dest_bucket.arn_for_objects('*')],
                    )


        dest_bucket.add_to_resource_policy(policy_doc)


        script_bucket = s3.Bucket(
            self, id="tsan-eltJob-script", bucket_name="tsan-etljob-script", versioned=False, block_public_access=s3.BlockPublicAccess.BLOCK_ALL)



        policy_doc = iam.PolicyStatement(actions=[
                        "s3:GetObject",
                        "s3:GetObjectVersion",
                        "s3:PutObject"
                    ],
                    principals=[iam.AnyPrincipal()],
                    resources=[script_bucket.bucket_arn, script_bucket.arn_for_objects('*')],
                    )

        script_bucket.add_to_resource_policy(policy_doc)


        s3_client.upload_file('etl_job/job.py', 'tsan-etljob-script','Scripts/job.py')

        glue_role = iam.Role(self, "Glue-role",
                        assumed_by=iam.ServicePrincipal("glue.amazonaws.com"))
        glue_role.add_managed_policy(iam.ManagedPolicy.from_aws_managed_policy_name(
            "service-role/AWSGlueServiceRole"))
        
        glue_role.add_managed_policy(iam.ManagedPolicy.from_aws_managed_policy_name("AmazonS3FullAccess"))

        PYTHON_VERSION = "3"
        COMMAND_NAME = "pythonshell"
        jobName = 'etljob'

        etljob = glue.CfnJob(self, id=jobName, name=jobName, role=glue_role.role_arn,
                             max_capacity=1,
                             command=glue.CfnJob.JobCommandProperty(
                                 name=COMMAND_NAME,
                                 python_version=PYTHON_VERSION,
                                 script_location="s3://tsan-etljob-script/Scripts/job.py"),
                                 glue_version="2.0"
                             )

        schedule = "cron(40 4 * * ? *)" #in UTC

        cfn_trigger = glue.CfnTrigger(self, "MyCfnTrigger",
            actions=[glue.CfnTrigger.ActionProperty(
            job_name=jobName,
            timeout=123
            )],
            type="SCHEDULED",
            name="scheduled-etljob",
            schedule=schedule,
            start_on_creation=True,
            )

import aws_cdk as core
import aws_cdk.assertions as assertions

from etl_job.etl_job_stack import EtlJobStack

# example tests. To run these tests, uncomment this file along with the example
# resource in etl_job/etl_job_stack.py
def test_sqs_queue_created():
    app = core.App()
    stack = EtlJobStack(app, "etl-job")
    template = assertions.Template.from_stack(stack)

#     template.has_resource_properties("AWS::SQS::Queue", {
#         "VisibilityTimeout": 300
#     })

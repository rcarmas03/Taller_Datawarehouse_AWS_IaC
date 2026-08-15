import aws_cdk as core
import aws_cdk.assertions as assertions

from taller_datawarehouse_aws_ia_c.taller_datawarehouse_aws_ia_c_stack import TallerDatawarehouseAwsIaCStack

# example tests. To run these tests, uncomment this file along with the example
# resource in taller_datawarehouse_aws_ia_c/taller_datawarehouse_aws_ia_c_stack.py
def test_sqs_queue_created():
    app = core.App()
    stack = TallerDatawarehouseAwsIaCStack(app, "taller-datawarehouse-aws-ia-c")
    template = assertions.Template.from_stack(stack)

#     template.has_resource_properties("AWS::SQS::Queue", {
#         "VisibilityTimeout": 300
#     })

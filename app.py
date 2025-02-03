import aws_cdk as cdk
from stack import SimplePipelineStack

app = cdk.App()
SimplePipelineStack(app, "SimplePipelineStack")
app.synth()

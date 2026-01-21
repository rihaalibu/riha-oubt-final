from aws_cdk import App, Environment, Fn
from stacks.s3_kms_stack import StorageStack
from stacks.glue_stack import GlueStack
from stacks.orchestration_stack import OrchestrationStack
from stacks.redshift_stack import RedshiftServerlessStack
from stacks.network_stack import NetworkStack
# from stacks.script_deploy_stack import ScriptDeploymentStack

app = App()


env = Environment(
    account=app.node.try_get_context("account"),
    region=app.node.try_get_context("region"),
)
network = NetworkStack(app, "RihanaNetworkStack", env=env)

storage = StorageStack(
    app,
    "RihaStorageStack",
    env=env,
)
# kms_key_arn = Fn.import_value("RihaStorageStack:KmsKeyArn")
# scripts = ScriptDeploymentStack(app, "RihaScriptDeploymentStack", env=env)
# scripts.add_dependency(storage)
redshift = RedshiftServerlessStack(
    app,
    "RihaRedshiftServerlessStack",
    vpc=network.vpc,
    sg=network.redshift_sg,
    env=env,
)

glue = GlueStack(
    app,
    "RihaGlueStack",
    bucket=storage.bucket,
    # kms_key=storage.kms.key,
    redshift_workgroup=redshift.workgroup_name,
    redshift_db=redshift.database_name,
    env=env,
)


# 3. Orchestration stack
#    - Step Functions
#    - CloudWatch

OrchestrationStack(
    app,
    "RihaOrchestrationStack",
    glue_pipeline=glue.glue_pipeline,
    env=env,
)

app.synth()

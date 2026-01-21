#!/usr/bin/env python3
from aws_cdk import Stack, Fn, aws_kms as kms, aws_iam as iam
from constructs import Construct
from aws_cdk import aws_s3 as s3, aws_s3_deployment as s3deploy, Fn


class ScriptDeploymentStack(Stack):
    def __init__(self, scope: Construct, id: str, **kwargs):
        super().__init__(scope, id, **kwargs)
        kms_key = kms.Key.from_key_arn(
            self,
            "ImportedRihaDataKey",
            Fn.import_value("RihaDataLakeKmsKeyArn:KmsKeyArn"),
        )

        bucket_name = Fn.import_value("RihaStorageStack:BucketName")

        bucket = s3.Bucket.from_bucket_name(
            self,
            "ImportedDataLakeBucket",
            bucket_name,
        )

        deploy_role = iam.Role(
            self,
            "GlueScriptsDeployRole",
            assumed_by=iam.ServicePrincipal("lambda.amazonaws.com"),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name(
                    "service-role/AWSLambdaBasicExecutionRole"
                )
            ],
        )
        bucket.grant_read_write(deploy_role)
        deploy_role.add_to_policy(
            iam.PolicyStatement(
                actions=[
                    "kms:Encrypt",
                    "kms:GenerateDataKey*",
                    "kms:DescribeKey",
                ],
                resources=[kms_key.key_arn],
            )
        )

        deploy_role.add_to_policy(
            iam.PolicyStatement(
                actions=["s3:DeleteObject"],
                resources=[bucket.arn_for_objects("*")],
            )
        )

        deploy_role.add_to_policy(
            iam.PolicyStatement(
                actions=["s3:ListBucket"],
                resources=[bucket.bucket_arn],
            )
        )

        s3deploy.BucketDeployment(
            self,
            "DeployGlueScripts",
            destination_bucket=bucket,
            destination_key_prefix="scripts",
            sources=[s3deploy.Source.asset("glue_jobs")],
            server_side_encryption=s3deploy.ServerSideEncryption.AWS_KMS,
            server_side_encryption_aws_kms_key_id=kms_key.key_id,
            role=deploy_role,
            prune=True,
        )

#!/usr/bin/env python3
from aws_cdk import aws_iam as iam, Stack
from aws_cdk import aws_kms as kms
from aws_cdk import CfnOutput
from constructs import Construct


class KmsConstruct(Construct):
    def __init__(self, scope: Construct, id: str):
        super().__init__(scope, id)

        _key = kms.Key(
            self, "DataKey", enable_key_rotation=True, alias="alias/riha-data"
        )

        _key.add_to_resource_policy(
            iam.PolicyStatement(
                sid="AllowRedshiftUsage",
                principals=[
                    iam.ServicePrincipal("redshift.amazonaws.com"),
                    iam.ServicePrincipal("redshift-serverless.amazonaws.com"),
                ],
                actions=[
                    "kms:Encrypt",
                    "kms:Decrypt",
                    "kms:GenerateDataKey*",
                    "kms:DescribeKey",
                ],
                resources=["*"],
            )
        )

        _key.add_to_resource_policy(
            iam.PolicyStatement(
                sid="AllowLambdaS3Deployments",
                principals=[iam.ServicePrincipal("lambda.amazonaws.com")],
                actions=[
                    "kms:Encrypt",
                    "kms:GenerateDataKey*",
                    "kms:DescribeKey",
                ],
                resources=["*"],
                conditions={
                    "StringEquals": {
                        "aws:SourceAccount": Stack.of(self).account,
                    }
                },
            )
        )
        _key.add_to_resource_policy(
            iam.PolicyStatement(
                sid="AllowS3UseOfKey",
                principals=[iam.ServicePrincipal("s3.amazonaws.com")],
                actions=[
                    "kms:Encrypt",
                    "kms:Decrypt",
                    "kms:ReEncrypt*",
                    "kms:GenerateDataKey*",
                    "kms:DescribeKey",
                ],
                resources=["*"],
                conditions={
                    "StringEquals": {
                        "aws:SourceAccount": Stack.of(self).account,
                        "kms:ViaService": f"s3.{Stack.of(self).region}.amazonaws.com",
                    }
                },
            )
        )
        self.key_arn = _key.key_arn

        CfnOutput(
            self,
            "RihaDataKeyArn",
            value=self.key_arn,
            export_name="RihaDataKeyArn",
        )

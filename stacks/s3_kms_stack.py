#!/usr/bin/env python3
from aws_cdk import Stack, Duration, CfnOutput
from constructs import Construct
from constructs.kms_construct import KmsConstruct
from constructs.s3_construct import S3DataLakeConstruct
from aws_cdk import (
    aws_s3_deployment as s3deploy,
    aws_lambda as _lambda,
    custom_resources as cr,
    aws_kms as kms,
    CustomResource,
)
from pathlib import Path


class StorageStack(Stack):
    def __init__(self, scope: Construct, id: str, **kwargs):
        super().__init__(scope, id, **kwargs)

        self.kms = KmsConstruct(self, "Kms")
        imported_key = kms.Key.from_key_arn(
            self, "ImportedRihaDataKey", self.kms.key_arn
        )
        self.s3 = S3DataLakeConstruct(
            self,
            "S3",
            kms_key=imported_key,
        )
        self.bucket = self.s3.bucket
        CfnOutput(
            self,
            "RihaDataLakeKmsKeyArn",
            value=self.kms.key_arn,
            export_name="RihaDataLakeKmsKeyArn:KmsKeyArn",
        )

        CfnOutput(
            self,
            "RihaDataLakeBucketName",
            value=self.bucket.bucket_name,
            export_name=f"{self.stack_name}:BucketName",
        )

        # prefixes = [
        #     "raw/",
        #     "validated/nyc_taxi/",
        #     "processed/nyc_taxi/",
        #     "master/zones/source/",
        #     "master/vendors/",
        #     "scd2/dim_zones/",
        #     "scd2/dim_vendors/",
        #     "curated/",
        #     "audit/",
        #     "quarantine/",
        #     "backup/",
        #     "scripts/",
        # ]

        # folder_fn = _lambda.Function(
        #     self,
        #     "FolderMarkerFn",
        #     runtime=_lambda.Runtime.PYTHON_3_11,
        #     handler="index.handler",
        #     code=_lambda.Code.from_asset("lambdas/folder_markers"),
        #     timeout=Duration.seconds(60),
        # )

        # self.bucket.grant_read_write(folder_fn)

        # provider = cr.Provider(
        #     self,
        #     "FolderMarkerProvider",
        #     on_event_handler=folder_fn,
        # )

        # CustomResource(
        #     self,
        #     "CreateFolderMarkers",
        #     service_token=provider.service_token,
        #     properties={
        #         "Bucket": self.bucket.bucket_name,
        #         "Prefixes": prefixes,
        #     },
        # )

        # s3deploy.BucketDeployment(
        #     self,
        #     "DeployGlueScripts",
        #     destination_bucket=self.bucket,
        #     destination_key_prefix="scripts",
        #     sources=[s3deploy.Source.asset("glue_jobs")],
        # )

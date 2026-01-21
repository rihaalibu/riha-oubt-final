#!/usr/bin/env python3
from aws_cdk import aws_s3 as s3
from constructs import Construct
from typing import Optional


class S3DataLakeConstruct(Construct):
    def __init__(
        self, scope: Construct, id: str, *, kms_key, bucket_name: Optional[str] = None
    ):
        super().__init__(scope, id)

        bucket_props = {
            "encryption": s3.BucketEncryption.KMS,
            "encryption_key": kms_key,
            "versioned": True,
            "enforce_ssl": True,
            "block_public_access": s3.BlockPublicAccess.BLOCK_ALL,
        }
        if bucket_name:
            bucket_props["bucket_name"] = bucket_name

        self.bucket = s3.Bucket(self, "DataLake", **bucket_props)

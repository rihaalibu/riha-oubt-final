from aws_cdk import Stack, Aws, Fn
from constructs import Construct

from aws_cdk import (
    aws_glue as glue,
    aws_kms as kms,
    aws_s3 as s3,
)

from constructs.glue_jobs_construct import GluePipelineConstruct


class GlueStack(Stack):
    """
    Owns:
      - Glue Catalog database(s)
      - GluePipelineConstruct (jobs + curated crawler)
    Exposes:
      - self.glue_pipeline for OrchestrationStack
    """

    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        *,
        bucket: s3.IBucket,
        redshift_workgroup: str,
        redshift_db: str,
        **kwargs,
    ) -> None:
        super().__init__(scope, construct_id, **kwargs)
        kms_key = kms.Key.from_key_arn(
            self,
            "ImportedRihaDataKey",
            Fn.import_value("RihaDataLakeKmsKeyArn:KmsKeyArn"),
        )

        # Glue Catalog Database for curated layer
        # (This is the DB Athena/Spectrum will use)

        curated_db = glue.CfnDatabase(
            self,
            "RihaCuratedDb",
            catalog_id=Aws.ACCOUNT_ID,
            database_input=glue.CfnDatabase.DatabaseInputProperty(
                name=f"{self.stack_name.lower()}_curated_db"
            ),
        )

        # Glue pipeline
        self.glue_pipeline = GluePipelineConstruct(
            self,
            "RihaGluePipeline",
            bucket=bucket,
            kms_key=kms_key,
            curated_database_name=curated_db.ref,
            redshift_workgroup=redshift_workgroup,
            redshift_db=redshift_db,
        )
        self.pipeline_name = self.glue_pipeline.node.path
        self.curated_crawler_name = self.glue_pipeline.curated_crawler_name

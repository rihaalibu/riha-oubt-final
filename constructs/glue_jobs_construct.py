from constructs import Construct
from aws_cdk import aws_glue as glue, aws_iam as iam, Fn


class GluePipelineConstruct(Construct):
    def __init__(
        self,
        scope: Construct,
        id: str,
        *,
        bucket,
        kms_key,
        curated_database_name: str,
        redshift_workgroup: str,
        redshift_db: str,
    ):
        super().__init__(scope, id)

        # Glue IAM Role

        self.glue_role = iam.Role(
            self,
            "GlueExecutionRole",
            assumed_by=iam.ServicePrincipal("glue.amazonaws.com"),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name(
                    "service-role/AWSGlueServiceRole"
                )
            ],
        )
        redshift_iam_role_arn = Fn.import_value("RedshiftIamRoleArn")

        redshift_iam_role = iam.Role.from_role_arn(
            self,
            "ImportedRedshiftIamRole",
            redshift_iam_role_arn,
            mutable=False,
        )

        self.glue_role.add_to_policy(
            iam.PolicyStatement(
                effect=iam.Effect.ALLOW,
                actions=[
                    # Redshift Data API
                    "redshift-data:ExecuteStatement",
                    "redshift-data:DescribeStatement",
                    "redshift-data:GetStatementResult",
                    # Required to discover workgroup
                    "redshift-serverless:GetWorkgroup",
                    "redshift-serverless:GetNamespace",
                    # Glue Catalog (Spectrum)
                    "glue:GetDatabase",
                    "glue:GetDatabases",
                    "glue:GetTable",
                    "glue:GetTables",
                ],
                resources=["*"],  # Data API requires wildcard
            )
        )

        bucket.grant_read_write(self.glue_role)
        kms_key.grant_encrypt_decrypt(self.glue_role)
        bucket.grant_read(redshift_iam_role)
        kms_key.grant_encrypt_decrypt(redshift_iam_role)

        # Common Glue Arguments (INJECTED EVERYWHERE)

        common_args = {
            "--BUCKET": bucket.bucket_name,
            "--RAW_PREFIX": "raw/",
            "--VALIDATED_PREFIX": "validated/",
            "--PROCESSED_PREFIX": "processed/",
            "--MASTER_PREFIX": "master/",
            "--SCD2_PREFIX": "scd2/",
            "--CURATED_PREFIX": "curated/curated_trips",
            "--AUDIT_PREFIX": "audit/",
            "--QUARANTINE_PREFIX": "quarantine/",
            "--BACKUP_PREFIX": "backup/",
            "--job-bookmark-option": "job-bookmark-disable",
            "--enable-continuous-cloudwatch-log": "true",
            "--enable-metrics": "true",
            # "--additional-python-modules": "delta-spark==2.4.0",
            # "--spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
            # "--spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
            # "--additional-python-modules": "delta-spark==2.4.0",
            # "--additional-jars": "io.delta:delta-core_2.12:2.4.0",
            "--datalake-formats": "delta",
            "--conf": "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension ",
            "--conf1": "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog",
            "--REDSHIFT_WORKGROUP": Fn.import_value("RedshiftWorkgroupName"),
            "--REDSHIFT_DB": Fn.import_value("RedshiftDatabaseName"),
            "--REDSHIFT_IAM_ROLE_ARN": Fn.import_value("RedshiftIamRoleArn"),
            "--GLUE_CATALOG_DB": curated_database_name,
        }

        # Glue Jobs
        def glue_job(name: str, script: str, extra_args=None) -> glue.CfnJob:
            args = dict(common_args)
            if extra_args:
                args.update(extra_args)
            return glue.CfnJob(
                self,
                f"Job{name.replace('_', '').replace('-', '')}",
                name=name,
                role=self.glue_role.role_arn,
                command={
                    "name": "glueetl",
                    "scriptLocation": f"s3://{bucket.bucket_name}/scripts/{script}",
                    "pythonVersion": "3",
                },
                glue_version="4.0",
                number_of_workers=5,
                worker_type="G.1X",
                default_arguments=args,
            )

        self.jobs = {
            "validate": glue_job("01_validate_clean", "01_validate_clean.py"),
            "process": glue_job("02_process_trips", "02_process_trips.py"),
            "createZones": glue_job(
                "02b_create_golden_zones", "02b_create_golden_zones.py"
            ),
            "createVendors": glue_job(
                "02c_create_golden_vendors", "02c_create_golden_vendors.py"
            ),
            "zones": glue_job("03_scd2_zones", "03_scd2_zones.py"),
            "vendors": glue_job("03b_scd2_vendors", "03b_scd2_vendors.py"),
            "fact": glue_job("04_fact_trips", "04_fact_trips.py"),
            "redshift": glue_job(
                "05_redshift_load",
                "05_redshift_load.py",
                extra_args={
                    "--REDSHIFT_WORKGROUP": Fn.import_value("RedshiftWorkgroupName"),
                    "--REDSHIFT_DB": Fn.import_value("RedshiftDatabaseName"),
                    "--REDSHIFT_IAM_ROLE_ARN": Fn.import_value("RedshiftIamRoleArn"),
                    "--GLUE_CATALOG_DB": curated_database_name,
                },
            ),
            "governance": glue_job("06_governance_metrics", "06_governance_metrics.py"),
        }

        self.curated_crawler = glue.CfnCrawler(
            self,
            "CuratedCrawler",
            name="rihaoubt-curated-crawler",
            role=self.glue_role.role_arn,
            database_name=curated_database_name,  # ✅ FIXED
            targets=glue.CfnCrawler.TargetsProperty(
                s3_targets=[
                    glue.CfnCrawler.S3TargetProperty(
                        path=f"s3://{bucket.bucket_name}/curated/"
                    )
                ]
            ),
            schema_change_policy=glue.CfnCrawler.SchemaChangePolicyProperty(
                update_behavior="UPDATE_IN_DATABASE",
                delete_behavior="DEPRECATE_IN_DATABASE",
            ),
            recrawl_policy=glue.CfnCrawler.RecrawlPolicyProperty(
                recrawl_behavior="CRAWL_EVERYTHING"
            ),
            table_prefix="curated_",
        )
        self.curated_crawler_name = self.curated_crawler.name

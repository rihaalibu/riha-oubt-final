from constructs import Construct
from aws_cdk import CfnOutput
from aws_cdk import (
    Stack,
    aws_iam as iam,
    aws_ec2 as ec2,
    aws_kms as kms,
    aws_redshiftserverless as rss,
    Fn,
)


class RedshiftServerlessStack(Stack):
    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        *,
        vpc: ec2.IVpc,
        sg: ec2.ISecurityGroup,
        **kwargs,
    ):
        super().__init__(scope, construct_id, **kwargs)

        kms_key = kms.Key.from_key_arn(
            self,
            "ImportedRihaDataKey",
            Fn.import_value("RihaDataLakeKmsKeyArn:KmsKeyArn"),
        )
        base = self.stack_name.lower().replace("_", "-")
        self.workgroup_name = f"{base}-redshift-wg"
        self.database_name = f"{base}_dev"
        self.namespace_name = f"{base}-namespace"

        self.redshift_role = iam.Role(
            self,
            "RedshiftServerlessRole",
            assumed_by=iam.ServicePrincipal("redshift.amazonaws.com"),
            description="IAM role for Redshift Serverless access to S3 and Glue",
        )

        self.redshift_role.add_managed_policy(
            iam.ManagedPolicy.from_aws_managed_policy_name("AmazonS3ReadOnlyAccess")
        )

        self.redshift_role.add_managed_policy(
            iam.ManagedPolicy.from_aws_managed_policy_name("AWSGlueConsoleFullAccess")
        )
        self.redshift_role.add_to_policy(
            iam.PolicyStatement(
                actions=[
                    "kms:Decrypt",
                    "kms:GenerateDataKey*",
                    "kms:DescribeKey",
                ],
                resources=[kms_key.key_arn],
            )
        )

        namespace = rss.CfnNamespace(
            self,
            "RedshiftNamespace",
            namespace_name=self.namespace_name,
            db_name=self.database_name,
            manage_admin_password=True,
            iam_roles=[self.redshift_role.role_arn],
            kms_key_id=kms_key.key_arn,
        )

        workgroup = rss.CfnWorkgroup(
            self,
            "RedshiftWorkgroup",
            workgroup_name=self.workgroup_name,
            namespace_name=namespace.namespace_name,
            # Minimum allowed
            base_capacity=8,
            # MUST resolve to a non-empty list
            subnet_ids=vpc.select_subnets(
                subnet_type=ec2.SubnetType.PRIVATE_WITH_EGRESS
            ).subnet_ids,
            security_group_ids=[sg.security_group_id],
            publicly_accessible=False,
        )

        workgroup.add_dependency(namespace)
        CfnOutput(
            self,
            "RedshiftIamRoleArn",
            value=self.redshift_role.role_arn,
            export_name="RedshiftIamRoleArn",
        )

        CfnOutput(
            self,
            "RedshiftWorkgroupName",
            value=self.workgroup_name,
            export_name="RedshiftWorkgroupName",
        )

        CfnOutput(
            self,
            "RedshiftDatabaseName",
            value=self.database_name,
            export_name="RedshiftDatabaseName",
        )

from aws_cdk import Stack
from constructs import Construct
from aws_cdk import aws_ec2 as ec2


class NetworkStack(Stack):
    def __init__(self, scope: Construct, construct_id: str, **kwargs):
        super().__init__(scope, construct_id, **kwargs)

        self.vpc = ec2.Vpc(
            self,
            "RihaVpc",
            max_azs=2,
            nat_gateways=1,
            subnet_configuration=[
                ec2.SubnetConfiguration(
                    name="public",
                    subnet_type=ec2.SubnetType.PUBLIC,
                ),
                ec2.SubnetConfiguration(
                    name="private",
                    subnet_type=ec2.SubnetType.PRIVATE_WITH_EGRESS,
                ),
            ],
        )

        self.redshift_sg = ec2.SecurityGroup(
            self,
            "RedshiftSG",
            vpc=self.vpc,
            allow_all_outbound=True,
            description="Redshift Serverless security group",
        )

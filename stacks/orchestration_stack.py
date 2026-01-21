#!/usr/bin/env python3
import json

from aws_cdk import (
    Stack,
    Duration,
)
from constructs import Construct

from aws_cdk import (
    aws_stepfunctions as sfn,
    aws_stepfunctions_tasks as tasks,
    aws_logs as logs,
    aws_cloudwatch as cloudwatch,
)


class OrchestrationStack(Stack):
    def __init__(
        self,
        scope: Construct,
        id: str,
        *,
        glue_pipeline,
        **kwargs,
    ):
        super().__init__(scope, id, **kwargs)

        # Helper: synchronous Glue job runner

        def run_glue(job_name: str) -> sfn.IChainable:
            return tasks.GlueStartJobRun(
                self,
                f"Run{job_name.replace('_', '').replace('-', '')}",
                glue_job_name=job_name,
                integration_pattern=sfn.IntegrationPattern.RUN_JOB,
                timeout=Duration.hours(2),
            )

        # Glue crawler tasks

        start_crawler = tasks.CallAwsService(
            self,
            "StartCuratedCrawler",
            service="glue",
            action="startCrawler",
            parameters={"Name": glue_pipeline.curated_crawler.name},
            iam_resources=["*"],
        )

        wait_before_poll = sfn.Wait(
            self,
            "WaitBeforePollingCrawler",
            time=sfn.WaitTime.duration(Duration.seconds(30)),
        )

        get_crawler_status = tasks.CallAwsService(
            self,
            "GetCuratedCrawlerStatus",
            service="glue",
            action="getCrawler",
            parameters={"Name": glue_pipeline.curated_crawler.name},
            iam_resources=["*"],
        )

        crawler_finished = sfn.Choice(self, "CrawlerFinished?")

        # =================================================
        # Pipeline definition (STRICT ORDER)
        # =================================================
        definition = (
            run_glue("01_validate_clean")
            .next(run_glue("02_process_trips"))
            .next(run_glue("02b_create_golden_zones"))
            .next(run_glue("02c_create_golden_vendors"))
            .next(run_glue("03_scd2_zones"))
            .next(run_glue("03b_scd2_vendors"))
            .next(run_glue("04_fact_trips"))
            .next(start_crawler)
            .next(wait_before_poll)
            .next(get_crawler_status)
        )

        crawler_finished.when(
            sfn.Condition.string_equals("$.Crawler.State", "READY"),
            run_glue("05_redshift_load").next(run_glue("06_governance_metrics")),
        )

        crawler_finished.otherwise(wait_before_poll)

        definition = definition.next(crawler_finished)

        # =================================================
        # Step Functions logging
        # =================================================
        sm_logs = logs.LogGroup(
            self,
            "PipelineStateMachineLogs",
            retention=logs.RetentionDays.ONE_WEEK,
        )

        # =================================================
        # State machine
        # =================================================
        self.state_machine = sfn.StateMachine(
            self,
            "RihaOubtPipelineStateMachine",
            definition_body=sfn.DefinitionBody.from_chainable(definition),
            timeout=Duration.hours(6),
            logs=sfn.LogOptions(
                destination=sm_logs,
                level=sfn.LogLevel.ALL,
                include_execution_data=True,
            ),
        )

        # =================================================
        # CloudWatch Dashboard (Execution-level)
        # =================================================
        cloudwatch.CfnDashboard(
            self,
            "RihaOubtPipelineDashboard",
            dashboard_name=f"{self.stack_name}-pipeline-dashboard",
            dashboard_body=json.dumps(
                {
                    "widgets": [
                        {
                            "type": "metric",
                            "x": 0,
                            "y": 0,
                            "width": 12,
                            "height": 6,
                            "properties": {
                                "title": "Pipeline Executions",
                                "metrics": [
                                    [
                                        "AWS/States",
                                        "ExecutionsStarted",
                                        "StateMachineArn",
                                        self.state_machine.state_machine_arn,
                                    ],
                                    [".", "ExecutionsSucceeded", ".", "."],
                                    [".", "ExecutionsFailed", ".", "."],
                                ],
                                "stat": "Sum",
                                "period": 300,
                                "region": self.region,
                            },
                        },
                        {
                            "type": "metric",
                            "x": 12,
                            "y": 0,
                            "width": 12,
                            "height": 6,
                            "properties": {
                                "title": "Pipeline Duration (p95)",
                                "metrics": [
                                    [
                                        "AWS/States",
                                        "ExecutionTime",
                                        "StateMachineArn",
                                        self.state_machine.state_machine_arn,
                                    ]
                                ],
                                "stat": "p95",
                                "period": 300,
                                "region": self.region,
                            },
                        },
                    ]
                }
            ),
        )

        # CloudWatch Alarm (Pipeline failure)

        cloudwatch.Alarm(
            self,
            "PipelineFailureAlarm",
            alarm_name=f"{self.stack_name}-pipeline-failure",
            metric=cloudwatch.Metric(
                namespace="AWS/States",
                metric_name="ExecutionsFailed",
                dimensions_map={
                    "StateMachineArn": self.state_machine.state_machine_arn
                },
                statistic="Sum",
                period=Duration.minutes(5),
            ),
            threshold=1,
            evaluation_periods=1,
            alarm_description="Riha OUBT pipeline execution failed",
            treat_missing_data=cloudwatch.TreatMissingData.NOT_BREACHING,
        )

#!/usr/bin/env python3
from constructs import Construct
from aws_cdk import (
    Duration,
    aws_stepfunctions as sfn,
    aws_stepfunctions_tasks as tasks,
)


class PipelineStateMachineConstruct(Construct):
    def __init__(self, scope: Construct, id: str):
        super().__init__(scope, id)

        glue_job = lambda name: tasks.GlueStartJobRun(
            self,
            f"Run{name}",
            glue_job_name=name,
            integration_pattern=sfn.IntegrationPattern.RUN_JOB,
            timeout=Duration.hours(2),
        )

        start_crawler = tasks.CallAwsService(
            self,
            "StartCuratedCrawler",
            service="glue",
            action="startCrawler",
            parameters={"Name": "riha_curated_crawler"},
            iam_resources=["*"],
        )

        wait = sfn.Wait(
            self,
            "WaitBeforePollingCrawler",
            time=sfn.WaitTime.duration(Duration.seconds(30)),
        )

        get_crawler = tasks.CallAwsService(
            self,
            "CheckCrawlerStatus",
            service="glue",
            action="getCrawler",
            parameters={"Name": "riha_curated_crawler"},
            iam_resources=["*"],
        )

        crawler_ready = sfn.Choice(self, "CrawlerFinished?")
        crawler_ready.when(
            sfn.Condition.string_equals("$.Crawler.State", "READY"),
            glue_job("05_redshift_load"),
        ).otherwise(wait)

        definition = (
            glue_job("01_validate_clean")
            .next(glue_job("02_process_trips"))
            .next(glue_job("02b_create_golden_zones"))
            .next(glue_job("02c_create_golden_vendors"))
            .next(glue_job("03_scd2_zones"))
            .next(glue_job("03b_scd2_vendors"))
            .next(glue_job("04_fact_trips"))
            .next(start_crawler)
            .next(wait)
            .next(get_crawler)
            .next(crawler_ready)
            .next(glue_job("06_governance_metrics"))
        )

        self.state_machine = sfn.StateMachine(
            self,
            "RihaOubtPipeline",
            definition_body=sfn.DefinitionBody.from_chainable(definition),
            timeout=Duration.hours(6),
        )

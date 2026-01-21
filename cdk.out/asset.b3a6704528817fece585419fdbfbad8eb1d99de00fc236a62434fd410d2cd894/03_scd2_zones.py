############
import json
import sys
from datetime import datetime
from uuid import uuid4

from awsglue.utils import getResolvedOptions
from delta.tables import DeltaTable
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType


def utc_now_iso() -> str:
    return datetime.utcnow().isoformat()


def main() -> None:
    # -------------------------------------------------
    # Glue args
    # -------------------------------------------------
    args = getResolvedOptions(
        sys.argv,
        ["BUCKET", "MASTER_PREFIX", "SCD2_PREFIX", "AUDIT_PREFIX"],
    )

    bucket = args["BUCKET"]

    golden_zones_path = f"s3://{bucket}/{args['MASTER_PREFIX']}zones/"
    scd2_zones_path = f"s3://{bucket}/{args['SCD2_PREFIX']}dim_zones/"
    audit_path = f"s3://{bucket}/{args['AUDIT_PREFIX']}scd2_zones/"

    run_id = str(uuid4())
    run_ts = utc_now_iso()

    # -------------------------------------------------
    # Spark (Glue-managed; Delta enabled via job args)
    # -------------------------------------------------
    spark = SparkSession.builder.appName("03_scd2_zones").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    # Best-effort Delta lineage metadata
    def set_commit_metadata(payload: dict) -> None:
        spark.conf.set(
            "spark.databricks.delta.commitInfo.userMetadata",
            json.dumps(payload),
        )

    # -------------------------------------------------
    # Read Golden Zones
    # -------------------------------------------------
    new_raw = spark.read.option("header", True).csv(golden_zones_path)

    # -------------------------------------------------
    # Data-quality guards (schema + key integrity)
    # -------------------------------------------------
    required_cols = ["LocationID", "Zone", "Borough"]
    missing = [c for c in required_cols if c not in new_raw.columns]
    if missing:
        raise ValueError(f"SCD2 zones aborted: missing required columns: {missing}")

    new_df = (
        new_raw.withColumn("LocationID", F.col("LocationID").cast(IntegerType()))
        .withColumn("Zone", F.upper(F.trim(F.col("Zone"))))
        .withColumn("Borough", F.trim(F.col("Borough")))
    )

    null_keys = new_df.filter(F.col("LocationID").isNull()).count()
    if null_keys > 0:
        raise ValueError(f"SCD2 zones aborted: {null_keys} rows have NULL LocationID")

    dup_keys = new_df.groupBy("LocationID").count().filter(F.col("count") > 1).count()
    if dup_keys > 0:
        raise ValueError(
            f"SCD2 zones aborted: found {dup_keys} duplicate LocationID keys in golden input"
        )

    rows_in_golden = new_df.count()
    if rows_in_golden == 0:
        raise ValueError("SCD2 zones aborted: golden zones input is empty")

    # -------------------------------------------------
    # Standardization rules (aligned with your golden zones logic)
    # -------------------------------------------------
    new_df = new_df.withColumn(
        "Borough",
        F.when(
            F.col("Borough").isNull() | (F.col("Borough") == ""), F.lit("Outside NYC")
        ).otherwise(F.upper(F.col("Borough"))),
    )

    new_df = new_df.withColumn(
        "Borough",
        F.when(
            F.col("Borough").isin("UNKNOWN", "N/A", "NA"), F.lit("Outside NYC")
        ).otherwise(F.col("Borough")),
    )

    # -------------------------------------------------
    # Hash for change detection (business attrs only)
    # -------------------------------------------------
    new_df = new_df.withColumn(
        "hash_diff",
        F.sha2(
            F.concat_ws(
                "||",
                F.col("LocationID").cast("string"),
                F.coalesce(F.col("Zone"), F.lit("")),
                F.coalesce(F.col("Borough"), F.lit("")),
            ),
            256,
        ),
    )

    # -------------------------------------------------
    # First run: create SCD2 Delta table
    # -------------------------------------------------
    if not DeltaTable.isDeltaTable(spark, scd2_zones_path):
        base_df = (
            new_df.withColumn(
                "zone_key",
                F.sha2(
                    F.concat_ws(
                        "||",
                        F.col("LocationID").cast("string"),
                        F.current_timestamp().cast("string"),
                        F.lit("1"),
                    ),
                    256,
                ),
            )
            .withColumn("version_number", F.lit(1))
            .withColumn("is_current", F.lit(True))
            .withColumn("start_date", F.current_timestamp())
            .withColumn("end_date", F.lit(None).cast("timestamp"))
            .select(
                "zone_key",
                "LocationID",
                "Zone",
                "Borough",
                "hash_diff",
                "version_number",
                "is_current",
                "start_date",
                "end_date",
            )
        )

        set_commit_metadata(
            {
                "run_id": run_id,
                "run_ts": run_ts,
                "job": "03_scd2_zones",
                "action": "first_run_create",
                "source_path": golden_zones_path,
                "target_path": scd2_zones_path,
                "rows_in_golden": rows_in_golden,
            }
        )

        base_df.write.format("delta").mode("overwrite").save(scd2_zones_path)

        hist = (
            DeltaTable.forPath(spark, scd2_zones_path).history(1).collect()[0].asDict()
        )

        audit_df = spark.createDataFrame(
            [
                {
                    "run_id": run_id,
                    "run_ts": run_ts,
                    "rows_in_golden": rows_in_golden,
                    "rows_inserted": base_df.count(),
                    "rows_expired": 0,
                    "note": "first_run",
                    "delta_version": int(hist.get("version"))
                    if hist.get("version") is not None
                    else None,
                    "delta_operation": hist.get("operation"),
                    "delta_timestamp": str(hist.get("timestamp")),
                    "delta_userMetadata": hist.get("userMetadata"),
                    "delta_operationMetrics": json.dumps(
                        hist.get("operationMetrics") or {}
                    ),
                    "source_path": golden_zones_path,
                    "target_path": scd2_zones_path,
                }
            ]
        )
        audit_df.write.mode("append").json(audit_path)

        spark.stop()
        return

    # -------------------------------------------------
    # Load current SCD2 state
    # -------------------------------------------------
    delta_tbl = DeltaTable.forPath(spark, scd2_zones_path)
    scd2_df = delta_tbl.toDF()

    current_df = scd2_df.filter(F.col("is_current") == True).select(
        "LocationID", "hash_diff", "version_number"
    )

    joined = new_df.alias("n").join(current_df.alias("c"), on="LocationID", how="left")

    changes = joined.filter(
        F.col("c.LocationID").isNull() | (F.col("n.hash_diff") != F.col("c.hash_diff"))
    )

    changed_count = changes.count()
    if changed_count == 0:
        hist = delta_tbl.history(1).toDF().collect()[0].asDict()

        audit_df = spark.createDataFrame(
            [
                {
                    "run_id": run_id,
                    "run_ts": run_ts,
                    "rows_in_golden": rows_in_golden,
                    "rows_inserted": 0,
                    "rows_expired": 0,
                    "note": "noop_no_changes",
                    "delta_version": int(hist.get("version"))
                    if hist.get("version") is not None
                    else None,
                    "delta_operation": hist.get("operation"),
                    "delta_timestamp": str(hist.get("timestamp")),
                    "delta_userMetadata": hist.get("userMetadata"),
                    "delta_operationMetrics": json.dumps(
                        hist.get("operationMetrics") or {}
                    ),
                    "source_path": golden_zones_path,
                    "target_path": scd2_zones_path,
                }
            ]
        )
        audit_df.write.mode("append").json(audit_path)
        spark.stop()
        return

    # -------------------------------------------------
    # Compute next version_number for new records
    # -------------------------------------------------
    staged_base = changes.withColumn(
        "next_version",
        F.when(F.col("c.version_number").isNull(), F.lit(1)).otherwise(
            F.col("c.version_number") + 1
        ),
    ).select(
        F.col("LocationID"),
        F.col("n.Zone").alias("Zone"),
        F.col("n.Borough").alias("Borough"),
        F.col("n.hash_diff").alias("hash_diff"),
        F.col("next_version").alias("version_number"),
    )

    # -------------------------------------------------
    # Build staged rows for SINGLE MERGE
    # -------------------------------------------------
    expire_rows = staged_base.select(
        F.col("LocationID").alias("merge_key"),
        F.lit("expire").alias("op"),
        F.col("LocationID"),
        F.lit(None).cast("string").alias("zone_key"),
        F.lit(None).cast("string").alias("Zone"),
        F.lit(None).cast("string").alias("Borough"),
        F.lit(None).cast("string").alias("hash_diff"),
        F.lit(None).cast("int").alias("version_number"),
    )

    insert_rows = staged_base.select(
        F.lit(None).cast("int").alias("merge_key"),
        F.lit("insert").alias("op"),
        F.col("LocationID"),
        F.sha2(
            F.concat_ws(
                "||",
                F.col("LocationID").cast("string"),
                F.current_timestamp().cast("string"),
                F.col("version_number").cast("string"),
            ),
            256,
        ).alias("zone_key"),
        F.col("Zone"),
        F.col("Borough"),
        F.col("hash_diff"),
        F.col("version_number"),
    )

    staged = expire_rows.unionByName(insert_rows)

    set_commit_metadata(
        {
            "run_id": run_id,
            "run_ts": run_ts,
            "job": "03_scd2_zones",
            "action": "merge_scd2",
            "source_path": golden_zones_path,
            "target_path": scd2_zones_path,
            "rows_in_golden": rows_in_golden,
            "changed_keys": staged_base.select("LocationID").distinct().count(),
        }
    )

    # -------------------------------------------------
    # Delta MERGE SCD2
    # -------------------------------------------------
    (
        delta_tbl.alias("t")
        .merge(
            staged.alias("s"),
            "t.LocationID = s.merge_key AND t.is_current = true",
        )
        .whenMatchedUpdate(
            condition="s.op = 'expire'",
            set={
                "is_current": F.lit(False),
                "end_date": F.current_timestamp(),
            },
        )
        .whenNotMatchedInsert(
            condition="s.op = 'insert'",
            values={
                "zone_key": F.col("s.zone_key"),
                "LocationID": F.col("s.LocationID"),
                "Zone": F.col("s.Zone"),
                "Borough": F.col("s.Borough"),
                "hash_diff": F.col("s.hash_diff"),
                "version_number": F.col("s.version_number"),
                "is_current": F.lit(True),
                "start_date": F.current_timestamp(),
                "end_date": F.lit(None).cast("timestamp"),
            },
        )
        .execute()
    )

    # -------------------------------------------------
    # Capture delta version + metrics
    # -------------------------------------------------
    hist = delta_tbl.history(1).toDF().collect()[0].asDict()
    op_metrics = hist.get("operationMetrics") or {}

    audit_df = spark.createDataFrame(
        [
            {
                "run_id": run_id,
                "run_ts": run_ts,
                "rows_in_golden": rows_in_golden,
                "changed_input_rows": changed_count,
                "note": "merge_scd2",
                "delta_version": int(hist.get("version"))
                if hist.get("version") is not None
                else None,
                "delta_operation": hist.get("operation"),
                "delta_timestamp": str(hist.get("timestamp")),
                "delta_userMetadata": hist.get("userMetadata"),
                "delta_operationMetrics": json.dumps(op_metrics),
                "source_path": golden_zones_path,
                "target_path": scd2_zones_path,
            }
        ]
    )
    audit_df.write.mode("append").json(audit_path)

    spark.stop()


if __name__ == "__main__":
    main()

"""
### CODE OWNERS: Umang Gupta, Pierre Cornell
### OBJECTIVE:
  Calculate basic metrics for members by eligibility status
      (memmos, total costs, age, etc.)
### DEVELOPER NOTES:
  None
"""
from databricks.sdk.runtime import *
import os
import pyspark.sql.functions as F
from pyspark.sql import Window

INPUT = 'premier_data_feed'
OUTPUT = 'premier_data_feed'

WELLNESS_HCPCS = ["G0402", "G0438", "G0439", "G0468"]
RUNOUT = 3

# =============================================================================
# LIBRARIES, LOCATIONS, LITERALS, ETC. GO ABOVE HERE
# =============================================================================

def date_as_month(col):
    return (
        F.to_date(
            F.concat(
                F.date_format(col, "yyyy-MM"),
                F.lit("-15")
            )
        )
    )

def Basic() -> int:
    """Calculate high level metrics by eligibility status"""

    dfs_input = {
        path: spark.table(f"{INPUT}.{path}")
        for path in [
            "members",
            "outclaims_full",
            "member_time_windows",
            "risk_scores",
        ]
    }
    dfs_input["member_months"] = spark.table(f"{OUTPUT}.member_months"
                                             )
    member_months = dfs_input["member_months"].where(
        F.col("cover_medical") == "Y"
    )

    cnt_assigned_mems = (
        member_months.select(
            "elig_status",
            F.lit("cnt_assigned_mems").alias("metric_id"),
            "member_id",
        )
        .groupBy("elig_status", "metric_id")
        .agg(F.countDistinct("member_id").alias("metric_value"))
    )

    cnt_assigned_mems_all = (
        member_months.select(
            F.lit("All").alias("elig_status"),
            F.lit("cnt_assigned_mems").alias("metric_id"),
            "member_id",
        )
        .groupBy("elig_status", "metric_id")
        .agg(F.countDistinct("member_id").alias("metric_value"))
    )

    assigned_nonesrd = (
        member_months.where(F.col("elig_status") != "ESRD")
        .select(
            F.lit("Non-ESRD").alias("elig_status"),
            F.lit("cnt_assigned_mems_nonesrd").alias("metric_id"),
            "member_id",
        )
        .groupBy("elig_status", "metric_id")
        .agg(F.countDistinct("member_id").alias("metric_value"))
    )

    memmos_sum = (
        member_months.select(
            "elig_status", F.lit("memmos_sum").alias("metric_id"), "memmos"
        )
        .groupBy("elig_status", "metric_id")
        .agg(F.sum("memmos").alias("metric_value"))
    )

    risk_score = (
        member_months.select(
            "elig_status",
            F.lit("riskscr_1_avg").alias("metric_id"),
            "memmos",
            "risk_score",
        )
        .groupBy("elig_status", "metric_id")
        .agg(
            (
                F.sum(
                    F.col("memmos") * F.col("risk_score")
                )
                / F.sum("memmos")
            ).alias("metric_value")
        )
    )

    outclaims = dfs_input["outclaims_full"].withColumn(
        "month",
        date_as_month(F.col("prm_fromdate")),
    )

    elig_memmos = (
        dfs_input["member_time_windows"]
        .where(F.col("cover_medical") == "Y")
        .select("member_id", "date_start", "date_end")
    )

    outclaims_mem = outclaims.join(
        elig_memmos,
        on=[
            outclaims.member_id == elig_memmos.member_id,
            F.col("prm_fromdate").between(
                F.col("date_start"), F.col("date_end")
            ),
        ],
        how="inner",
    ).join(
        member_months,
        on=(outclaims.member_id == member_months.member_id)
        & (outclaims.month == member_months.elig_month),
        how="inner",
    )

    all_costs = (
        outclaims_mem.select(
            "elig_status",
            F.lit("prm_costs_sum_all_services").alias("metric_id"),
            "prm_costs",
        )
        .groupBy("elig_status", "metric_id")
        .agg(F.sum("prm_costs").alias("metric_value"))
    )

    memmos_summary = member_months.groupBy("member_id", "elig_status").agg(
        F.sum("memmos").alias("memmos")
    )

    trunc_costs = (
        outclaims_mem.groupBy(outclaims.member_id, "elig_status")
        .agg(F.sum("prm_costs").alias("costs"))
        .join(memmos_summary, on=["member_id", "elig_status"], how="left_outer")
        .withColumn(
            "truncation_threshold",
            F.when(
                F.col("elig_status") == "Aged Non-Dual",
                147189 * F.col("memmos") / 12,
            )
            .when(
                F.col("elig_status") == "Aged Dual",
                236991 * F.col("memmos") / 12,
            )
            .when(
                F.col("elig_status") == "Disabled",
                177548 * F.col("memmos") / 12,
            )
            .when(
                F.col("elig_status") == "ESRD",
                507077 * F.col("memmos") / 12,
            )
            .otherwise(99999999),
        )
        .select(
            "member_id",
            "elig_status",
            F.lit("prm_costs_truncated").alias("metric_id"),
            F.when(
                F.col("costs") > F.col("truncation_threshold"),
                F.col("truncation_threshold"),
            )
            .otherwise(F.col("costs"))
            .alias("costs_truncated"),
        )
        .groupBy("elig_status", "metric_id")
        .agg(F.sum("costs_truncated").alias("metric_value"))
    )

    mem_age = member_months.join(
        dfs_input["members"], on="member_id", how="inner"
    ).withColumn(
        "age_month",
        F.datediff(F.col("elig_month"), F.col("dob"))
        / 365.25,
    )

    total_age = (
        mem_age.select(
            "elig_status",
            F.lit("total_age").alias("metric_id"),
            "memmos",
            "age_month",
        )
        .groupBy("elig_status", "metric_id")
        .agg(
            F.sum(
                F.col("memmos") * F.col("age_month")
            ).alias("metric_value")
        )
    )

    wellness_visits = outclaims.where(
        F.col("hcpcs").isin(WELLNESS_HCPCS)
    ).select("member_id", "prm_fromdate")

    wellness_window = Window.partitionBy("member_id", "elig_month").orderBy(
        F.desc("prm_fromdate")
    )

    recent_wellness_visit = (
        member_months.join(wellness_visits, on="member_id", how="inner")
        .where(F.col("prm_fromdate") <= F.last_day("elig_month"))
        .withColumn("row_rank", F.row_number().over(wellness_window))
        .withColumn(
            "tag",
            F.when(
                F.add_months(F.trunc("elig_month", "month"), -11)
                <= F.col("prm_fromdate"),
                1,
            ).otherwise(0),
        )
        .where(F.col("row_rank") == 1)
    )

    max_date = recent_wellness_visit.select(F.max("elig_month")).collect()[0][
        0
    ]
    recent_wellness_visit = recent_wellness_visit.where(
        F.col("elig_month") == max_date
    )

    cnt_wellness_visits_numer = (
        recent_wellness_visit.select(
            "elig_status",
            F.lit("cnt_wellness_visits_numer").alias("metric_id"),
            recent_wellness_visit.member_id,
            "tag",
        )
        .groupBy("elig_status", "metric_id")
        .agg(F.sum("tag").alias("metric_value"))
    )

    cnt_wellness_visits_numer_all = cnt_wellness_visits_numer.select(
        F.lit("All").alias("elig_status"),
        F.lit("cnt_wellness_visits_numer").alias("metric_id"),
        F.sum("metric_value").alias("metric_value"),
    )

    cnt_wellness_visits_denom = (
        member_months.where(F.col("elig_month") == max_date)
        .select(
            "elig_status",
            F.lit("cnt_wellness_visits_denom").alias("metric_id"),
            "member_id",
        )
        .groupBy("elig_status", "metric_id")
        .agg(F.countDistinct("member_id").alias("metric_value"))
    )

    cnt_wellness_visits_denom_all = cnt_wellness_visits_denom.select(
        F.lit("All").alias("elig_status"),
        F.lit("cnt_wellness_visits_denom").alias("metric_id"),
        F.sum("metric_value").alias("metric_value"),
    )

    basic_metrics = (
        cnt_assigned_mems.union(assigned_nonesrd)
        .union(memmos_sum)
        .union(risk_score)
        .union(all_costs)
        .union(trunc_costs)
        .union(total_age)
        .union(cnt_assigned_mems_all)
        .union(cnt_wellness_visits_numer)
        .union(cnt_wellness_visits_numer_all)
        .union(cnt_wellness_visits_denom)
        .union(cnt_wellness_visits_denom_all)
        .coalesce(10)
    )

    basic_metrics.write.mode("overwrite").saveAsTable(f'{OUTPUT}.basic_metrics')
    
    return 0


if __name__ == "__main__":
    Basic()

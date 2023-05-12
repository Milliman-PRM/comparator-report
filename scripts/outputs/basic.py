"""
### CODE OWNERS: Umang Gupta, Pierre Cornell
### OBJECTIVE:
  Calculate basic metrics for members by eligibility status
      (memmos, total costs, age, etc.)
### DEVELOPER NOTES:
  None
"""
# pylint: disable=no-member
import logging

from prm.spark.app import SparkApp
import pyspark.sql.functions as spark_funcs
from prm.dates.utils import date_as_month
import comparator_report.meta.project
from pyspark.sql import Window

LOGGER = logging.getLogger(__name__)
META_SHARED = comparator_report.meta.project.gather_metadata()

NAME_MODULE = "outputs"
PATH_INPUTS = META_SHARED["path_data_nyhealth_shared"] / NAME_MODULE
PATH_RS = META_SHARED["path_data_nyhealth_shared"] / "risk_scores"
PATH_OUTPUTS = META_SHARED["path_data_comparator_report"] / NAME_MODULE
WELLNESS_HCPCS = ["G0402", "G0438", "G0439", "G0468"]
RUNOUT = 3

# =============================================================================
# LIBRARIES, LOCATIONS, LITERALS, ETC. GO ABOVE HERE
# =============================================================================


def main() -> int:
    """Calculate high level metrics by eligibility status"""
    sparkapp = SparkApp(META_SHARED["pipeline_signature"])

    dfs_input = {
        path.stem: sparkapp.load_df(path)
        for path in [
            PATH_INPUTS / "members.parquet",
            PATH_OUTPUTS / "member_months.parquet",
            PATH_INPUTS / "outclaims.parquet",
            PATH_INPUTS / "member_time_windows.parquet",
        ]
    }

    member_months = dfs_input["member_months"].where(
        spark_funcs.col("cover_medical") == "Y"
    )

    cnt_assigned_mems = (
        member_months.select(
            "elig_status",
            spark_funcs.lit("cnt_assigned_mems").alias("metric_id"),
            "member_id",
        )
        .groupBy("elig_status", "metric_id")
        .agg(spark_funcs.countDistinct("member_id").alias("metric_value"))
    )

    cnt_assigned_mems_all = (
        member_months.select(
            spark_funcs.lit("All").alias("elig_status"),
            spark_funcs.lit("cnt_assigned_mems").alias("metric_id"),
            "member_id",
        )
        .groupBy("elig_status", "metric_id")
        .agg(spark_funcs.countDistinct("member_id").alias("metric_value"))
    )

    assigned_nonesrd = (
        member_months.where(spark_funcs.col("elig_status") != "ESRD")
        .select(
            spark_funcs.lit("Non-ESRD").alias("elig_status"),
            spark_funcs.lit("cnt_assigned_mems_nonesrd").alias("metric_id"),
            "member_id",
        )
        .groupBy("elig_status", "metric_id")
        .agg(spark_funcs.countDistinct("member_id").alias("metric_value"))
    )

    memmos_sum = (
        member_months.select(
            "elig_status", spark_funcs.lit("memmos_sum").alias("metric_id"), "memmos"
        )
        .groupBy("elig_status", "metric_id")
        .agg(spark_funcs.sum("memmos").alias("metric_value"))
    )

    risk_score = (
        member_months.select(
            "elig_status",
            spark_funcs.lit("riskscr_1_avg").alias("metric_id"),
            "memmos",
            "risk_score",
        )
        .groupBy("elig_status", "metric_id")
        .agg(
            (
                spark_funcs.sum(
                    spark_funcs.col("memmos") * spark_funcs.col("risk_score")
                )
                / spark_funcs.sum("memmos")
            ).alias("metric_value")
        )
    )

    outclaims = dfs_input["outclaims"].withColumn(
        "month",
        spark_funcs.when(
            spark_funcs.col("prm_line") == "I31",
            date_as_month(spark_funcs.col("prm_fromdate_case")),
        ).otherwise(date_as_month(spark_funcs.col("prm_fromdate"))),
    )

    elig_memmos = (
        dfs_input["member_time_windows"]
        .where(spark_funcs.col("cover_medical") == "Y")
        .select("member_id", "date_start", "date_end")
    )

    outclaims_mem = outclaims.join(
        elig_memmos,
        on=[
            outclaims.member_id == elig_memmos.member_id,
            spark_funcs.col("prm_fromdate").between(
                spark_funcs.col("date_start"), spark_funcs.col("date_end")
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
            spark_funcs.lit("prm_costs_sum_all_services").alias("metric_id"),
            "prm_costs",
        )
        .groupBy("elig_status", "metric_id")
        .agg(spark_funcs.sum("prm_costs").alias("metric_value"))
    )

    memmos_summary = member_months.groupBy("member_id", "elig_status").agg(
        spark_funcs.sum("memmos").alias("memmos")
    )

    trunc_costs = (
        outclaims_mem.groupBy(outclaims.member_id, "elig_status")
        .agg(spark_funcs.sum("prm_costs").alias("costs"))
        .join(memmos_summary, on=["member_id", "elig_status"], how="left_outer")
        .withColumn(
            "truncation_threshold",
            spark_funcs.when(
                spark_funcs.col("elig_status") == "Aged Non-Dual",
                138974 * spark_funcs.col("memmos") / 12,
            )
            .when(
                spark_funcs.col("elig_status") == "Aged Dual",
                225780 * spark_funcs.col("memmos") / 12,
            )
            .when(
                spark_funcs.col("elig_status") == "Disabled",
                167346 * spark_funcs.col("memmos") / 12,
            )
            .when(
                spark_funcs.col("elig_status") == "ESRD",
                483440 * spark_funcs.col("memmos") / 12,
            )
            .otherwise(99999999),
        )
        .select(
            "member_id",
            "elig_status",
            spark_funcs.lit("prm_costs_truncated").alias("metric_id"),
            spark_funcs.when(
                spark_funcs.col("costs") > spark_funcs.col("truncation_threshold"),
                spark_funcs.col("truncation_threshold"),
            )
            .otherwise(spark_funcs.col("costs"))
            .alias("costs_truncated"),
        )
        .groupBy("elig_status", "metric_id")
        .agg(spark_funcs.sum("costs_truncated").alias("metric_value"))
    )

    mem_age = member_months.join(
        dfs_input["members"], on="member_id", how="inner"
    ).withColumn(
        "age_month",
        spark_funcs.datediff(spark_funcs.col("elig_month"), spark_funcs.col("dob"))
        / 365.25,
    )

    total_age = (
        mem_age.select(
            "elig_status",
            spark_funcs.lit("total_age").alias("metric_id"),
            "memmos",
            "age_month",
        )
        .groupBy("elig_status", "metric_id")
        .agg(
            spark_funcs.sum(
                spark_funcs.col("memmos") * spark_funcs.col("age_month")
            ).alias("metric_value")
        )
    )

    wellness_visits = outclaims.where(
        spark_funcs.col("hcpcs").isin(WELLNESS_HCPCS)
    ).select("member_id", "prm_fromdate")

    wellness_window = Window.partitionBy("member_id", "elig_month").orderBy(
        spark_funcs.desc("prm_fromdate")
    )

    recent_wellness_visit = (
        member_months.join(wellness_visits, on="member_id", how="inner")
        .where(spark_funcs.col("prm_fromdate") <= spark_funcs.last_day("elig_month"))
        .withColumn("row_rank", spark_funcs.row_number().over(wellness_window))
        .withColumn(
            "tag",
            spark_funcs.when(
                spark_funcs.add_months(spark_funcs.trunc("elig_month", "month"), -11)
                <= spark_funcs.col("prm_fromdate"),
                1,
            ).otherwise(0),
        )
        .where(spark_funcs.col("row_rank") == 1)
    )

    max_date = recent_wellness_visit.select(spark_funcs.max("elig_month")).collect()[0][
        0
    ]
    recent_wellness_visit = recent_wellness_visit.where(
        spark_funcs.col("elig_month") == max_date
    )

    cnt_wellness_visits_numer = (
        recent_wellness_visit.select(
            "elig_status",
            spark_funcs.lit("cnt_wellness_visits_numer").alias("metric_id"),
            recent_wellness_visit.member_id,
            "tag",
        )
        .groupBy("elig_status", "metric_id")
        .agg(spark_funcs.sum("tag").alias("metric_value"))
    )

    cnt_wellness_visits_numer_all = cnt_wellness_visits_numer.select(
        spark_funcs.lit("All").alias("elig_status"),
        spark_funcs.lit("cnt_wellness_visits_numer").alias("metric_id"),
        spark_funcs.sum("metric_value").alias("metric_value"),
    )

    cnt_wellness_visits_denom = (
        member_months.where(spark_funcs.col("elig_month") == max_date)
        .select(
            "elig_status",
            spark_funcs.lit("cnt_wellness_visits_denom").alias("metric_id"),
            "member_id",
        )
        .groupBy("elig_status", "metric_id")
        .agg(spark_funcs.countDistinct("member_id").alias("metric_value"))
    )

    cnt_wellness_visits_denom_all = cnt_wellness_visits_denom.select(
        spark_funcs.lit("All").alias("elig_status"),
        spark_funcs.lit("cnt_wellness_visits_denom").alias("metric_id"),
        spark_funcs.sum("metric_value").alias("metric_value"),
    )

    outclaims_mem_followup_visits = outclaims_mem.withColumn(
        "cnt_followup_visits_within_7_days_numer_yn",
        spark_funcs.when(
            spark_funcs.col("followup_visit_within_7_days_numer_yn") == "Y",
            spark_funcs.lit(1),
        ).otherwise(spark_funcs.lit(0)),
    ).withColumn(
        "cnt_followup_visit_within_7_days_denom_yn",
        spark_funcs.when(
            spark_funcs.col("followup_visit_within_7_days_denom_yn") == "Y",
            spark_funcs.lit(1),
        ).otherwise(spark_funcs.lit(0)),
    )

    cnt_followup_visits_within_7_days_numer = (
        outclaims_mem_followup_visits.select(
            "elig_status",
            spark_funcs.lit("cnt_followup_visits_within_7_days_numer").alias(
                "metric_id"
            ),
            "cnt_followup_visits_within_7_days_numer_yn",
        )
        .groupBy("elig_status", "metric_id")
        .agg(
            spark_funcs.sum("cnt_followup_visits_within_7_days_numer_yn").alias(
                "metric_value"
            )
        )
    )

    cnt_followup_visits_within_7_days_denom = (
        outclaims_mem_followup_visits.select(
            "elig_status",
            spark_funcs.lit("cnt_followup_visits_within_7_days_denom").alias(
                "metric_id"
            ),
            "cnt_followup_visit_within_7_days_denom_yn",
        )
        .groupBy("elig_status", "metric_id")
        .agg(
            spark_funcs.sum("cnt_followup_visit_within_7_days_denom_yn").alias(
                "metric_value"
            )
        )
    )

    cnt_followup_visits_within_7_days_numer_all = cnt_followup_visits_within_7_days_numer.select(
        spark_funcs.lit("All").alias("elig_status"),
        spark_funcs.lit("cnt_followup_visits_within_7_days_numer").alias("metric_id"),
        spark_funcs.sum("metric_value").alias("metric_value"),
    )

    cnt_followup_visits_within_7_days_denom_all = cnt_followup_visits_within_7_days_denom.select(
        spark_funcs.lit("All").alias("elig_status"),
        spark_funcs.lit("cnt_followup_visits_within_7_days_denom").alias("metric_id"),
        spark_funcs.sum("metric_value").alias("metric_value"),
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
        .union(cnt_followup_visits_within_7_days_numer)
        .union(cnt_followup_visits_within_7_days_numer_all)
        .union(cnt_followup_visits_within_7_days_denom)
        .union(cnt_followup_visits_within_7_days_denom_all)
        .coalesce(10)
    )

    sparkapp.save_df(basic_metrics, PATH_OUTPUTS / "basic_metrics.parquet")

    return 0


if __name__ == "__main__":
    # pylint: disable=wrong-import-position, wrong-import-order, ungrouped-imports
    import sys
    import prm.utils.logging_ext
    import prm.spark.defaults_prm

    prm.utils.logging_ext.setup_logging_stdout_handler()
    SPARK_DEFAULTS_PRM = prm.spark.defaults_prm.get_spark_defaults(META_SHARED)

    with SparkApp(META_SHARED["pipeline_signature"], **SPARK_DEFAULTS_PRM):
        RETURN_CODE = main()

    sys.exit(RETURN_CODE)

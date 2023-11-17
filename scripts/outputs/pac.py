"""
### CODE OWNERS: Umang Gupta, Pierre Cornell
### OBJECTIVE:
  Calculate PAC metrics and output PAC DRG Summary data table
### DEVELOPER NOTES:
  None
"""
# pylint: disable=no-member
import logging
import os

from prm.spark.app import SparkApp
import pyspark.sql.functions as spark_funcs
from prm.dates.utils import date_as_month
from pathlib import Path

import comparator_report.meta.project

LOGGER = logging.getLogger(__name__)
META_SHARED = comparator_report.meta.project.gather_metadata()

NAME_MODULE = "outputs"
PATH_INPUTS = META_SHARED["path_data_nyhealth_shared"] / NAME_MODULE
PATH_OUTPUTS = META_SHARED["path_data_comparator_report"] / NAME_MODULE
PATH_RISKADJ = Path(os.environ["reference_data_pathref"])

if os.environ.get('STLMT_Enabled', 'False').lower() == 'true':
    RUNOUT = 3
else: 
    RUNOUT = 9

# =============================================================================
# LIBRARIES, LOCATIONS, LITERALS, ETC. GO ABOVE HERE
# =============================================================================
def calc_pac_episode_flags(outclaims_with_case_decor: "DataFrame") -> "DataFrame":
    """Pre-calculate whether an episode has certain types of utilization"""
    pac_case_counts = outclaims_with_case_decor.filter('pac_index_yn = "N"').select(
        "*",
        spark_funcs.when(
            (
                (spark_funcs.col("pac_major_category") == "IP")
                & (spark_funcs.col("pac_minor_category") == "Acute")
            ),
            spark_funcs.col("prm_util"),
        )
        .otherwise(spark_funcs.lit(0))
        .alias("pac_ip_acute_days"),
        spark_funcs.when(
            (
                (spark_funcs.col("pac_major_category") == "IP")
                & (spark_funcs.col("pac_minor_category") == "Rehab")
            ),
            spark_funcs.col("prm_util"),
        )
        .otherwise(spark_funcs.lit(0))
        .alias("pac_ip_rehab_days"),
        spark_funcs.when(
            spark_funcs.col("pac_major_category") == "SNF", spark_funcs.col("prm_util")
        )
        .otherwise(spark_funcs.lit(0))
        .alias("pac_snf_days"),
        spark_funcs.when(
            spark_funcs.col("pac_major_category") == "HH", spark_funcs.col("prm_util")
        )
        .otherwise(spark_funcs.lit(0))
        .alias("pac_hh_visits"),
    )
    pac_case_summary = (
        pac_case_counts.groupBy("pac_caseadmitid")
        .agg(
            spark_funcs.sum("pac_ip_acute_days").alias("pac_ip_acute_days"),
            spark_funcs.sum("pac_ip_rehab_days").alias("pac_ip_rehab_days"),
            spark_funcs.sum("pac_snf_days").alias("pac_snf_days"),
            spark_funcs.sum("pac_hh_visits").alias("pac_hh_visits"),
        )
        .select(
            "pac_caseadmitid",
            spark_funcs.when(
                spark_funcs.col("pac_ip_acute_days") > 0, spark_funcs.lit("Y")
            )
            .otherwise("N")
            .alias("pac_has_ip_acute_yn"),
            spark_funcs.when(
                spark_funcs.col("pac_ip_rehab_days") > 0, spark_funcs.lit("Y")
            )
            .otherwise("N")
            .alias("pac_has_ip_rehab_yn"),
            spark_funcs.when(spark_funcs.col("pac_snf_days") > 0, spark_funcs.lit("Y"))
            .otherwise("N")
            .alias("pac_has_snf_yn"),
            spark_funcs.when(spark_funcs.col("pac_hh_visits") > 0, spark_funcs.lit("Y"))
            .otherwise("N")
            .alias("pac_has_hh_yn"),
        )
    )
    return pac_case_summary


def calc_pac_metrics(outclaims: "DataFrame") -> "DataFrame":
    """Calculate metrics using flags calculated in calc_pac_episode_flags"""
    outclaims_pac = outclaims.where(spark_funcs.col("pac_index_yn") == "Y")

    pac_count = (
        outclaims_pac.select(
            "elig_status", spark_funcs.lit("pac_epi").alias("metric_id"), "prm_admits"
        )
        .groupBy("elig_status", "metric_id")
        .agg(spark_funcs.sum("prm_admits").alias("metric_value"))
    )

    pac_died = (
        outclaims_pac.where(spark_funcs.col("pac_died_in_hospital_yn") == "Y")
        .select(
            "elig_status",
            spark_funcs.lit("pac_died_in_hosp").alias("metric_id"),
            "prm_admits",
        )
        .groupBy("elig_status", "metric_id")
        .agg(spark_funcs.sum("prm_admits").alias("metric_value"))
    )

    pac_ipreadmit = (
        outclaims_pac.where(spark_funcs.col("pac_has_ip_acute_yn") == "Y")
        .select(
            "elig_status",
            spark_funcs.lit("pac_ipreadmit").alias("metric_id"),
            "prm_admits",
        )
        .groupBy("elig_status", "metric_id")
        .agg(spark_funcs.sum("prm_admits").alias("metric_value"))
    )

    pac_snf = (
        outclaims_pac.where(spark_funcs.col("pac_has_snf_yn") == "Y")
        .select(
            "elig_status", spark_funcs.lit("pac_snf").alias("metric_id"), "prm_admits"
        )
        .groupBy("elig_status", "metric_id")
        .agg(spark_funcs.sum("prm_admits").alias("metric_value"))
    )

    pac_rehab = (
        outclaims_pac.where(spark_funcs.col("pac_has_ip_rehab_yn") == "Y")
        .select(
            "elig_status", spark_funcs.lit("pac_rehab").alias("metric_id"), "prm_admits"
        )
        .groupBy("elig_status", "metric_id")
        .agg(spark_funcs.sum("prm_admits").alias("metric_value"))
    )

    pac_hh = (
        outclaims_pac.where(spark_funcs.col("pac_has_hh_yn") == "Y")
        .select(
            "elig_status", spark_funcs.lit("pac_hh").alias("metric_id"), "prm_admits"
        )
        .groupBy("elig_status", "metric_id")
        .agg(spark_funcs.sum("prm_admits").alias("metric_value"))
    )

    pac_followup_visits_within_7_days_numer_med = (
        outclaims_pac.select(
            "elig_status",
            spark_funcs.lit("cnt_followup_visits_within_7_days_numer_med").alias(
                "metric_id"
            ),
            spark_funcs.when(
                (
                    (spark_funcs.col("followup_visit_within_7_days_numer_yn") == "Y")
                    & (spark_funcs.col("prm_line") == "I11a")
                ),
                spark_funcs.lit(1),
            )
            .otherwise(spark_funcs.lit(0))
            .alias("cnt_followup_visits_within_7_days_numer_yn"),
        )
        .groupBy("elig_status", "metric_id")
        .agg(
            spark_funcs.sum("cnt_followup_visits_within_7_days_numer_yn").alias(
                "metric_value"
            )
        )
    )

    pac_followup_visits_within_7_days_denom_med = (
        outclaims_pac.select(
            "elig_status",
            spark_funcs.lit("cnt_followup_visits_within_7_days_denom_med").alias(
                "metric_id"
            ),
            spark_funcs.when(
                (
                    (spark_funcs.col("followup_visit_within_7_days_denom_yn") == "Y")
                    & (spark_funcs.col("prm_line") == "I11a")
                ),
                spark_funcs.lit(1),
            )
            .otherwise(spark_funcs.lit(0))
            .alias("cnt_followup_visits_within_7_days_denom_yn"),
        )
        .groupBy("elig_status", "metric_id")
        .agg(
            spark_funcs.sum("cnt_followup_visits_within_7_days_denom_yn").alias(
                "metric_value"
            )
        )
    )

    pac_followup_visits_within_7_days_numer_sur = (
        outclaims_pac.select(
            "elig_status",
            spark_funcs.lit("cnt_followup_visits_within_7_days_numer_sur").alias(
                "metric_id"
            ),
            spark_funcs.when(
                (
                    (spark_funcs.col("followup_visit_within_7_days_numer_yn") == "Y")
                    & (spark_funcs.col("prm_line") == "I12")
                ),
                spark_funcs.lit(1),
            )
            .otherwise(spark_funcs.lit(0))
            .alias("cnt_followup_visits_within_7_days_numer_yn"),
        )
        .groupBy("elig_status", "metric_id")
        .agg(
            spark_funcs.sum("cnt_followup_visits_within_7_days_numer_yn").alias(
                "metric_value"
            )
        )
    )

    pac_followup_visits_within_7_days_denom_sur = (
        outclaims_pac.select(
            "elig_status",
            spark_funcs.lit("cnt_followup_visits_within_7_days_denom_sur").alias(
                "metric_id"
            ),
            spark_funcs.when(
                (
                    (spark_funcs.col("followup_visit_within_7_days_denom_yn") == "Y")
                    & (spark_funcs.col("prm_line") == "I12")
                ),
                spark_funcs.lit(1),
            )
            .otherwise(spark_funcs.lit(0))
            .alias("cnt_followup_visits_within_7_days_denom_yn"),
        )
        .groupBy("elig_status", "metric_id")
        .agg(
            spark_funcs.sum("cnt_followup_visits_within_7_days_denom_yn").alias(
                "metric_value"
            )
        )
    )

    pac_metrics = (
        pac_count.union(pac_died)
        .union(pac_ipreadmit)
        .union(pac_snf)
        .union(pac_rehab)
        .union(pac_hh)
        .union(pac_followup_visits_within_7_days_numer_med)
        .union(pac_followup_visits_within_7_days_denom_med)
        .union(pac_followup_visits_within_7_days_numer_sur)
        .union(pac_followup_visits_within_7_days_denom_sur)
        .coalesce(10)
    )

    return pac_metrics


def main() -> int:
    """Pass outclaims to PAC metric calculation functions"""
    sparkapp = SparkApp(META_SHARED["pipeline_signature"])

    dfs_input = {
        path.stem: sparkapp.load_df(path)
        for path in [
            PATH_OUTPUTS / "member_months.parquet",
            PATH_INPUTS / "member_time_windows.parquet",
            PATH_INPUTS / "outclaims.parquet",
            PATH_INPUTS / "decor_case.parquet",
            PATH_INPUTS / "members.parquet",
            PATH_INPUTS / "ref_pac_benchmarks.parquet",
        ]
    }

    member_months = dfs_input["member_months"].where(
        spark_funcs.col("cover_medical") == "Y"
    )

    elig_memmos = (
        dfs_input["member_time_windows"]
        .where(spark_funcs.col("cover_medical") == "Y")
        .select("member_id", "date_start", "date_end")
    )

    outclaims = (
        dfs_input["outclaims"]
        .withColumn(
            "month",
            spark_funcs.when(
                spark_funcs.col("prm_line") == "I31",
                date_as_month(spark_funcs.col("prm_fromdate_case")),
            ).otherwise(date_as_month(spark_funcs.col("prm_fromdate"))),
        )
        .withColumn(
            "died_in_hospital",
            spark_funcs.when(
                (
                    (spark_funcs.col("dischargestatus") == "20")
                    & (spark_funcs.col("prm_line").startswith("I"))
                    & (spark_funcs.col("prm_line") != "I31")
                ),
                spark_funcs.lit("Y"),
            ).otherwise(spark_funcs.lit("N")),
        )
    )

    died_in_hosp = (
        outclaims.select("member_id", "died_in_hospital")
        .where('died_in_hospital = "Y"')
        .distinct()
    )

    mem_death = dfs_input["members"].join(died_in_hosp, on="member_id", how="inner")

    pac_case_summary = calc_pac_episode_flags(outclaims)

    pac_flags = (
        outclaims.join(pac_case_summary, on="pac_caseadmitid", how="left_outer")
        .join(mem_death, on="member_id", how="left_outer")
        .withColumn(
            "pac_died_in_hospital_yn",
            spark_funcs.when(
                spark_funcs.col("death_date")
                <= spark_funcs.col("pac_episode_end_date"),
                spark_funcs.lit("Y"),
            ).otherwise(spark_funcs.lit("N")),
        )
    )

    pac_elig_drgs = (
        dfs_input["ref_pac_benchmarks"]
        .where(spark_funcs.col("benchmarks_pac_freq_wm_readm").isNotNull())
        .select(spark_funcs.col("msdrg").alias("prm_drg"))
    )

    pac_flags_trim = (
        pac_flags.join(
            elig_memmos,
            on=[
                outclaims.member_id == elig_memmos.member_id,
                spark_funcs.col("prm_fromdate").between(
                    spark_funcs.col("date_start"), spark_funcs.col("date_end")
                ),
            ],
            how="inner",
        )
        .join(
            member_months,
            on=(pac_flags.member_id == member_months.member_id)
            & (pac_flags.month == member_months.elig_month),
            how="inner",
        )
        .join(pac_elig_drgs, on="prm_drg", how="inner")
    )

    pac_metrics = calc_pac_metrics(pac_flags_trim)

    pac_drg_summary = (
        pac_flags_trim.where('pac_index_yn = "Y"')
        .select(
            "elig_status",
            "prm_drg",
            "pac_index_yn",
            "pac_has_ip_acute_yn",
            "pac_has_ip_rehab_yn",
            "pac_has_snf_yn",
            "pac_has_hh_yn",
            "pac_died_in_hospital_yn",
            "prm_admits",
        )
        .groupBy("elig_status", "prm_drg")
        .agg(
            spark_funcs.sum("prm_admits").alias("pac_count"),
            spark_funcs.sum(
                spark_funcs.when(
                    spark_funcs.col("pac_has_ip_acute_yn") == "Y",
                    spark_funcs.col("prm_admits"),
                ).otherwise(0)
            ).alias("pac_acute_count"),
            spark_funcs.sum(
                spark_funcs.when(
                    spark_funcs.col("pac_has_ip_rehab_yn") == "Y",
                    spark_funcs.col("prm_admits"),
                ).otherwise(0)
            ).alias("pac_rehab_count"),
            spark_funcs.sum(
                spark_funcs.when(
                    spark_funcs.col("pac_has_snf_yn") == "Y",
                    spark_funcs.col("prm_admits"),
                ).otherwise(0)
            ).alias("pac_snf_count"),
            spark_funcs.sum(
                spark_funcs.when(
                    spark_funcs.col("pac_has_hh_yn") == "Y",
                    spark_funcs.col("prm_admits"),
                ).otherwise(0)
            ).alias("pac_hh_count"),
            spark_funcs.sum(
                spark_funcs.when(
                    spark_funcs.col("pac_died_in_hospital_yn") == "Y",
                    spark_funcs.col("prm_admits"),
                ).otherwise(0)
            ).alias("pac_death_count"),
        )
    )

    sparkapp.save_df(pac_metrics, PATH_OUTPUTS / "pac_metrics.parquet")

    sparkapp.save_df(pac_drg_summary, PATH_OUTPUTS / "pac_drg_summary.parquet")

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

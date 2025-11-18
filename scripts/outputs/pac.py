"""
### CODE OWNERS: Umang Gupta, Pierre Cornell
### OBJECTIVE:
  Calculate PAC metrics and output PAC DRG Summary data table
### DEVELOPER NOTES:
  None
"""
# pylint: disable=no-member
from databricks.sdk.runtime import *
import pyspark.sql.functions as F

INPUT = 'permier_data_feed'
OUTPUT = 'premier_data_feed'
PATH_REF = 'comparator_references'

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

def calc_pac_metrics(outclaims: "DataFrame") -> "DataFrame":
    """Calculate metrics using flags calculated in calc_pac_episode_flags"""
    outclaims_pac = outclaims

    pac_count = (
        outclaims_pac.select(
            "elig_status", F.lit("pac_epi").alias("metric_id"), "prm_admits"
        )
        .groupBy("elig_status", "metric_id")
        .agg(F.sum("prm_admits").alias("metric_value"))
    )

    pac_died = (
        outclaims_pac.where(F.col("pac_died_in_hospital_yn") == "Y")
        .select(
            "elig_status",
            F.lit("pac_died_in_hosp").alias("metric_id"),
            "prm_admits",
        )
        .groupBy("elig_status", "metric_id")
        .agg(F.sum("prm_admits").alias("metric_value"))
    )

    pac_ipreadmit = (
        outclaims_pac.where(F.col("pac_has_ip_acute_yn") == "Y")
        .select(
            "elig_status",
            F.lit("pac_ipreadmit").alias("metric_id"),
            "prm_admits",
        )
        .groupBy("elig_status", "metric_id")
        .agg(F.sum("prm_admits").alias("metric_value"))
    )

    pac_snf = (
        outclaims_pac.where(F.col("pac_has_snf_yn") == "Y")
        .select(
            "elig_status", F.lit("pac_snf").alias("metric_id"), "prm_admits"
        )
        .groupBy("elig_status", "metric_id")
        .agg(F.sum("prm_admits").alias("metric_value"))
    )

    pac_rehab = (
        outclaims_pac.where(F.col("pac_has_ip_rehab_yn") == "Y")
        .select(
            "elig_status", F.lit("pac_rehab").alias("metric_id"), "prm_admits"
        )
        .groupBy("elig_status", "metric_id")
        .agg(F.sum("prm_admits").alias("metric_value"))
    )

    pac_hh = (
        outclaims_pac.where(F.col("pac_has_hh_yn") == "Y")
        .select(
            "elig_status", F.lit("pac_hh").alias("metric_id"), "prm_admits"
        )
        .groupBy("elig_status", "metric_id")
        .agg(F.sum("prm_admits").alias("metric_value"))
    )

    pac_followup_visits_within_7_days_numer_med = (
        outclaims_pac.select(
            "elig_status",
            F.lit("cnt_followup_visits_within_7_days_numer_med").alias(
                "metric_id"
            ),
            F.when(
                (
                    (F.col("followup_visit_within_7_days_numer_yn") == "Y")
                    & (F.col("prm_line") == "I11a")
                ),
                F.lit(1),
            )
            .otherwise(F.lit(0))
            .alias("cnt_followup_visits_within_7_days_numer_yn"),
        )
        .groupBy("elig_status", "metric_id")
        .agg(
            F.sum("cnt_followup_visits_within_7_days_numer_yn").alias(
                "metric_value"
            )
        )
    )

    pac_followup_visits_within_7_days_denom_med = (
        outclaims_pac.select(
            "elig_status",
            F.lit("cnt_followup_visits_within_7_days_denom_med").alias(
                "metric_id"
            ),
            F.when(
                (
                    (F.col("followup_visit_within_7_days_denom_yn") == "Y")
                    & (F.col("prm_line") == "I11a")
                ),
                F.lit(1),
            )
            .otherwise(F.lit(0))
            .alias("cnt_followup_visits_within_7_days_denom_yn"),
        )
        .groupBy("elig_status", "metric_id")
        .agg(
            F.sum("cnt_followup_visits_within_7_days_denom_yn").alias(
                "metric_value"
            )
        )
    )

    pac_followup_visits_within_7_days_numer_sur = (
        outclaims_pac.select(
            "elig_status",
            F.lit("cnt_followup_visits_within_7_days_numer_sur").alias(
                "metric_id"
            ),
            F.when(
                (
                    (F.col("followup_visit_within_7_days_numer_yn") == "Y")
                    & (F.col("prm_line") == "I12")
                ),
                F.lit(1),
            )
            .otherwise(F.lit(0))
            .alias("cnt_followup_visits_within_7_days_numer_yn"),
        )
        .groupBy("elig_status", "metric_id")
        .agg(
            F.sum("cnt_followup_visits_within_7_days_numer_yn").alias(
                "metric_value"
            )
        )
    )

    pac_followup_visits_within_7_days_denom_sur = (
        outclaims_pac.select(
            "elig_status",
            F.lit("cnt_followup_visits_within_7_days_denom_sur").alias(
                "metric_id"
            ),
            F.when(
                (
                    (F.col("followup_visit_within_7_days_denom_yn") == "Y")
                    & (F.col("prm_line") == "I12")
                ),
                F.lit(1),
            )
            .otherwise(F.lit(0))
            .alias("cnt_followup_visits_within_7_days_denom_yn"),
        )
        .groupBy("elig_status", "metric_id")
        .agg(
            F.sum("cnt_followup_visits_within_7_days_denom_yn").alias(
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


def PAC() -> int:
    """Pass outclaims to PAC metric calculation functions"""
    dfs_input = {
        path: spark.table(f"{OUTPUT}.{path}")
        for path in [
           "member_months",
           "member_time_windows",
           "outclaims_full",
           "members",
           "pac"
        ]
    }

    ref_pac_benchmarks = spark.table('main.vbci_ref_2025_11_07.vbci_ref_pac_benchmarks')

    member_months = dfs_input["member_months"].where(
        F.col("cover_medical") == "Y"
    )

    elig_memmos = (
        dfs_input["member_time_windows"]
        .where(F.col("cover_medical") == "Y")
        .select("member_id", "date_start", "date_end")
    )

    outclaims = (
        dfs_input["outclaims_full"]
        .withColumn(
            "month",
            date_as_month(F.col("prm_fromdate")),
        )
    )

    pac_case_summary = dfs_input['pac'].withColumn(
        'pac_util',
        F.when(
            F.col('pac_setting').isin(["IP","SNF","IRF"]),
            F.col("pac_days")
        ).when(
            F.col('pac_setting').isin(['HH']),
            F.col('pac_visits')
        ).otherwise(F.lit(0))
    ).groupBy(
        'caseadmitid',
        'pac_died_in_hospital_yn'
    ).pivot(
        'pac_setting',
    ).sum(
        'pac_util'
    ).select(
        'caseadmitid',
        F.when(F.col('HH') > 0, F.lit('Y')).otherwise(F.lit('N')).alias('pac_has_hh_yn'),
        F.when(F.col('SNF') > 0, F.lit('Y')).otherwise(F.lit('N')).alias('pac_has_snf_yn'),
        F.when(F.col('IP') > 0, F.lit('Y')).otherwise(F.lit('N')).alias('pac_has_ip_acute_yn'),
        F.when(F.col('IRF') > 0, F.lit('Y')).otherwise(F.lit('N')).alias('pac_has_ip_rehab_yn'),
        'pac_died_in_hospital_yn'
    )

    pac_flags = outclaims.join(pac_case_summary, on="caseadmitid", how="inner")

    pac_elig_drgs = (
        ref_pac_benchmarks
        .where(F.col("BENCHMARKS_PAC_FREQ_WM_READM") > 0)
        .where(F.col("HCG_TYPE") == "Medicare")
        .select(F.col("MSDRG").alias("drg"))
    )

    pac_flags_trim = (
        pac_flags.join(
            elig_memmos,
            on=[
                outclaims.member_id == elig_memmos.member_id,
                F.col("prm_fromdate").between(
                    F.col("date_start"), F.col("date_end")
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
        .join(pac_elig_drgs, on="drg", how="inner").withColumnRenamed('drg','prm_drg')
    )

    pac_metrics = calc_pac_metrics(pac_flags_trim)

    pac_drg_summary = (
        pac_flags_trim
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
            F.sum("prm_admits").alias("pac_count"),
            F.sum(
                F.when(
                    F.col("pac_has_ip_acute_yn") == "Y",
                    F.col("prm_admits"),
                ).otherwise(0)
            ).alias("pac_acute_count"),
            F.sum(
                F.when(
                    F.col("pac_has_ip_rehab_yn") == "Y",
                    F.col("prm_admits"),
                ).otherwise(0)
            ).alias("pac_rehab_count"),
            F.sum(
                F.when(
                    F.col("pac_has_snf_yn") == "Y",
                    F.col("prm_admits"),
                ).otherwise(0)
            ).alias("pac_snf_count"),
            F.sum(
                F.when(
                    F.col("pac_has_hh_yn") == "Y",
                    F.col("prm_admits"),
                ).otherwise(0)
            ).alias("pac_hh_count"),
            F.sum(
                F.when(
                    F.col("pac_died_in_hospital_yn") == "Y",
                    F.col("prm_admits"),
                ).otherwise(0)
            ).alias("pac_death_count"),
        )
    )
    pac_metrics.write.mode("overwrite").saveAsTable(f'{OUTPUT}.pac_metrics')

    pac_drg_summary.write.mode("overwrite").saveAsTable(f'{OUTPUT}.pac_drg_summary')

    return 0


if __name__ == "__main__":
    PAC()
"""
### CODE OWNERS: Giang Vu, Ocean Liu
### OBJECTIVE:
  Add count of HCC markers for each member
### DEVELOPER NOTES:
  None
"""
import logging
import os
import datetime
import typing
from prm.spark.app import SparkApp
import pyspark.sql.functions as spark_funcs

from pyspark.sql import Window
import comparator_report.meta.project
import prm_ny_data_share.meta.project
import prm.riskscr.hcc
from prm.dates.windows import ClaimDateWindow
import cms_hcc.pyspark_api

from prm.spark.io_txt import export_csv #temporary just to export csv for review

LOGGER = logging.getLogger(__name__)
META_SHARED = prm_ny_data_share.meta.project.gather_metadata()
META_COMPARATOR = comparator_report.meta.project.gather_metadata()

NAME_MODULE = 'outputs'
PATH_INPUTS = META_SHARED['path_data_nyhealth_shared'] / NAME_MODULE
PATH_OUTPUTS = META_COMPARATOR['path_data_comparator_report'] / NAME_MODULE

HCC_COLS = [
    "hcc1",
    "hcc2",
    "hcc6",
    "hcc8",
    "hcc9",
    "hcc10",
    "hcc11",
    "hcc12",
    "hcc17",
    "hcc18",
    "hcc19",
    "hcc21",
    "hcc22",
    "hcc23",
    "hcc27",
    "hcc28",
    "hcc29",
    "hcc33",
    "hcc34",
    "hcc35",
    "hcc39",
    "hcc40",
    "hcc46",
    "hcc47",
    "hcc48",
    "hcc51",
    "hcc52",
    "hcc54",
    "hcc55",
    "hcc56",
    "hcc57",
    "hcc58",
    "hcc59",
    "hcc60",
    "hcc70",
    "hcc71",
    "hcc72",
    "hcc73",
    "hcc74",
    "hcc75",
    "hcc76",
    "hcc77",
    "hcc78",
    "hcc79",
    "hcc80",
    "hcc82",
    "hcc83",
    "hcc84",
    "hcc85",
    "hcc86",
    "hcc87",
    "hcc88",
    "hcc96",
    "hcc99",
    "hcc100",
    "hcc103",
    "hcc104",
    "hcc106",
    "hcc107",
    "hcc108",
    "hcc110",
    "hcc111",
    "hcc112",
    "hcc114",
    "hcc115",
    "hcc122",
    "hcc124",
    "hcc135",
    "hcc136",
    "hcc137",
    "hcc138",
    "hcc157",
    "hcc158",
    "hcc159",
    "hcc161",
    "hcc162",
    "hcc166",
    "hcc167",
    "hcc169",
    "hcc170",
    "hcc173",
    "hcc176",
    "hcc186",
    "hcc188",
    "hcc189",
]

# =============================================================================
# LIBRARIES, LOCATIONS, LITERALS, ETC. GO ABOVE HERE
# =============================================================================

def _create_time_periods(
    sparkapp: SparkApp, meta_shared: typing.Mapping[str, typing.Any]
) -> cms_hcc.pyspark_api.TimePeriods:
    """Create time periods input parameter for CMS-HCC processing"""
    modeling_windows = sparkapp.load_df(PATH_INPUTS / "time_periods.parquet").where(
        spark_funcs.col("time_period_id").isin(["202312"]) #set time period to 2023, can modify later
    )

    iter_time_windows = modeling_windows.collect()
    time_periods = []
    for time_window in iter_time_windows:
        diag_date_window = ClaimDateWindow(
            incstart=time_window.risk_score_date_start,
            incend=time_window.risk_score_date_end,
            paidthru=max(
                meta_shared["date_latestpaid"], time_window.risk_score_date_end
            ),
        )
        payment_date_window = ClaimDateWindow(
            incstart=time_window.reporting_date_start,
            incend=time_window.reporting_date_end,
            paidthru=max(
                meta_shared["date_latestpaid"], time_window.reporting_date_end
            ),
        )
        process_params = cms_hcc.pyspark_api.ProcessParams(
            diag_date_range=diag_date_window,
            payment_date_range=payment_date_window,
            version=prm.riskscr.hcc.HCC_VERSION,
            esrd_version=prm.riskscr.hcc.ESRD_VERSION,
            name_period=time_window.time_period_id,
            date_as_of=payment_date_window.incstart + datetime.timedelta(days=31),
        )
        time_periods.append(process_params)
    return time_periods


def main() -> int:
    """Create a table with count of HCC markers for each member"""
    sparkapp = SparkApp(META_SHARED['pipeline_signature'])
    
    dfs_input_hcc = {
        "outclaims": sparkapp.load_df(
            META_SHARED[73, "out"] / "outclaims_prm.parquet"
        ),
        "member": sparkapp.load_df(
            META_SHARED[35, "out"] / "member.parquet"
        ),
        "member_time": sparkapp.load_df(
            META_SHARED[35, "out"] / "member_time.parquet"
        ),
    }
    
    time_periods = _create_time_periods(
        sparkapp, META_SHARED
    )  ### Ensure same paid through date for all ACOs
    
    hcc_results = prm.riskscr.hcc.calc_hccs(sparkapp, dfs_input_hcc, time_periods) 
    
    dfs_input = {
            path.stem: sparkapp.load_df(path)
        for path in [
            PATH_OUTPUTS / 'member_months.parquet',
            PATH_INPUTS / 'member_time_windows.parquet',
        ]
    }
    
    hcc_count_results = hcc_results["feature_info"].where(
            spark_funcs.col("feature_name").isin(HCC_COLS)
    ).groupBy(
            "member_id"
    ).agg(
            spark_funcs.count((spark_funcs.col("feature_name"))).alias("hcc_count")
    ).withColumn(
            "hcc_count_bin",
            spark_funcs.when(
                    spark_funcs.col("hcc_count") > 5,
                    spark_funcs.lit("hcc_count_6_and_above")
            ).otherwise(
                    spark_funcs.concat(spark_funcs.lit("hcc_count_"), spark_funcs.lit(spark_funcs.col("hcc_count")))
            )
    )
    
    #aggregate results by elig status
    hcc_feature_results = hcc_results["feature_info"].where(
            spark_funcs.col("feature_name").isin(HCC_COLS)
    )
    
    member_months = dfs_input['member_months'].where(
        spark_funcs.col('cover_medical') == 'Y'
    ).select(
        spark_funcs.col('member_id').alias('member_id_memmos'),
        'elig_month',
        'elig_status',
    )
    
    elig_memmos = dfs_input['member_time_windows'].where(
        spark_funcs.col('cover_medical') == 'Y'
    ).select(
        spark_funcs.col('member_id').alias('member_id_elig'),
        'date_start',
        'date_end',
    )
    
    hcc_mem_results = hcc_feature_results.join(
        elig_memmos,
        on=[
            hcc_feature_results.member_id == elig_memmos.member_id_elig,
            spark_funcs.col('latest_date_coded').between(
                spark_funcs.col('date_start'),
                spark_funcs.col('date_end'),
            )
            ],
        how='inner',
    ).join(
        member_months,
        on=(hcc_feature_results.member_id == member_months.member_id_memmos)
        & (spark_funcs.col('elig_month').between(
                spark_funcs.col('date_start'),
                spark_funcs.col('date_end'))
            ), 
        how='inner'
    ).select(
        spark_funcs.col("member_id"),
        spark_funcs.col("elig_status")
    ).distinct()
    #join with elig memmos & then member months to get elig status of each member for a specific month
    
    #then join with hcc count per member to count number of members for each hcc_count bin per elig status
    hcc_count_elig = hcc_mem_results.join(
        hcc_count_results,
        on = "member_id",
        how= "left"
    ).groupBy(
            spark_funcs.col("elig_status"),
            spark_funcs.col("hcc_count_bin").alias("metric_id")
    ).agg(
            spark_funcs.count((spark_funcs.col("member_id"))).alias("metric_value")
    )
    
    #export hcc count per member for overview of their distribution before continuing putting them into bins
#    export_csv(
#        hcc_count_results,
#        PATH_OUTPUTS / "hcc_count_by_member.csv",
#        header = True,
#        single_file = True,
#        line_endings = "\n"
#    )
    
    #diag date range: incstart=2022-01-01,incend=2022-12-31
    
    sparkapp.save_df(
        hcc_count_elig,
        PATH_OUTPUTS / 'risk_scores_metrics.parquet',
    )
    
    return 0

if __name__ == '__main__':
    # pylint: disable=wrong-import-position, wrong-import-order, ungrouped-imports
    import sys
    import prm.utils.logging_ext
    import prm.spark.defaults_prm

    prm.utils.logging_ext.setup_logging_stdout_handler()
    SPARK_DEFAULTS_PRM = prm.spark.defaults_prm.get_spark_defaults(META_SHARED)

    with SparkApp(META_SHARED['pipeline_signature'], **SPARK_DEFAULTS_PRM):
        RETURN_CODE = main()

    sys.exit(RETURN_CODE)

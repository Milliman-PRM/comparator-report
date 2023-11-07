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
            PATH_INPUTS / "outclaims.parquet"
        ),
        "member": sparkapp.load_df(
            PATH_INPUTS / "members.parquet"
        ),
        "member_time": sparkapp.load_df(
            PATH_INPUTS / "member_time_windows.parquet"
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
        ]
    }
    
    #count hcc markers per member_id
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
                    spark_funcs.concat(spark_funcs.lit("hcc_count_"), spark_funcs.col("hcc_count").cast("string"))
            )
    )
    
    #get latest elig status of each member ID
    member_months = dfs_input['member_months'].where(
        spark_funcs.col('cover_medical') == 'Y'
    ).select(
        'member_id',
        'elig_month',
        'elig_status',
    )
    
    flag_window = Window.partitionBy("member_id").orderBy(
            spark_funcs.col("elig_month").desc()
    )
    
    elig_memmos = member_months.withColumn(
            "rank",
            spark_funcs.row_number().over(flag_window)
    ).where(
            spark_funcs.col("rank") == 1
    )
    
    #left join elig memmos with hcc count results to also include members without any HCC marker
    #hcc_mem_results contain all member_id that have cover_medical = Y, their latest elig_status and their number of hcc markers
    hcc_mem_results = elig_memmos.join(
        hcc_count_results,
        on= "member_id",
        how='left',
    ).select(
        spark_funcs.col("member_id"),
        spark_funcs.col("elig_status"),
        spark_funcs.coalesce(spark_funcs.col("hcc_count"), spark_funcs.lit(0)).alias("hcc_count"),
        spark_funcs.coalesce(spark_funcs.col("hcc_count_bin"), spark_funcs.lit("hcc_count_0")).alias("hcc_count_bin")
    )

    #then aggregate hcc_mem_results to count number of members for each hcc_count bin per elig status
    mem_count_per_bin_per_elig = hcc_mem_results.groupBy(
            spark_funcs.col("elig_status"),
            spark_funcs.col("hcc_count_bin").alias("metric_id")
    ).agg(
            spark_funcs.count((spark_funcs.col("member_id"))).alias("metric_value")
    )
    
    #find average number of hcc count per elig status
    hcc_count_avg_per_elig = hcc_mem_results.groupBy(
             spark_funcs.col("elig_status"),
             spark_funcs.lit("avg_hcc_count").alias("metric_id")
    ).agg(
            spark_funcs.mean(spark_funcs.col("hcc_count")).alias("metric_value")
    )
    
    #find number of members per each hcc marker, grouped by elig status. 
    #inner join with elig_memmos because there are members that are not in member_months/elig_memmos but have hcc markers in hcc_results
    mem_count_per_hcc = hcc_results["feature_info"].where(
            spark_funcs.col("feature_name").isin(HCC_COLS)
    ).join(
            elig_memmos,
            on = "member_id",
            how = "inner"
    ).select(
            spark_funcs.col("member_id"),
            spark_funcs.col("elig_status"),
            spark_funcs.col("feature_name")
    ).groupBy(
            spark_funcs.col("elig_status"),
            spark_funcs.concat(spark_funcs.col("feature_name"), spark_funcs.lit("_member_count")).alias("metric_id")
    ).agg(
            spark_funcs.countDistinct(spark_funcs.col("member_id")).alias("metric_value")
    )
    
    #stack all calculation on top of each other for final risk score output
    risk_scores_metric_out = mem_count_per_bin_per_elig.union(
            hcc_count_avg_per_elig
    ).union(
            mem_count_per_hcc
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
        risk_scores_metric_out,
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

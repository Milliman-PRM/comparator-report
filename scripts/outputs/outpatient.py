"""
### CODE OWNERS: Umang Gupta, Pierre Cornell
### OBJECTIVE:
  Create Membership basis for all other queries
### DEVELOPER NOTES:
  None
"""
import logging

from prm.spark.app import SparkApp
import pyspark.sql.functions as spark_funcs
from prm.dates.utils import date_as_month
from prm.spark.io_sas import read_sas_data

import comparator_report.meta.project

LOGGER = logging.getLogger(__name__)
META_SHARED = comparator_report.meta.project.gather_metadata()

NAME_MODULE = 'outputs'
PATH_INPUTS = META_SHARED['path_data_nyhealth_shared'] / NAME_MODULE
PATH_OUTPUTS = META_SHARED['path_data_comparator_report'] / NAME_MODULE
PATH_RISKADJ = META_SHARED[15, 'out']

runout = 3

# =============================================================================
# LIBRARIES, LOCATIONS, LITERALS, ETC. GO ABOVE HERE
# =============================================================================

def metric_calc(
        outclaims: "DataFrame",
        risk_score: "DataFrame",
        hcc_risk_adj: "DataFrame",
        mr_line: "String",
        metric_id: "String",
        ) -> "DataFrame":
    
    outclaims_mcrm = outclaims.withColumn(
                'mcrm_line',
                spark_funcs.col('prm_line').substr(1, 3)
            ).where(
                spark_funcs.col('mcrm_line') == mr_line
            )
    
    util = outclaims_mcrm.select(
                'elig_status',
                'mcrm_line',
                'prm_util',
            ).groupBy(
                'elig_status',
                'mcrm_line',
            ).agg(
                spark_funcs.sum('prm_util').alias('util')
            )
    
    util_adj = util.join(
                risk_score,
                on='elig_status',
                how='inner',
            ).join(
                hcc_risk_adj,
                on='mcrm_line',
                how='left_outer',
            ).where(
                spark_funcs.col('risk_score_avg').between(
                    spark_funcs.col('hcc_range_bottom'),
                    spark_funcs.col('hcc_range_top')
                )
            ).withColumn(
                'util_riskadj',
                spark_funcs.col('util')*spark_funcs.col('factor_util')
            )
                
    metric = util_adj.select(
                'elig_status',
                spark_funcs.lit(metric_id).alias('metric_id'),
                'util'
            ).groupBy(
                'elig_status',
                'metric_id',
            ).agg(
                spark_funcs.sum('util').alias('metric_value')
            ).union(
                util_adj.select(
                    'elig_status',
                    spark_funcs.lit(metric_id + '_riskadj').alias('metric_id'),
                    'util_riskadj',
                ).groupBy(
                    'elig_status',
                    'metric_id',
                ).agg(
                    spark_funcs.sum('util_riskadj').alias('metric_value')
                )
            )
    
    return metric

def main() -> int:
    sparkapp = SparkApp(META_SHARED['pipeline_signature'])
    
    dfs_input = {
            path.stem: sparkapp.load_df(path)
            for path in [
                    PATH_OUTPUTS / 'member_months.parquet',
                    PATH_INPUTS / 'time_periods.parquet',
                    PATH_INPUTS / 'outclaims.parquet',
                    ]
            }
    
    min_incurred_date, max_incurred_date = dfs_input['time_periods'].where(
            spark_funcs.col('months_of_claims_runout') == runout
            ).select(
                    spark_funcs.col('reporting_date_start').alias('min_incurred_date'),
                    spark_funcs.col('reporting_date_end').alias('max_incurred_date'),
            ).collect()[0]
    
    member_months = dfs_input['member_months']

    risk_score = member_months.select(
                'elig_status',
                'memmos',
                'risk_score',
            ).groupBy(
                'elig_status',
            ).agg(
                spark_funcs.format_number((spark_funcs.sum(spark_funcs.col('memmos')*spark_funcs.col('risk_score')) / spark_funcs.sum(spark_funcs.col('memmos'))), 2).alias('risk_score_avg')
            )
        
    outclaims = dfs_input['outclaims'].where(
            spark_funcs.col('prm_fromdate').between(
                    min_incurred_date,
                    max_incurred_date,)
            ).withColumn(
                'month',
                date_as_month(spark_funcs.col('prm_fromdate'))
            ).where(
                spark_funcs.col('prm_line').substr(1,3).isin(
                    'O14', 'O10', 'P57', 'P59', 'P33', 'P32',
                    )
            )
            
    outclaims_mem = outclaims.join(
                    member_months,
                    on=(outclaims.member_id == member_months.member_id)
                       & (outclaims.month == member_months.elig_month),
                    how = 'inner'
            )
    
    hcc_risk_adj = read_sas_data(
            sparkapp,
            PATH_RISKADJ / 'mcrm_hcc_calibrations.sas7bdat',
            )
    
    hi_tec_img = metric_calc(outclaims_mem, risk_score, hcc_risk_adj, 'O14', 'high_tech_imaging')
    obs_stays = metric_calc(outclaims_mem, risk_score, hcc_risk_adj, 'O10', 'observation_stays')
    hi_tec_img_fop = metric_calc(outclaims_mem, risk_score, hcc_risk_adj, 'P57', 'hi_tec_img_fop')
    hi_tec_img_office = metric_calc(outclaims_mem, risk_score, hcc_risk_adj, 'P59', 'hi_tec_img_office')
    urg_care = metric_calc(outclaims_mem, risk_score, hcc_risk_adj, 'P33', 'urgent_care_prof')
    office_vis = metric_calc(outclaims_mem, risk_score, hcc_risk_adj, 'P32', 'office_visits')
    
    outpatient_metrics = hi_tec_img.union(
                obs_stays
            ).union(
                hi_tec_img_fop
            ).union(
                hi_tec_img_office
            ).union(
                urg_care
            ).union(
                office_vis
            )
       
    sparkapp.save_df(
            outpatient_metrics,
            PATH_OUTPUTS / 'outpatient_metrics.parquet',
            )

    hcc_risk_adj.unpersist()

    return 0

if __name__ == '__main__':
    import sys
    import prm.utils.logging_ext
    import prm.spark.defaults_prm
    
    prm.utils.logging_ext.setup_logging_stdout_handler()
    SPARK_DEFAULTS_PRM = prm.spark.defaults_prm.get_spark_defaults(META_SHARED)
    
    with SparkApp(META_SHARED['pipeline_signature'], **SPARK_DEFAULTS_PRM):
        RETURN_CODE = main()
    
    sys.exit(RETURN_CODE)

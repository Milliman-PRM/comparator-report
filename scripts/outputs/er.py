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

def nyu_measures(
        outclaims: "DataFrame",
        prm_col: "String",
        metric_id: "String",
        ) -> "DataFrame":
    
    metric = outclaims.select(
                'elig_status',
                spark_funcs.lit(metric_id).alias('metric_id'),
                'prm_util',
                spark_funcs.col(prm_col).alias('nyu_cat'),
            ).groupBy(
                'elig_status',
                'metric_id',
            ).agg(
                spark_funcs.sum(spark_funcs.col('prm_util') * spark_funcs.col('nyu_cat')).alias('metric_value')
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
                    PATH_INPUTS / 'decor_case.parquet',
                    ]
            }
    
    min_incurred_date, max_incurred_date = dfs_input['time_periods'].where(
            spark_funcs.col('months_of_claims_runout') == runout
            ).select(
                    spark_funcs.col('reporting_date_start').alias('min_incurred_date'),
                    spark_funcs.col('reporting_date_end').alias('max_incurred_date'),
            ).collect()[0]
    
    member_months = dfs_input['member_months']

    decor_limited = dfs_input['decor_case'].select(
            'member_id',
            'caseadmitid',
            *[
                column
                for column in dfs_input['decor_case'].columns
                if column.startswith('prm_nyu_')
            ],
            )

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
            ).join(
                decor_limited,
                on=['member_id', 'caseadmitid'],
                how='left_outer'
            ).withColumn(
                'month',
                date_as_month(spark_funcs.col('prm_fromdate'))
            ).where(
                spark_funcs.col('prm_line').substr(1,3) == 'O11'
            ).withColumn(
                'mcrm_line',
                spark_funcs.lit('O11')
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
    
    util = outclaims_mem.select(
                'elig_status',
                'mcrm_line',
                spark_funcs.lit('ED').alias('metric_id'),
                'prm_util',
            ).groupBy(
                'elig_status',
                'mcrm_line',
                'metric_id',
            ).agg(
                spark_funcs.sum('prm_util').alias('metric_value')
            )
    
    util_riskadj = util.join(
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
                        spark_funcs.col('hcc_range_top'),
                )
            ).withColumn(
                'util_riskadj',
                spark_funcs.col('metric_value')*spark_funcs.col('factor_util')
            )
    
    non_avoidable = nyu_measures(outclaims_mem, 'prm_nyu_emergent_non_avoidable', 'ED_emer_nec')
    avoidable = nyu_measures(outclaims_mem, 'prm_nyu_emergent_avoidable', 'ED_emer_prev')
    primary_care = nyu_measures(outclaims_mem, 'prm_nyu_emergent_primary_care', 'ED_emer_pricare')
    injury = nyu_measures(outclaims_mem, 'prm_nyu_injury', 'ED_injury')
    nonemergent = nyu_measures(outclaims_mem, 'prm_nyu_nonemergent', 'ED_nonemer')
    unclassified = nyu_measures(outclaims_mem, 'prm_nyu_unclassified', 'ED_other')
       
    er_metrics = util.select(
                    'elig_status',
                    'metric_id',
                    'metric_value',
                ).union(
                    util_riskadj.select(
                        'elig_status',
                        spark_funcs.lit('ED_rskadj').alias('metric_id'),
                        spark_funcs.col('util_riskadj').alias('metric_value'),
                    )
                ).union(
                    non_avoidable
                ).union(
                    avoidable
                ).union(
                    primary_care
                ).union(
                    injury
                ).union(
                    nonemergent
                ).union(
                    unclassified
                )
                
    
    sparkapp.save_df(
            er_metrics,
            PATH_OUTPUTS / 'er_metrics.parquet',
            )

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

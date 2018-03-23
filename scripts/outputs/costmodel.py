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
PATH_REFS = META_SHARED[15, 'out']

runout = 3

# =============================================================================
# LIBRARIES, LOCATIONS, LITERALS, ETC. GO ABOVE HERE
# =============================================================================

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
    
    time_period = str(min_incurred_date.year) + 'Q' + str((min_incurred_date.month - 1) // 3 + 1) + '_' + str(max_incurred_date.year) + 'Q' + str((max_incurred_date.month - 1) // 3 + 1)
                
    member_months = dfs_input['member_months']

    ref_link_mcrm_line = read_sas_data(
        sparkapp,
        PATH_REFS / 'link_mr_mcrm_line.sas7bdat',
    )
    
    outclaims = dfs_input['outclaims'].where(
            spark_funcs.col('prm_fromdate').between(
                    min_incurred_date,
                    max_incurred_date,)
            ).withColumn(
                'month',
                date_as_month(spark_funcs.col('prm_fromdate'))
            )
            
    outclaims_mem = outclaims.join(
                member_months,
                on=(outclaims.member_id == member_months.member_id)
                   & (outclaims.month == member_months.elig_month),
                how = 'inner'
            ).join(
                ref_link_mcrm_line.where(spark_funcs.col('lob') == 'Medicare'),
                on=(outclaims.prm_line == ref_link_mcrm_line.mr_line),
                how='inner',
            )
    
    costmodel = outclaims_mem.select(
                spark_funcs.lit(META_SHARED['name_client']).alias('name_client'),
                spark_funcs.lit(time_period).alias('time_period'),
                'prm_line',
                'mcrm_line',
                'elig_status',
                spark_funcs.col('prm_oon_yn').alias('prv_net_aco_yn'),
                'prm_admits',
                'prm_util',
                'prm_costs',
                spark_funcs.lit('Medical').alias('prm_coverage_type'),
            ).groupBy(
                'name_client',
                'time_period',
                'elig_status',
                'prm_line',
                'mcrm_line',
                'prv_net_aco_yn',
                'prm_coverage_type',
            ).agg(
                spark_funcs.sum('prm_admits').alias('prm_discharges'),
                spark_funcs.sum('prm_util').alias('prm_days'),
                spark_funcs.sum('prm_util').alias('prm_util'),
                spark_funcs.sum('prm_costs').alias('prm_costs'),
                spark_funcs.sum('prm_costs').alias('prm_allowed'),
                spark_funcs.sum('prm_costs').alias('prm_paid'),
            ).select(
                'name_client',
                'time_period',
                'prm_line',
                'mcrm_line',
                'elig_status',
                'prv_net_aco_yn',
                'prm_discharges',
                'prm_days',
                'prm_util',
                'prm_costs',
                'prm_allowed',
                'prm_paid',
                'prm_coverage_type',
            ).withColumn(
                'idx',
                spark_funcs.concat(
                    spark_funcs.col('name_client'),
                    spark_funcs.lit('_'),
                    spark_funcs.col('time_period'),
                    spark_funcs.lit('_'),                    
                    spark_funcs.col('elig_status'),
                    spark_funcs.lit('_'),                    
                    spark_funcs.when(
                        spark_funcs.col('mcrm_line') == 'x99',
                        spark_funcs.lit('oth'),
                        ).otherwise(
                            spark_funcs.col('prm_line')
                        ),
                    spark_funcs.lit('_'),                    
                    spark_funcs.when(
                        spark_funcs.col('mcrm_line') == 'x99',
                        spark_funcs.lit('oth'),
                        ).otherwise(
                            spark_funcs.col('mcrm_line')
                        )
                )
            )
            
    sparkapp.save_df(
            costmodel,
            PATH_OUTPUTS / 'costmodel.parquet',
            )

    ref_link_mcrm_line.unpersist()

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

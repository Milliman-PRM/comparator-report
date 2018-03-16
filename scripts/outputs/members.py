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
import comparator_report.meta.project

LOGGER = logging.getLogger(__name__)
META_SHARED = comparator_report.meta.project.gather_metadata()

NAME_MODULE = 'outputs'
PATH_INPUTS = META_SHARED['path_data_nyhealth_shared'] / NAME_MODULE
PATH_RS = META_SHARED['path_data_nyhealth_shared'] / 'risk_scores'
PATH_OUTPUTS = META_SHARED['path_data_comparator_report'] / NAME_MODULE

runout = 3

# =============================================================================
# LIBRARIES, LOCATIONS, LITERALS, ETC. GO ABOVE HERE
# =============================================================================

def main() -> int:
    sparkapp = SparkApp(META_SHARED['pipeline_signature'])
    
    dfs_input = {
            path.stem: sparkapp.load_df(path)
            for path in [
                    PATH_INPUTS / 'member_time_windows.parquet',
                    PATH_INPUTS / 'time_periods.parquet',
                    PATH_RS / 'risk_scores.parquet'
                    ]
            }
    
    min_incurred_date, max_incurred_date = dfs_input['time_periods'].where(
            spark_funcs.col('months_of_claims_runout') == runout
            ).select(
                    spark_funcs.col('reporting_date_start').alias('min_incurred_date'),
                    spark_funcs.col('reporting_date_end').alias('max_incurred_date'),
            ).collect()[0]
       
    member_months = dfs_input['member_time_windows'].filter(
            spark_funcs.col('elig_month').between(
                    min_incurred_date,
                    max_incurred_date,)
            ).filter(
                'assignment_indicator = "Y"'
            )
        
    risk_scores = dfs_input['risk_scores'].join(
            dfs_input['time_periods'].where(spark_funcs.col('months_of_claims_runout') == runout),
            on='time_period_id',
            how='inner')
    
    member_months_sum = member_months.select(
            'member_id',
            'elig_month',
            'memmos_medical',
            ).groupBy(
                'member_id'
            ).agg(
                spark_funcs.max('elig_month').alias('max_elig_month'),
                spark_funcs.sum('memmos_medical').alias('mms'),
            ).withColumnRenamed(
                    'max_elig_month',
                    'elig_month',
            )
       
    members = member_months_sum.join(
                risk_scores,
                on='member_id',
                how='left_outer'
            ).join(
                member_months,
                on=['member_id', 'elig_month'],
                how='left_outer'
            ).select(
                    'member_id',
                    spark_funcs.col('elig_status_1').alias('elig_status'),
                    'risk_score',
                    'mms',
            )
       
    sparkapp.save_df(
            member_months,
            PATH_OUTPUTS / 'member_months_cr.parquet',
            )
    
    sparkapp.save_df(
            members,
            PATH_OUTPUTS / 'members_cr.parquet',
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
    
    sys.exist(RETURN_CODE)

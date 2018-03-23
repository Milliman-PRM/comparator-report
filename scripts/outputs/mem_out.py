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
                    PATH_INPUTS / 'members.parquet',
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
    
    member_summary = member_months.select(
                'member_id',
                'elig_status',
                'memmos',
                'risk_score'
            ).groupBy(
                'member_id',
                'elig_status',
            ).agg(
                spark_funcs.sum(spark_funcs.col('memmos') * spark_funcs.col('risk_score')).alias('riskscr_wt')
            )
    
    member_age = member_summary.join(
                dfs_input['members'],
                on='member_id',
                how='left_outer',
            ).select(
                member_summary.member_id,
                'elig_status',
                'riskscr_wt',
                'dob',
                spark_funcs.lit(max_incurred_date).alias('age_date'),
            ).withColumn(
                'age',
                spark_funcs.datediff(spark_funcs.col('age_date'),
                                     spark_funcs.col('dob')) / 365.25
            )

    mem_out = member_age.select(
                'elig_status',
                'member_id',
                'riskscr_wt',
                'age',
            ).groupBy(
                'elig_status',
            ).agg(
                spark_funcs.sum('riskscr_wt').alias('riskscr_wgt'),
                spark_funcs.sum('age').alias('Total_Age'),
                spark_funcs.count('member_id').alias('memcnt'),
            ).select(
                spark_funcs.lit(META_SHARED['name_client']).alias('name_client'),
                spark_funcs.lit(time_period).alias('time_period'),
                'elig_status',
                spark_funcs.lit('').alias('deceased_yn'),
                spark_funcs.lit('').alias('deceased_hospital_yn'),
                spark_funcs.lit('').alias('age_avg'),
                spark_funcs.lit('').alias('endoflife_numer_yn_chemolt14days'),
                spark_funcs.lit('').alias('endoflife_denom_yn_chemolt14days'),
                spark_funcs.lit('').alias('endoflife_numer_yn_hospicelt3day'),
                spark_funcs.lit('').alias('endoflife_denom_yn_hospicelt3day'),
                spark_funcs.lit('').alias('endoflife_numer_yn_hospicenever'),
                spark_funcs.lit('').alias('endoflife_denom_yn_hospicenever'),
                spark_funcs.lit('').alias('final_hospice_days'),
                spark_funcs.lit('').alias('costs_final_30_days_sum'),
                spark_funcs.lit('').alias('risk_score_type'),
                'riskscr_wgt',
                spark_funcs.lit('').alias('riskscr_cred'),
                'memcnt',
                'Total_Age',
            ).withColumn(
                'idx',
                spark_funcs.concat(
                    spark_funcs.col('name_client'),
                    spark_funcs.lit('_'),
                    spark_funcs.col('time_period'),
                    spark_funcs.lit('_'),
                    spark_funcs.col('elig_status')
                )
            )              

    sparkapp.save_df(
            mem_out,
            PATH_OUTPUTS / 'mem_out.parquet',
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

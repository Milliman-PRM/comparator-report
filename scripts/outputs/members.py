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

# =============================================================================
# LIBRARIES, LOCATIONS, LITERALS, ETC. GO ABOVE HERE
# =============================================================================

def main() -> int:
    sparkapp = SparkApp(META_SHARED['pipeline_signature'])
    
    dfs_input = {
            path.stem: sparkapp.load_df(path)
            for path in [
                    PATH_INPUTS / 'members.parquet',
                    PATH_INPUTS / 'members_rolling.parquet',
                    PATH_INPUTS / 'member_months.parquet',
                    PATH_INPUTS / 'time_periods.parquet'
                    ]
            }
    
    min_incurred_date, max_incurred_date = dfs_input['time_periods'].filter(
            'months_of_claims_runout = 3'
            ).select(
                    spark_funcs.col('reporting_date_start').alias('min_incurred_date'),
                    spark_funcs.col('reporting_date_end').alias('max_incurred_date'),
            ).collect()[0]
    
    quarter_start, quarter_end = [str((dt.month - 1) // 3 + 1) for dt in [min_incurred_date, max_incurred_date]]
    time_span = ''.join([str(min_incurred_date.year), 'Q', quarter_start, '_', str(max_incurred_date.year), 'Q', quarter_end])
    
    max_member_months = dfs_input['member_months'].filter(
            spark_funcs.col('month').between(
                    min_incurred_date,
                    max_incurred_date,)
            ).select(
                    'member_id', 
                    'month',
            ).groupBy(
                    'member_id',
            ).agg(
                    spark_funcs.max('month').alias('max_elig'),
            )
            
    elig_status = dfs_input['member_months'].join(
                max_member_months,
                on=(dfs_input['member_months'].member_id == max_member_months.member_id) 
                & (dfs_input['member_months'].month == max_member_months.max_elig),
                how='inner',
        ).select(
               dfs_input['member_months'].member_id, 
               spark_funcs.col('elig_status_1_timeline'),
        )
    
    members = dfs_input['members'].join(
            elig_status,
            on='member_id',
            how='leftouter',
        ).join(
            dfs_input['members_rolling'].filter(spark_funcs.col('month_rolling') == max_incurred_date),
            on = 'member_id',
            how = 'leftouter',
        ).filter(
            'assignment_indicator_current = "Y"'
        ).select(
            dfs_input['members'].member_id,       
            spark_funcs.lit(META_SHARED['name_client']).alias('name_client'),
            spark_funcs.lit(time_span).alias('time_period'),
            spark_funcs.col('elig_status_1_timeline').alias('elig_status'),
            spark_funcs.when(
                    spark_funcs.col('death_date').isNull(),
                    spark_funcs.lit('N'),
                    ).otherwise(
                            spark_funcs.lit('Y')
            ).alias('deceased_yn'),
            spark_funcs.col('endoflife_died_in_hospital').alias('deceased_hospital_yn'),
            spark_funcs.col('age_current').alias('age'),
            ##ChemoFlagNumer
            spark_funcs.when(
                    spark_funcs.col('death_date').isNull(),
                    spark_funcs.lit('N'),
                    ).otherwise(
                            spark_funcs.lit('Y')
            ).alias('eol_denom_yn_chemo14'),
            spark_funcs.col('endoflife_numer_yn_hospicelt3day').alias('eol_numer_yn_hospice3'),
            spark_funcs.col('endoflife_denom_yn_hospicelt3day').alias('eol_denom_yn_hospice3'),
            spark_funcs.col('endoflife_numer_yn_hospicenever').alias('eol_numer_yn_hospicenever'),
            spark_funcs.col('endoflife_denom_yn_hospicenever').alias('eol_denom_yn_hospicenever'),
            spark_funcs.col('hospice_days').alias('final_hospice_days'),
            spark_funcs.col('endoflife_costs_last_30_days').alias('eol_costs_30_days'),
            spark_funcs.lit('CMS HCC Risk Score').alias('risk_score_type'),
            spark_funcs.col('risk_score_rolling').alias('riskscr'),
            spark_funcs.col('memmos_rolling').alias('memmos'),
            spark_funcs.lit('').alias('riskscr_cred'),
            spark_funcs.lit(1).alias('mem_count'),                       
        ).filter(
            'memmos != 0'
        )

    sparkapp.save_df(
            members,
            PATH_OUTPUTS / 'members.parquet',
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

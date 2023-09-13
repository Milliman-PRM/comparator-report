"""
### CODE OWNERS: Umang Gupta, Pierre Cornell
### OBJECTIVE:
  Create Membership basis for all other queries
### DEVELOPER NOTES:
  None
"""
# pylint: disable=no-member
import logging
import os

from datetime import date
from prm.dates.utils import date_as_month
from prm.spark.app import SparkApp
import pyspark.sql.functions as spark_funcs
from pyspark.sql import Window
import comparator_report.meta.project

LOGGER = logging.getLogger(__name__)
META_SHARED = comparator_report.meta.project.gather_metadata()

NAME_MODULE = 'outputs'
PATH_INPUTS = META_SHARED['path_data_nyhealth_shared'] / NAME_MODULE
PATH_OUTPUTS = META_SHARED['path_data_comparator_report'] / NAME_MODULE

RUNOUT = 3

ESRD_REV = ['0820','0821','0822','0823','0824','0825','0826','0829','0830','0831','0832','0833','0834','0835','0839','0840','0841','0842','0843','0844','0845','0849','0850','0851','0852','0853','0854','0855','0859',]
ESRD_HCPCS = ['90963','90964','90965','90966',]
ESRD_BILLTYPE = '72'
# =============================================================================
# LIBRARIES, LOCATIONS, LITERALS, ETC. GO ABOVE HERE
# =============================================================================
def flag_esrd(
    outclaims,
    min_incurred_date,
    max_incurred_date
    ) -> "DataFrame":
    """Calculate whether a member should be flagged as ESRD"""
    esrd_claims = outclaims.where(
        (spark_funcs.col('hcpcs').isin(ESRD_HCPCS)) |
        (spark_funcs.col('revcode').isin(ESRD_REV)) |
        (spark_funcs.col('billtype').startswith(ESRD_BILLTYPE))
    ).select(
        'member_id',
        date_as_month('prm_fromdate').alias('esrd_month'),
    ).distinct()

    esrd_cont_window = Window.partitionBy(
        'member_id',
    ).orderBy(
        'esrd_month',
    )
    
    continuous_esrd = esrd_claims.withColumn(
        'row_number',
        spark_funcs.row_number().over(esrd_cont_window)
    ).select(
        'member_id',
        'esrd_month',
        (
            spark_funcs.year('esrd_month') * 12
            + spark_funcs.month('esrd_month')
            - spark_funcs.col('row_number')
        ).alias('grouped_monthid')
    )
        
    elig_esrd = continuous_esrd.groupBy(
        'member_id',
        'grouped_monthid'
    ).agg(
        spark_funcs.count('esrd_month').alias('cont_months')
    ).where(
        spark_funcs.col('cont_months') >= 3
    )
    
    return continuous_esrd.join(
        elig_esrd,
        on=['member_id', 'grouped_monthid'],
        how='inner'
    ).where(
        spark_funcs.col('esrd_month').between(
            min_incurred_date,
            max_incurred_date
        )
    ).select(
        spark_funcs.col('member_id').alias('member_id_esrd')
    ).distinct()
    
def main() -> int:
    """Create a member month table for assigned members"""
    sparkapp = SparkApp(META_SHARED['pipeline_signature'])

    dfs_input = {
        path.stem: sparkapp.load_df(path)
        for path in [
            PATH_INPUTS / 'member_time_windows.parquet',
            PATH_INPUTS / 'time_periods.parquet',
            PATH_INPUTS / 'members.parquet',
            PATH_INPUTS / 'risk_scores.parquet',
            PATH_INPUTS / 'outclaims.parquet',
        ]
    }

    min_incurred_date, max_incurred_date = dfs_input['time_periods'].where(
        spark_funcs.col('months_of_claims_runout') == RUNOUT
    ).select(
        spark_funcs.col('reporting_date_start').alias('min_incurred_date'),
        spark_funcs.col('reporting_date_end').alias('max_incurred_date'),
    ).collect()[0]

    if os.environ.get('YTD_Only', 'False').lower() == 'true':
        min_incurred_date = date(
            max_incurred_date.year,
            1,
            1
        )

    if os.environ.get('Currently_Assigned_Enabled', 'False').lower() == 'true':
        memmos_filter = spark_funcs.col('elig_month').between(
                            min_incurred_date,
                            max_incurred_date,
                        )
    else:
        memmos_filter = (spark_funcs.col('elig_month').between(
                            min_incurred_date,
                            max_incurred_date
                        )) & (spark_funcs.col('assignment_indicator') == 'Y')

    member_months = dfs_input['member_time_windows'].filter(
        memmos_filter
    ).groupBy(
        'member_id',
        'elig_month',
    ).agg(
        spark_funcs.sum('memmos_medical').alias('memmos')
    )

    recent_info_window = Window().partitionBy(
        'member_id',
        'elig_month',
    ).orderBy(
        spark_funcs.desc('date_end'),
    )

    recent_info = dfs_input['member_time_windows'].filter(
        memmos_filter
    ).select(
        '*',
        spark_funcs.row_number().over(recent_info_window).alias('order'),
    ).filter(
        'order = 1'
    )

    risk_scores = dfs_input['risk_scores'].join(
        dfs_input['time_periods'].where(spark_funcs.col('months_of_claims_runout') == RUNOUT),
        on='time_period_id',
        how='inner'
    )

    esrd_mems = flag_esrd(
        dfs_input['outclaims'],
        min_incurred_date,
        max_incurred_date
    )

    if os.environ.get('Currently_Assigned_Enabled', 'False').lower() == 'true':
        current_assigned = dfs_input['members'].filter(
            spark_funcs.col('assignment_indicator') == 'Y'
        ).select(
            'member_id',
            'dob',
        )        
        
        member_join = member_months.join(
            recent_info,
            on=['member_id', 'elig_month'],
            how='inner'
        ).join(
            risk_scores,
            on='member_id',
            how='left_outer'
        ).join(
            current_assigned,
            on='member_id',
            how='inner',
        ).join(
            esrd_mems,
            on=(member_months.member_id==esrd_mems.member_id_esrd),
            how='left_outer',
        ).withColumn(
            'eligibility_type',
            spark_funcs.when(
                spark_funcs.col('member_id_esrd').isNotNull(),
                spark_funcs.lit('ESRD'),
            ).when(
                max_incurred_date.year - spark_funcs.year('dob') < 65,
                spark_funcs.lit('Under 65')
            ).otherwise(
                spark_funcs.lit('Aged')
            )
        ).select(
            'member_id',
            'elig_month',
            spark_funcs.concat_ws(
                ' - ',
                spark_funcs.col('elig_status_1'),
                spark_funcs.col('eligibility_type'),
            ).alias('elig_status'),
            member_months.memmos,
            'risk_score',
            spark_funcs.when(
                member_months.memmos > 0,
                spark_funcs.lit('Y'),
            ).otherwise(
                spark_funcs.lit('N')
            ).alias('cover_medical'),
        ).where(
            spark_funcs.col('elig_status') != 'Unknown'
        )
    else:
        member_join = member_months.join(
            recent_info,
            on=['member_id', 'elig_month'],
            how='inner'
        ).join(
            risk_scores,
            on='member_id',
            how='left_outer'
        ).join(
            dfs_input['members'].select('member_id', 'dob'),
            on='member_id',
            how='left_outer',
        ).join(
            esrd_mems,
            on=(member_months.member_id==esrd_mems.member_id_esrd),
            how='left_outer',
        ).withColumn(
            'eligibility_type',
            spark_funcs.when(
                spark_funcs.col('member_id_esrd').isNotNull(),
                spark_funcs.lit('ESRD'),
            ).when(
                max_incurred_date.year - spark_funcs.year('dob') < 65,
                spark_funcs.lit('Under 65')
            ).otherwise(
                spark_funcs.lit('Aged')
            )
        ).select(
            'member_id',
            'elig_month',
            spark_funcs.concat_ws(
                ' - ',
                spark_funcs.col('elig_status_1'),
                spark_funcs.col('eligibility_type'),
            ).alias('elig_status'),
            member_months.memmos,
            'risk_score',
            spark_funcs.when(
                member_months.memmos > 0,
                spark_funcs.lit('Y'),
            ).otherwise(
                spark_funcs.lit('N')
            ).alias('cover_medical'),
        ).where(
            spark_funcs.col('elig_status') != 'Unknown'
        )


    sparkapp.save_df(
        member_join,
        PATH_OUTPUTS / 'member_months.parquet',
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

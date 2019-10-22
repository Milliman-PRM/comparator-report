"""
### CODE OWNERS: Umang Gupta, Pierre Cornell
### OBJECTIVE:
  Calculate Betos summary by prm_line and runout date. 
### DEVELOPER NOTES:
  None
"""
# pylint: disable=no-member
import logging
import os

from datetime import date
from prm.spark.app import SparkApp
import pyspark.sql.functions as spark_funcs
from prm.dates.utils import date_as_month
from datetime import timedelta

import comparator_report.meta.project

LOGGER = logging.getLogger(__name__)
META_SHARED = comparator_report.meta.project.gather_metadata()

NAME_MODULE = 'outputs'
PATH_INPUTS = META_SHARED['path_data_nyhealth_shared'] / NAME_MODULE
PATH_OUTPUTS = META_SHARED['path_data_comparator_report'] / NAME_MODULE

RUNOUT = 3

# =============================================================================
# LIBRARIES, LOCATIONS, LITERALS, ETC. GO ABOVE HERE
# =============================================================================


def main() -> int:
    """Calculate Betos summary by prm_line and runout date"""
    sparkapp = SparkApp(META_SHARED['pipeline_signature'])

    dfs_input = {
        path.stem: sparkapp.load_df(path)
        for path in [
            PATH_INPUTS / 'time_periods.parquet',
            PATH_INPUTS / 'outclaims.parquet',
            PATH_INPUTS / 'member_time_windows.parquet',
            PATH_INPUTS / 'members.parquet',            
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
    
    qexpu_runout_7 = max_incurred_date + timedelta(days=7)
    qexpu_runout_14 = max_incurred_date + timedelta(days=14)

    members_ca = dfs_input['members'].where(
        spark_funcs.col('assignment_indicator') == 'Y'
    ).select(
        'member_id'
    )

    member_months = dfs_input['member_time_windows'].join(
        members_ca,
        on='member_id',
        how='inner',
    ).where(
        spark_funcs.col('cover_medical') == 'Y'
    )

    outclaims = dfs_input['outclaims'].where(
        (spark_funcs.col('prm_fromdate').between(
            min_incurred_date,
            max_incurred_date,
        )) |
        (spark_funcs.col('prm_todate').between(
            min_incurred_date,
            max_incurred_date,
        ))    
    ).withColumn(
        'month',
        spark_funcs.when(
            spark_funcs.col('prm_fromdate').between(
                min_incurred_date,
                max_incurred_date,
            ),
            date_as_month(spark_funcs.col('prm_fromdate')),
        ).otherwise(
            date_as_month(spark_funcs.col('prm_todate'))
        )
    ).withColumn(
        'runout_7_yn',
        spark_funcs.when(
            spark_funcs.col('paiddate') <= qexpu_runout_7,
            spark_funcs.lit('Y')
        ).otherwise(
            spark_funcs.lit('N')
        )
    ).withColumn(
        'runout_14_yn',
        spark_funcs.when(
            spark_funcs.col('paiddate') <= qexpu_runout_14,
            spark_funcs.lit('Y')
        ).otherwise(
            spark_funcs.lit('N')
        )
    ).withColumn(
        'fromdate_elig_yn',
        spark_funcs.when(
            spark_funcs.col('prm_fromdate').between(
                min_incurred_date,
                max_incurred_date
            ),
            spark_funcs.lit('Y')
        ).otherwise(
            spark_funcs.lit('N')
        )
    ).withColumn(
        'todate_elig_yn',
        spark_funcs.when(
            spark_funcs.col('prm_todate').between(
                min_incurred_date,
                max_incurred_date,
            ),
            spark_funcs.lit('Y')
        ).otherwise(
            spark_funcs.lit('N')
        )
    )

    outclaims_mem = outclaims.join(
        member_months,
        on=[
            outclaims.member_id == member_months.member_id,
            spark_funcs.col('prm_fromdate').between(
                spark_funcs.col('date_start'),
                spark_funcs.col('date_end')
            )
        ],
        how='inner'
    )

    betos_summary = outclaims_mem.groupBy(
        'prm_betos_code',
        'prm_line',
        'runout_7_yn',
        'runout_14_yn',
        'fromdate_elig_yn',
        'todate_elig_yn',
        spark_funcs.col('elig_status_1').alias('elig_status'),
    ).agg(
        spark_funcs.sum('prm_costs').alias('costs')
    )

    sparkapp.save_df(
        betos_summary,
        PATH_OUTPUTS / 'betos.parquet',
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

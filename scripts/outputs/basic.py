"""
### CODE OWNERS: Umang Gupta, Pierre Cornell
### OBJECTIVE:
  Calculate basic metrics for members by eligibility status
      (memmos, total costs, age, etc.)
### DEVELOPER NOTES:
  None
"""
# pylint: disable=no-member
import logging

from prm.spark.app import SparkApp
import pyspark.sql.functions as spark_funcs
from prm.dates.utils import date_as_month
import comparator_report.meta.project

LOGGER = logging.getLogger(__name__)
META_SHARED = comparator_report.meta.project.gather_metadata()

NAME_MODULE = 'outputs'
PATH_INPUTS = META_SHARED['path_data_nyhealth_shared'] / NAME_MODULE
PATH_RS = META_SHARED['path_data_nyhealth_shared'] / 'risk_scores'
PATH_MEMTIME = META_SHARED[18, 'out']
PATH_OUTPUTS = META_SHARED['path_data_comparator_report'] / NAME_MODULE

RUNOUT = 3

# =============================================================================
# LIBRARIES, LOCATIONS, LITERALS, ETC. GO ABOVE HERE
# =============================================================================

def main() -> int:
    """Calculate high level metrics by eligibility status"""
    sparkapp = SparkApp(META_SHARED['pipeline_signature'])

    dfs_input = {
        path.stem: sparkapp.load_df(path)
        for path in [
            PATH_INPUTS / 'members.parquet',
            PATH_OUTPUTS / 'member_months.parquet',
            PATH_INPUTS / 'outclaims.parquet',
            PATH_INPUTS / 'time_periods.parquet',
            PATH_MEMTIME / 'client_member_time.parquet',
        ]
    }

    min_incurred_date, max_incurred_date = dfs_input['time_periods'].where(
        spark_funcs.col('months_of_claims_runout') == RUNOUT
    ).select(
        spark_funcs.col('reporting_date_start').alias('min_incurred_date'),
        spark_funcs.col('reporting_date_end').alias('max_incurred_date'),
    ).collect()[0]

    member_months = dfs_input['member_months']

    attrib_lives = dfs_input['client_member_time'].where(
        (spark_funcs.col('date_start').between(
            min_incurred_date,
            max_incurred_date,
        )) &
        (spark_funcs.col('assignment_indicator') == 'Y')
    ).select(
        spark_funcs.lit('All').alias('elig_status'),
        spark_funcs.lit('cnt_attrib_lives').alias('metric_id'),
        'member_id',
    ).groupBy(
        'elig_status',
        'metric_id',
    ).agg(
        spark_funcs.countDistinct('member_id')
    )

    cnt_assigned_mems = member_months.select(
        'elig_status',
        spark_funcs.lit('cnt_assigned_mems').alias('metric_id'),
        'member_id',
    ).groupBy(
        'elig_status',
        'metric_id',
    ).agg(
        spark_funcs.countDistinct('member_id').alias('metric_value')
    )

    memmos_sum = member_months.select(
        'elig_status',
        spark_funcs.lit('memmos_sum').alias('metric_id'),
        'memmos',
    ).groupBy(
        'elig_status',
        'metric_id',
    ).agg(
        spark_funcs.sum('memmos').alias('metric_value')
    )

    risk_score = member_months.select(
        'elig_status',
        spark_funcs.lit('riskscr_1_avg').alias('metric_id'),
        'memmos',
        'risk_score',
    ).groupBy(
        'elig_status',
        'metric_id',
    ).agg(
        (spark_funcs.sum(spark_funcs.col('memmos') * spark_funcs.col('risk_score')) /
         spark_funcs.sum('memmos')).alias('metric_value'),
    )

    outclaims = dfs_input['outclaims'].where(
        spark_funcs.col('prm_fromdate').between(
            min_incurred_date,
            max_incurred_date,
        )
    ).withColumn(
        'month',
        date_as_month(spark_funcs.col('prm_fromdate'))
    )

    outclaims_mem = outclaims.join(
        member_months,
        on=(outclaims.member_id == member_months.member_id)
        & (outclaims.month == member_months.elig_month),
        how='inner'
    )

    all_costs = outclaims_mem.select(
        'elig_status',
        spark_funcs.lit('prm_costs_sum_all_services').alias('metric_id'),
        'prm_costs',
    ).groupBy(
        'elig_status',
        'metric_id',
    ).agg(
        spark_funcs.sum('prm_costs').alias('metric_value')
    )

    mem_age = member_months.join(
        dfs_input['members'],
        on='member_id',
        how='inner'
    ).withColumn(
        'age_month',
        spark_funcs.datediff(
            spark_funcs.col('elig_month'),
            spark_funcs.col('dob')
        ) / 365.25
    )

    total_age = mem_age.select(
        'elig_status',
        spark_funcs.lit('total_age').alias('metric_id'),
        'memmos',
        'age_month',
    ).groupBy(
        'elig_status',
        'metric_id',
    ).agg(
        spark_funcs.sum(spark_funcs.col('memmos')*spark_funcs.col('age_month')).alias('metric_value'),
    )

    basic_metrics = cnt_assigned_mems.union(
        memmos_sum
    ).union(
        risk_score
    ).union(
        all_costs
    ).union(
        total_age
    ).union(
        attrib_lives
    ).coalesce(10)

    sparkapp.save_df(
        basic_metrics,
        PATH_OUTPUTS / 'basic_metrics.parquet',
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

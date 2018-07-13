"""
### CODE OWNERS: Umang Gupta, Pierre Cornell
### OBJECTIVE:
  Calculate SNF metrics used in comparator report.
### DEVELOPER NOTES:
  None
"""# pylint: disable=no-member
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

RUNOUT = 3

# =============================================================================
# LIBRARIES, LOCATIONS, LITERALS, ETC. GO ABOVE HERE
# =============================================================================

def main() -> int:
    """Calculate SNF metrics"""
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
        spark_funcs.col('months_of_claims_runout') == RUNOUT
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
            max_incurred_date,
        )
    ).withColumn(
        'month',
        date_as_month(spark_funcs.col('prm_fromdate'))
    ).where(
        spark_funcs.col('prm_line').substr(1, 3) == 'I31'
    )

    outclaims_mem = outclaims.join(
        member_months,
        on=(outclaims.member_id == member_months.member_id)
        & (outclaims.month == member_months.elig_month),
        how='inner'
    )

    hcc_risk_adj = read_sas_data(
        sparkapp,
        PATH_RISKADJ / 'mcrm_hcc_calibrations.sas7bdat',
    )

    admits = outclaims_mem.select(
        'elig_status',
        spark_funcs.col('prm_line').alias('mcrm_line'),
        spark_funcs.lit('SNF').alias('metric_id'),
        'prm_admits',
    ).groupBy(
        'elig_status',
        'mcrm_line',
        'metric_id',
    ).agg(
        spark_funcs.sum('prm_admits').alias('metric_value')
    )

    admits_riskadj = admits.join(
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
        'admits_riskadj',
        spark_funcs.col('metric_value') / spark_funcs.col('factor_util')
    )

    over_21 = outclaims_mem.where(
        spark_funcs.col('prm_util') > 21
    ).select(
        'elig_status',
        spark_funcs.lit('number_SNF_over_21_days').alias('metric_id'),
        'prm_admits',
    ).groupBy(
        'elig_status',
        'metric_id',
    ).agg(
        spark_funcs.sum('prm_admits').alias('metric_value')
    )

    readmits = outclaims_mem.where(
        spark_funcs.col('prm_readmit_all_cause_yn') == 'Y'
    ).select(
        'elig_status',
        spark_funcs.lit('number_SNF_readmits').alias('metric_id'),
        'prm_admits',
    ).groupBy(
        'elig_status',
        'metric_id',
    ).agg(
        spark_funcs.sum('prm_admits').alias('metric_value')
    )

    distinct_snfs = outclaims_mem.select(
        'elig_status',
        spark_funcs.lit('distinct_SNFs').alias('metric_id'),
        'providerid',
    ).groupBy(
        'elig_status',
        'metric_id',
    ).agg(
        spark_funcs.countDistinct('providerid'),
    )

    snf_metrics = admits.select(
        'elig_status',
        'metric_id',
        'metric_value',
    ).union(
        admits_riskadj.select(
            'elig_status',
            spark_funcs.lit('SNF_rskadj').alias('metric_id'),
            spark_funcs.col('admits_riskadj').alias('metric_value'),
        )
    ).union(
        over_21
    ).union(
        readmits
    ).union(
        distinct_snfs
    ).coalesce(10)

    sparkapp.save_df(
        snf_metrics,
        PATH_OUTPUTS / 'snf_metrics.parquet',
    )

    hcc_risk_adj.unpersist()

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

"""
### CODE OWNERS: Umang Gupta, Pierre Cornell
### OBJECTIVE:
  Calculate costs/util for each MR_Line used in comparator report
### DEVELOPER NOTES:
  None
"""
# pylint: disable=no-member
import logging
import os

from prm.spark.app import SparkApp
import pyspark.sql.functions as spark_funcs
from prm.dates.utils import date_as_month
from pathlib import Path

import comparator_report.meta.project

LOGGER = logging.getLogger(__name__)
META_SHARED = comparator_report.meta.project.gather_metadata()

NAME_MODULE = 'outputs'
PATH_INPUTS = META_SHARED['path_data_nyhealth_shared'] / NAME_MODULE
PATH_OUTPUTS = META_SHARED['path_data_comparator_report'] / NAME_MODULE
PATH_RISKADJ = Path(os.environ['reference_data_pathref'])

RUNOUT = 3

# =============================================================================
# LIBRARIES, LOCATIONS, LITERALS, ETC. GO ABOVE HERE
# =============================================================================

def mr_line_summary(
        outclaims: "DataFrame",
        prm_line: "String"
    ) -> "DataFrame":
    """Calculate a summary of costs and utilization by mr_line combination"""
    outclaims_mrline = outclaims.where(
        spark_funcs.col('prm_line').startswith(prm_line)
    )

    mr_line_cost = outclaims_mrline.select(
        'elig_status',
        spark_funcs.lit(prm_line + '_cost').alias('metric_id'),
        'prm_costs',
    ).groupBy(
        'elig_status',
        'metric_id',
    ).agg(
        spark_funcs.sum('prm_costs').alias('metric_value')
    )

    mr_line_admits = outclaims_mrline.select(
        'elig_status',
        spark_funcs.lit(prm_line + '_admits').alias('metric_id'),
        'prm_admits',
    ).groupBy(
        'elig_status',
        'metric_id',
    ).agg(
        spark_funcs.sum('prm_admits').alias('metric_value')
    )

    mr_line_util = outclaims_mrline.select(
        'elig_status',
        spark_funcs.lit(prm_line + '_util').alias('metric_id'),
        'prm_util',
    ).groupBy(
        'elig_status',
        'metric_id',
    ).agg(
        spark_funcs.sum('prm_util').alias('metric_value')
    )

    mr_metrics = mr_line_cost.union(
        mr_line_util
    ).union(
        mr_line_admits
    )

    return mr_metrics

def calc_all_mr_lines(
        outclaims: "DataFrame",
    ) -> "DataFrame":
    """Calculate admits, util, and costs for every individual mr_line"""
    mr_line_costs = outclaims.select(
        'elig_status',
        spark_funcs.concat(
            spark_funcs.col('prm_line'),
            spark_funcs.lit('_cost')
        ).alias('metric_id'),
        'prm_costs',
    ).groupBy(
        'elig_status',
        'metric_id',
    ).agg(
        spark_funcs.sum('prm_costs').alias('metric_value')
    )

    mr_line_admits = outclaims.select(
        'elig_status',
        spark_funcs.concat(
            spark_funcs.col('prm_line'),
            spark_funcs.lit('_admits')
        ).alias('metric_id'),
        'prm_admits',
    ).groupBy(
        'elig_status',
        'metric_id',
    ).agg(
        spark_funcs.sum('prm_admits').alias('metric_value')
    ) 

    mr_line_util = outclaims.select(
        'elig_status',
        spark_funcs.concat(
            spark_funcs.col('prm_line'),
            spark_funcs.lit('_util')
        ).alias('metric_id'),
        'prm_util',
    ).groupBy(
        'elig_status',
        'metric_id',
    ).agg(
        spark_funcs.sum('prm_util').alias('metric_value')
    ) 

    all_mr_line_metrics = mr_line_costs.union(
        mr_line_admits
    ).union(
        mr_line_util
    )

    return all_mr_line_metrics

def main() -> int:
    """Pass claims by mr_line to mr_line_summary to calculate metrics"""
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

    member_months = dfs_input['member_months'].where(
        spark_funcs.col('cover_medical') == 'Y'
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

    mr_lines_combo = ['I', 'I11', 'P32', 'O16', 'P34', 'P99', 'P2', 'P',]

    for mr_line in mr_lines_combo:
        if mr_lines_combo.index(mr_line) == 0:
            mr_line_metrics = mr_line_summary(outclaims_mem, mr_line)
        else:
            mr_line_metrics = mr_line_metrics.union(
                mr_line_summary(outclaims_mem, mr_line)
            )
    
    all_mr_line_metrics = calc_all_mr_lines(outclaims_mem)

    mr_line_metrics_stack = mr_line_metrics.union(
        all_mr_line_metrics
    ).coalesce(15)

    sparkapp.save_df(
        mr_line_metrics_stack,
        PATH_OUTPUTS / 'mr_line_metrics.parquet',
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

"""
### CODE OWNERS: Umang Gupta, Pierre Cornell
### OBJECTIVE:
  Calculate costs/util for each MR_Line used in comparator report
### DEVELOPER NOTES:
  None
"""
from databricks.sdk.runtime import *

import pyspark.sql.functions as F

INPUT = 'premier_data_feed'
OUTPUT = 'premier_data_feed'

RUNOUT = 3

# =============================================================================
# LIBRARIES, LOCATIONS, LITERALS, ETC. GO ABOVE HERE
# =============================================================================

def date_as_month(col):
    return (
        F.to_date(
            F.concat(
                F.date_format(col, "yyyy-MM"),
                F.lit("-15")
            )
        )
    )

def mr_line_summary(
        outclaims: "DataFrame",
        prm_line: "String"
    ) -> "DataFrame":
    """Calculate a summary of costs and utilization by mr_line combination"""
    outclaims_mrline = outclaims.where(
        F.col('prm_line').startswith(prm_line)
    )

    mr_line_cost = outclaims_mrline.select(
        'elig_status',
        F.lit(prm_line + '_cost').alias('metric_id'),
        'prm_costs',
    ).groupBy(
        'elig_status',
        'metric_id',
    ).agg(
        F.sum('prm_costs').alias('metric_value')
    )

    mr_line_admits = outclaims_mrline.select(
        'elig_status',
        F.lit(prm_line + '_admits').alias('metric_id'),
        'prm_admits',
    ).groupBy(
        'elig_status',
        'metric_id',
    ).agg(
        F.sum('prm_admits').alias('metric_value')
    )

    mr_line_util = outclaims_mrline.select(
        'elig_status',
        F.lit(prm_line + '_util').alias('metric_id'),
        'prm_util',
    ).groupBy(
        'elig_status',
        'metric_id',
    ).agg(
        F.sum('prm_util').alias('metric_value')
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
        F.concat(
            F.col('prm_line'),
            F.lit('_cost')
        ).alias('metric_id'),
        'prm_costs',
    ).groupBy(
        'elig_status',
        'metric_id',
    ).agg(
        F.sum('prm_costs').alias('metric_value')
    )

    mr_line_admits = outclaims.select(
        'elig_status',
        F.concat(
            F.col('prm_line'),
            F.lit('_admits')
        ).alias('metric_id'),
        'prm_admits',
    ).groupBy(
        'elig_status',
        'metric_id',
    ).agg(
        F.sum('prm_admits').alias('metric_value')
    ) 

    mr_line_util = outclaims.select(
        'elig_status',
        F.concat(
            F.col('prm_line'),
            F.lit('_util')
        ).alias('metric_id'),
        'prm_util',
    ).groupBy(
        'elig_status',
        'metric_id',
    ).agg(
        F.sum('prm_util').alias('metric_value')
    ) 

    all_mr_line_metrics = mr_line_costs.union(
        mr_line_admits
    ).union(
        mr_line_util
    )

    return all_mr_line_metrics

def MRLineMetrics() -> int:
    """Pass claims by mr_line to mr_line_summary to calculate metrics"""
    
    dfs_input = {
        path: spark.table(f"{INPUT}.{path}")
        for path in [
            'member_time_windows',
            'outclaims_full',
        ]
    }

    dfs_input["member_months"] = spark.table(f"{OUTPUT}.member_months")

    member_months = dfs_input['member_months'].where(
        F.col('cover_medical') == 'Y'
    )

    elig_memmos = dfs_input['member_time_windows'].where(
        F.col('cover_medical') == 'Y'
    ).select(
        'member_id',
        'date_start',
        'date_end',
    )

    outclaims = dfs_input['outclaims_full'].withColumn(
        'month',
        date_as_month(F.col('prm_fromdate'))
    )

    outclaims_mem = outclaims.join(
        elig_memmos,
        on=[
            outclaims.member_id == elig_memmos.member_id,
            F.col('prm_fromdate').between(
                    F.col('date_start'),
                    F.col('date_end'),
                
            )
            ],
        how='inner',
    ).join(
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

    mr_line_metrics_stack.write.mode("overwrite").saveAsTable(f'{OUTPUT}.mr_line_metrics')
    return 0

if __name__ == '__main__':
    # pylint: disable=wrong-import-position, wrong-import-order, ungrouped-imports
    MRLineMetrics()
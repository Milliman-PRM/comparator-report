"""
### CODE OWNERS: Umang Gupta, Pierre Cornell
### OBJECTIVE:
  Calculate end of life metrics used in comparator report
### DEVELOPER NOTES:
  None
"""
# pylint: disable=no-member
import logging
import os

from datetime import date
from prm.spark.app import SparkApp
import pyspark.sql.functions as spark_funcs
import comparator_report.meta.project

LOGGER = logging.getLogger(__name__)
META_SHARED = comparator_report.meta.project.gather_metadata()

NAME_MODULE = 'outputs'
PATH_INPUTS = META_SHARED['path_data_nyhealth_shared'] / NAME_MODULE
PATH_RS = META_SHARED['path_data_nyhealth_shared'] / 'risk_scores'
PATH_OUTPUTS = META_SHARED['path_data_comparator_report'] / NAME_MODULE

RUNOUT = 3

# =============================================================================
# LIBRARIES, LOCATIONS, LITERALS, ETC. GO ABOVE HERE
# =============================================================================

def calc_metrics(
        members: "DataFrame",
        metric_name: "String",
        metric_id: "String"
    ) -> "DataFrame":
    """Calculate EOL metrics by ID Name"""
    metric_val = members.where(
        spark_funcs.col('death_flag') == 1
    ).select(
        'elig_status',
        'prv_hier_2',
        spark_funcs.lit(metric_name).alias('metric_id'),
        spark_funcs.col(metric_id)
    ).groupBy(
        'elig_status',
        'prv_hier_2',
        'metric_id'
    ).agg(
        spark_funcs.sum(metric_id).alias('metric_value')
    )

    return metric_val

def main() -> int:
    """A function to pass arguments to calc_metrics"""
    sparkapp = SparkApp(META_SHARED['pipeline_signature'])

    dfs_input = {
        path.stem: sparkapp.load_df(path)
        for path in [
            PATH_OUTPUTS / 'member_months.parquet',
            PATH_INPUTS / 'members.parquet',
            PATH_INPUTS / 'outclaims.parquet',
            PATH_INPUTS / 'time_periods.parquet',
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

    member_months = dfs_input['member_months'].where(spark_funcs.col("memmos")!=0.0)

    mem_elig_distinct = member_months.select(
        'member_id',
        'elig_status',
        'prv_hier_2',
    ).distinct()

    mem_distinct = member_months.select(
        'member_id',
    ).distinct()

    mem_death = mem_distinct.join(
        dfs_input['members'],
        on='member_id',
        how='left_outer',
    ).select(
        mem_distinct.member_id,
        'death_date',
        spark_funcs.when(
            spark_funcs.col('death_date').between(
                min_incurred_date,
                max_incurred_date,
            ),
            spark_funcs.lit(1)
        ).otherwise(
            spark_funcs.lit(0)
        ).alias('death_flag'),
        'endoflife_numer_yn_chemolt14days',
        'endoflife_denom_yn_chemolt14days',
    ).withColumn(
        'cnt_cancer',
        spark_funcs.when(
            spark_funcs.col('death_flag') == 1,
            spark_funcs.when(
                spark_funcs.col('endoflife_denom_yn_chemolt14days') == 'Y',
                spark_funcs.lit(1)
            ).otherwise(
                spark_funcs.lit(0)
            )
        ).otherwise(
            spark_funcs.lit(0)
        )
    ).withColumn(
        'cnt_chemo',
        spark_funcs.when(
            spark_funcs.col('death_flag') == 1,
            spark_funcs.when(
                spark_funcs.col('endoflife_numer_yn_chemolt14days') == 'Y',
                spark_funcs.lit(1)
            ).otherwise(
                spark_funcs.lit(0)
            )
        ).otherwise(
            spark_funcs.lit(0)
        )
    )

    claims_mem = mem_death.join(
        dfs_input['outclaims'],
        on='member_id',
        how='inner',
    ).withColumn(
        'lt_30_days',
        spark_funcs.when(
            spark_funcs.datediff(spark_funcs.col('death_date'),
                spark_funcs.col('prm_todate')) <= 30,
            spark_funcs.lit('Y')
        ).otherwise(
            spark_funcs.lit('N')
        )
    )

    claims_cost = claims_mem.where(
        spark_funcs.col('lt_30_days') == 'Y'
    ).select(
        'member_id',
        'prm_costs',
    ).groupBy(
        'member_id',
    ).agg(
        spark_funcs.sum('prm_costs').alias('total_cost')
    )

    hosp_days = claims_mem.where(
        spark_funcs.col('prm_line').isin(["P82d", "P82e"])
    ).select(
        'member_id',
        'prm_util',
    ).groupBy(
        'member_id',
    ).agg(
        spark_funcs.sum('prm_util').alias('hospice_days')
    )

    death_in_hosp = dfs_input['outclaims'].where(
        (spark_funcs.col('dischargestatus') == '20')
        & (spark_funcs.col('prm_line').like('I%'))
        & (spark_funcs.col('prm_line') != 'I31')
    ).select(
        'member_id',
        spark_funcs.lit(1).alias('death_in_hosp'),
    ).distinct()    

    mem_decor = mem_death.join(
        claims_cost,
        on='member_id',
        how='left_outer',
    ).join(
        hosp_days,
        on='member_id',
        how='left_outer',
    ).join(
        death_in_hosp,
        on='member_id',
        how='left_outer',
    ).withColumn(
        'cnt_death_in_hosp',
        spark_funcs.when(
            spark_funcs.col('death_in_hosp').isNull(),
            spark_funcs.lit(0)
        ).otherwise(
            spark_funcs.lit(1)
        )
    ).fillna({
        'total_cost': 0.0,
        'hospice_days': 0,
    }).withColumn(
        'hosp_never',
        spark_funcs.when(
            spark_funcs.col('hospice_days') == 0,
            spark_funcs.lit(1)
        ).otherwise(
            spark_funcs.lit(0)
        )
    ).withColumn(
        'hosp_lt3',
        spark_funcs.when(
            spark_funcs.col('hospice_days').isin([1, 2]),
            spark_funcs.lit(1)
        ).otherwise(
            spark_funcs.lit(0)
        )
    ).select(
        'member_id',
        'death_flag',
        'cnt_cancer',
        'total_cost',
        'cnt_death_in_hosp',
        'hosp_never',
        'hosp_lt3',
        'cnt_chemo'
    )

    mem_elig_death = mem_decor.join(
        mem_elig_distinct,
        on='member_id',
        how='inner',
    ).select(
        'member_id',
        'elig_status',
        'prv_hier_2',
        'death_flag',
        'cnt_cancer',
        'total_cost',
        'cnt_death_in_hosp',
        'hosp_never',
        'hosp_lt3',
        'cnt_chemo',
    )

    non_esrd_eol = mem_elig_death.where(
        spark_funcs.col('elig_status') != 'ESRD'
    ).select(
        'member_id',
        spark_funcs.lit('Non-ESRD').alias('elig_status'),
        'prv_hier_2',
        'death_flag',
        'cnt_cancer',
        'total_cost',
        'cnt_death_in_hosp',
        'hosp_never',
        'hosp_lt3',
        'cnt_chemo',
    ).distinct()

    mem_decor_stack = mem_elig_death.union(
        non_esrd_eol
    )

    cnt_cancer = calc_metrics(mem_decor_stack, 'cnt_cancer', 'cnt_cancer')
    decedent_count = calc_metrics(mem_decor_stack, 'decedent_count', 'death_flag')
    tot_cost = calc_metrics(mem_decor_stack, 'tot_cost_final_30days', 'total_cost')
    death_hosp = calc_metrics(mem_decor_stack, 'cnt_death_in_hosp', 'cnt_death_in_hosp')
    hosp_never = calc_metrics(mem_decor_stack, 'cnt_hospice_never', 'hosp_never')
    hosp_lt3 = calc_metrics(mem_decor_stack, 'cnt_hospice_lt3days', 'hosp_lt3')
    cnt_chemo = calc_metrics(mem_decor_stack, 'cnt_chemo', 'cnt_chemo')

    decedent_count_all = mem_elig_death.where(
        spark_funcs.col('death_flag') == 1
    ).select(
        spark_funcs.lit('All').alias('elig_status'),
        'prv_hier_2',
        spark_funcs.lit('decedent_count').alias('metric_id'),
        'member_id'
    ).groupBy(
        'elig_status',
        'prv_hier_2',
        'metric_id'
    ).agg(
        spark_funcs.countDistinct('member_id').alias('metric_value')
    )

    eol_metrics = cnt_cancer.union(
        decedent_count
    ).union(
        tot_cost
    ).union(
        death_hosp
    ).union(
        hosp_never
    ).union(
        hosp_lt3
    ).union(
        cnt_chemo
    ).union(
        decedent_count_all
    ).coalesce(10)

    sparkapp.save_df(
        eol_metrics,
        PATH_OUTPUTS / 'eol_metrics.parquet',
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

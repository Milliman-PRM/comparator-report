"""
### CODE OWNERS: Umang Gupta, Pierre Cornell
### OBJECTIVE:
  Calculate ED metrics used in comparator report.
### DEVELOPER NOTES:
  None
"""
# pylint: disable=no-member
import logging

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
PATH_REF = Path(os.environ['reference_data_pathref'])

RUNOUT = 3

# =============================================================================
# LIBRARIES, LOCATIONS, LITERALS, ETC. GO ABOVE HERE
# =============================================================================

def nyu_measures(
        outclaims: "DataFrame",
        prm_col: "String",
        metric_id: "String",
    ) -> "DataFrame":
    """Calculate NYU Metrics by category and Elig Status"""
    metric = outclaims.select(
        'elig_status',
        spark_funcs.lit(metric_id).alias('metric_id'),
        'prm_util',
        spark_funcs.col(prm_col).alias('nyu_cat'),
    ).groupBy(
        'elig_status',
        'metric_id',
    ).agg(
        spark_funcs.sum(spark_funcs.col('prm_util') * spark_funcs.col('nyu_cat')).alias('metric_value')
    )

    return metric

def main() -> int:
    """Function to set up NYU measure calculation and calculate risk adjusted metrics"""
    sparkapp = SparkApp(META_SHARED['pipeline_signature'])

    dfs_input = {
        path.stem: sparkapp.load_df(path)
        for path in [
            PATH_OUTPUTS / 'member_months.parquet',
            PATH_INPUTS / 'time_periods.parquet',
            PATH_INPUTS / 'outclaims.parquet',
            PATH_INPUTS / 'decor_case.parquet',
            PATH_REF / 'ref_link_hcg_researcher_line.parquet',
            PATH_REF / 'ref_hcg_bt.parquet',                        
        ]
    }

    mr_line_map = dfs_input['ref_link_hcg_researcher_line'].where(
        spark_funcs.col('lob') == 'Medicare'
    )

    min_incurred_date, max_incurred_date = dfs_input['time_periods'].where(
        spark_funcs.col('months_of_claims_runout') == RUNOUT
    ).select(
        spark_funcs.col('reporting_date_start').alias('min_incurred_date'),
        spark_funcs.col('reporting_date_end').alias('max_incurred_date'),
    ).collect()[0]

    member_months = dfs_input['member_months'].where(
        spark_funcs.col('cover_medical') == 'Y'
    )

    decor_limited = dfs_input['decor_case'].select(
        'member_id',
        'caseadmitid',
        *[
            column
            for column in dfs_input['decor_case'].columns
            if column.startswith('prm_nyu_')
        ],
    )

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
    ).join(
        decor_limited,
        on=['member_id', 'caseadmitid'],
        how='left_outer'
    ).withColumn(
        'month',
        date_as_month(spark_funcs.col('prm_fromdate'))
    ).where(
        spark_funcs.col('prm_line').substr(1, 3) == 'O11'
    )

    outclaims_mem = outclaims.join(
        member_months,
        on=(outclaims.member_id == member_months.member_id)
        & (outclaims.month == member_months.elig_month),
        how='inner'
    ).join(
        mr_line_map,
        on=(outclaims.prm_line == mr_line_map.researcherline),
        how='left_outer',
    )

    hcc_risk_trim = dfs_input['ref_hcg_bt'].where(
        (spark_funcs.col('lob') == 'Medicare')
        & (spark_funcs.col('basis') == 'RS')
    ).withColumn(
        'hcc_bot_round',
        spark_funcs.when(
            spark_funcs.col('riskband') == '<0.50',
            spark_funcs.lit(-1.0)
        ).when(
            spark_funcs.col('riskband') == '0.50-0.59',
            spark_funcs.lit(.5)
        ).when(
            spark_funcs.col('riskband') == '0.60-0.69',
            spark_funcs.lit(.6)
        ).when(
            spark_funcs.col('riskband') == '0.70-0.79',
            spark_funcs.lit(.7)
        ).when(
            spark_funcs.col('riskband') == '0.80-0.89',
            spark_funcs.lit(.8)
        ).when(
            spark_funcs.col('riskband') == '0.90-0.99',
            spark_funcs.lit(.9)
        ).when(
            spark_funcs.col('riskband') == '1.00-1.09',
            spark_funcs.lit(1.0)
        ).when(
            spark_funcs.col('riskband') == '1.10-1.19',
            spark_funcs.lit(1.1)
        ).when(
            spark_funcs.col('riskband') == '1.20-1.34',
            spark_funcs.lit(1.2)
        ).when(
            spark_funcs.col('riskband') == '1.35-1.49',
            spark_funcs.lit(1.35)
        ).when(
            spark_funcs.col('riskband') == '1.50-1.74',
            spark_funcs.lit(1.5)
        ).when(
            spark_funcs.col('riskband') == '1.75-2.00',
            spark_funcs.lit(1.75)
        ).otherwise(
            spark_funcs.lit(2.0)
        )
    ).withColumn(
        'hcc_top_round',
        spark_funcs.when(
            spark_funcs.col('riskband') == '<0.50',
            spark_funcs.lit(.499)
        ).when(
            spark_funcs.col('riskband') == '0.50-0.59',
            spark_funcs.lit(.599)
        ).when(
            spark_funcs.col('riskband') == '0.60-0.69',
            spark_funcs.lit(.699)
        ).when(
            spark_funcs.col('riskband') == '0.70-0.79',
            spark_funcs.lit(.799)
        ).when(
            spark_funcs.col('riskband') == '0.80-0.89',
            spark_funcs.lit(.899)
        ).when(
            spark_funcs.col('riskband') == '0.90-0.99',
            spark_funcs.lit(.999)
        ).when(
            spark_funcs.col('riskband') == '1.00-1.09',
            spark_funcs.lit(1.099)
        ).when(
            spark_funcs.col('riskband') == '1.10-1.19',
            spark_funcs.lit(1.199)
        ).when(
            spark_funcs.col('riskband') == '1.20-1.34',
            spark_funcs.lit(1.349)
        ).when(
            spark_funcs.col('riskband') == '1.35-1.49',
            spark_funcs.lit(1.499)
        ).when(
            spark_funcs.col('riskband') == '1.50-1.74',
            spark_funcs.lit(1.749)
        ).when(
            spark_funcs.col('riskband') == '1.75-2.00',
            spark_funcs.lit(1.99)
        ).otherwise(
            spark_funcs.lit(500.0)
        )
    ).select(
        'btnumber',
        'hcc_bot_round',
        'hcc_top_round',
        'admitfactor',
        'utilfactor',
        'pmpmfactor',
    )

    util = outclaims_mem.select(
        'elig_status',
        'btnumber',
        spark_funcs.lit('ED').alias('metric_id'),
        'prm_util',
    ).groupBy(
        'elig_status',
        'btnumber',
        'metric_id',
    ).agg(
        spark_funcs.sum('prm_util').alias('metric_value')
    )

    util_riskadj = util.join(
        risk_score,
        on='elig_status',
        how='inner',
    ).join(
        hcc_risk_trim,
        on='btnumber',
        how='left_outer',
    ).where(
        spark_funcs.col('risk_score_avg').between(
            spark_funcs.col('hcc_bot_round'),
            spark_funcs.col('hcc_top_round'),
        )
    ).withColumn(
        'util_riskadj',
        spark_funcs.col('metric_value') / spark_funcs.col('utilfactor')
    )

    non_avoidable = nyu_measures(outclaims_mem, 'prm_nyu_emergent_non_avoidable', 'ED_emer_nec')
    avoidable = nyu_measures(outclaims_mem, 'prm_nyu_emergent_avoidable', 'ED_emer_prev')
    primary_care = nyu_measures(outclaims_mem, 'prm_nyu_emergent_primary_care', 'ED_emer_pricare')
    injury = nyu_measures(outclaims_mem, 'prm_nyu_injury', 'ED_injury')
    nonemergent = nyu_measures(outclaims_mem, 'prm_nyu_nonemergent', 'ED_nonemer')

    er_metrics = util.select(
        'elig_status',
        'metric_id',
        'metric_value',
    ).union(
        util_riskadj.select(
            'elig_status',
            spark_funcs.lit('ED_rskadj').alias('metric_id'),
            spark_funcs.col('util_riskadj').alias('metric_value'),
        )
    ).union(
        non_avoidable
    ).union(
        avoidable
    ).union(
        primary_care
    ).union(
        injury
    ).union(
        nonemergent
    ).coalesce(10)

    sparkapp.save_df(
        er_metrics,
        PATH_OUTPUTS / 'er_metrics.parquet',
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

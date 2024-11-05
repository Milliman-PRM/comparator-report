"""
### CODE OWNERS: Umang Gupta, Pierre Cornell
### OBJECTIVE:
  Calculate SNF metrics used in comparator report.
### DEVELOPER NOTES:
  None
"""# pylint: disable=no-member
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
PATH_REF = Path(os.environ['reference_data_pathref'])

RUNOUT = os.environ.get('runout', 3)

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
            PATH_INPUTS / 'member_time_windows.parquet',
            PATH_INPUTS / 'outclaims.parquet',
            PATH_REF / 'ref_link_hcg_researcher_line.parquet',
            PATH_REF / 'ref_hcg_bt.parquet',                        
        ]
    }

    mr_line_map = dfs_input['ref_link_hcg_researcher_line'].where(
        spark_funcs.col('lob') == 'Medicare'
    )

    member_months = dfs_input['member_months'].where(
        spark_funcs.col('cover_medical') == 'Y'
    )

    elig_memmos = dfs_input['member_time_windows'].where(
        spark_funcs.col('cover_medical') == 'Y'
    ).select(
        'member_id',
        'date_start',
        'date_end',
    )

    risk_score = member_months.select(
        'elig_status',
        'memmos',
        'risk_score',
    ).groupBy(
        'elig_status',
    ).agg(
        spark_funcs.format_number((spark_funcs.sum(spark_funcs.col('memmos')*spark_funcs.col('risk_score')) / spark_funcs.sum(spark_funcs.col('memmos'))), 3).alias('risk_score_avg')
    )

    outclaims = dfs_input['outclaims'].withColumn(
        'month',
        date_as_month(spark_funcs.col('prm_fromdate_case'))
    ).where(
        spark_funcs.col('prm_line').substr(1, 3) == 'I31'
    )

    outclaims_mem = outclaims.join(
        elig_memmos,
        on=[
            outclaims.member_id == elig_memmos.member_id,
            spark_funcs.col('prm_fromdate_case').between(
                spark_funcs.col('date_start'),
                spark_funcs.col('date_end'),
            )
            ],
        how='inner',
    ).join(
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
            spark_funcs.lit(1.999)
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

    admits = outclaims_mem.select(
        'elig_status',
        'btnumber',
        spark_funcs.lit('SNF').alias('metric_id'),
        'prm_admits',
    ).groupBy(
        'elig_status',
        'btnumber',
        'metric_id',
    ).agg(
        spark_funcs.sum('prm_admits').alias('metric_value')
    )

    admits_riskadj = admits.join(
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
        'admits_riskadj',
        spark_funcs.col('metric_value') / spark_funcs.col('admitfactor')
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

    ip_readmits = dfs_input['outclaims'].where(
        spark_funcs.col('prm_line_agg') == 'i1'
    ).groupBy(
        'member_id',
        'caseadmitid',
    ).agg(
        spark_funcs.min('prm_fromdate_case').alias('readmit_start')
    ).withColumn(
        'window_start',
        spark_funcs.date_add(
            spark_funcs.col('readmit_start'),
            -30,
        )
    ).withColumnRenamed(
        'caseadmitid',
        'caseadmitid_readmit'
    )
    
    snf_max_disch = outclaims_mem.groupBy(
        outclaims.member_id,
        'caseadmitid'
    ).agg(
        spark_funcs.max('prm_fromdate_case').alias('snf_start_date')
    )
    
    ip_max_disch = dfs_input['outclaims'].where(
        spark_funcs.col('prm_line_agg') == 'i1'
    ).groupBy(
        'member_id',
        'caseadmitid',
    ).agg(
        spark_funcs.max('prm_todate_case').alias('index_end')
    ).withColumnRenamed(
        'caseadmitid',
        'caseadmitid_index',
    )
    
    ip_readmits_w_snf = ip_readmits.join(
        snf_max_disch,
        on='member_id',
        how='inner'
    ).join(
        ip_max_disch,
        on='member_id',
        how='inner',
    ).where(
        spark_funcs.col('snf_start_date').between(
            spark_funcs.col('window_start'),
            spark_funcs.col('readmit_start'),
        )
    ).where(
        spark_funcs.col('index_end').between(
            spark_funcs.col('window_start'),
            spark_funcs.col('snf_start_date'),
        )
    ).select(
        'caseadmitid'
    ).distinct()
        
    snf_readmits = outclaims_mem.filter(
        outclaims_mem.snfrm_numer_yn == 'Y'
    ).select(
        'elig_status',
        spark_funcs.lit('SNF_Readmits').alias('metric_id'),
        'prm_admits',
    ).groupBy(
        'elig_status',
        'metric_id',
    ).agg(
        spark_funcs.sum('prm_admits').alias('metric_value')
    )
    
    snf_readmits_denom = outclaims_mem.filter(
        outclaims_mem.snfrm_denom_yn == 'Y'
    ).select(
        'elig_status',
        spark_funcs.lit('SNF_Readmits_denom').alias('metric_id'),
        'prm_admits',
    ).groupBy(
        'elig_status',
        'metric_id',
    ).agg(
        spark_funcs.sum('prm_admits').alias('metric_value')
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
    ).union(
        snf_readmits
    ).union(
        snf_readmits_denom
    ).coalesce(10)

    sparkapp.save_df(
        snf_metrics,
        PATH_OUTPUTS / 'snf_metrics.parquet',
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

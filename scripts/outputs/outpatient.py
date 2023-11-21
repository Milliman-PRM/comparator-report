"""
### CODE OWNERS: Umang Gupta, Pierre Cornell
### OBJECTIVE:
  Calculate outpatient metrics used in comparator report
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
PATH_REF = Path(os.environ['reference_data_pathref'])

if os.environ.get('STLMT_Enabled', 'False').lower() == 'true':
    RUNOUT = 3
else: 
    RUNOUT = 9

# =============================================================================
# LIBRARIES, LOCATIONS, LITERALS, ETC. GO ABOVE HERE
# =============================================================================

def metric_calc(
        outclaims: "DataFrame",
        risk_score: "DataFrame",
        hcc_risk_adj: "DataFrame",
        mr_line: "String",
        metric_id: "String",
    ) -> "DataFrame":
    """Calculate risk adjusted and non-risk adjusted metrics"""
    outclaims_mcrm = outclaims.where(
        spark_funcs.col('prm_line').startswith(mr_line)
    )

    util = outclaims_mcrm.select(
        'elig_status',
        'btnumber',
        'prm_util',
    ).groupBy(
        'elig_status',
        'btnumber',
    ).agg(
        spark_funcs.sum('prm_util').alias('util')
    )

    util_adj = util.join(
        risk_score,
        on='elig_status',
        how='inner',
    ).join(
        hcc_risk_adj,
        on='btnumber',
        how='left_outer',
    ).where(
        spark_funcs.col('risk_score_avg').between(
            spark_funcs.col('hcc_bot_round'),
            spark_funcs.col('hcc_top_round')
        )
    ).withColumn(
        'util_riskadj',
        spark_funcs.col('util') / spark_funcs.col('utilfactor')
    )

    metric = util_adj.select(
        'elig_status',
        spark_funcs.lit(metric_id).alias('metric_id'),
        'util'
    ).groupBy(
        'elig_status',
        'metric_id',
    ).agg(
        spark_funcs.sum('util').alias('metric_value')
    ).union(
        util_adj.select(
            'elig_status',
            spark_funcs.lit(metric_id + '_riskadj').alias('metric_id'),
            'util_riskadj',
        ).groupBy(
            'elig_status',
            'metric_id',
        ).agg(
            spark_funcs.sum('util_riskadj').alias('metric_value')
        )
    )

    return metric

def metric_calc_ov(
        outclaims: "DataFrame",
        risk_score: "DataFrame",
        hcc_risk_adj: "DataFrame",
        metric_id: "String",
    ) -> "DataFrame":
    """Office Visit metric calculations"""
    outclaims_mcrm = outclaims.where(
        spark_funcs.col('prm_line').isin(
            'P32c', 'P32d'
        )
    )

    util = outclaims_mcrm.select(
        'elig_status',
        'prm_line',
        'btnumber',
        'prm_util',
    ).groupBy(
        'elig_status',
        'prm_line',
        'btnumber',
    ).agg(
        spark_funcs.sum('prm_util').alias('util')
    )

    util_adj = util.join(
        risk_score,
        on='elig_status',
        how='inner',
    ).join(
        hcc_risk_adj,
        on='btnumber',
        how='left_outer',
    ).where(
        spark_funcs.col('risk_score_avg').between(
            spark_funcs.col('hcc_bot_round'),
            spark_funcs.col('hcc_top_round')
        )
    ).withColumn(
        'util_riskadj',
        spark_funcs.col('util') / spark_funcs.col('utilfactor')
    )

    metric = util_adj.select(
        'elig_status',
        spark_funcs.lit(metric_id).alias('metric_id'),
        'util'
    ).groupBy(
        'elig_status',
        'metric_id',
    ).agg(
        spark_funcs.sum('util').alias('metric_value')
    ).union(
        util_adj.select(
            'elig_status',
            spark_funcs.lit(metric_id + '_riskadj').alias('metric_id'),
            'util_riskadj',
        ).groupBy(
            'elig_status',
            'metric_id',
        ).agg(
            spark_funcs.sum('util_riskadj').alias('metric_value')
        )
    )

    return metric

def metric_calc_psp(
        outclaims: "DataFrame",
    ) -> "DataFrame":
    """Preference Sensitive Procedure metric calculation"""
    outclaims_psp = outclaims.where(
        (~spark_funcs.col('psp_category').isNull()) &
        (spark_funcs.col('prm_line').startswith('O12')) &
        (spark_funcs.col('psp_preventable_yn') == 'Y')
    )

    psp_gen = outclaims_psp.select(
        'elig_status',
        'mr_cases_admits',
    ).groupBy(
        'elig_status',
    ).agg(
        spark_funcs.sum('mr_cases_admits').alias('metric_value'),
    )

    psp_ind = outclaims_psp.select(
        'elig_status',
        spark_funcs.col('psp_category').alias('metric_id'),
        'mr_cases_admits',
    ).groupBy(
        'elig_status',
        'metric_id',
    ).agg(
        spark_funcs.sum('mr_cases_admits').alias('metric_value')
    ).select(
        'elig_status',
        spark_funcs.concat(
            spark_funcs.lit('psp_procs_'),
            spark_funcs.col('metric_id'),
        ).alias('metric_id'),
        'metric_value',
    )

    psps = psp_gen.select(
        'elig_status',
        spark_funcs.lit('psp_procs').alias('metric_id'),
        'metric_value',
    ).union(
        psp_ind
    )

    return psps

def main() -> int:
    """Pass outclaims through to outpatient metric calculation functions"""
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
    ).fillna({
        'risk_score_avg': 0
    })

    outclaims = dfs_input['outclaims'].withColumn(
        'month',
        date_as_month(spark_funcs.col('prm_fromdate'))
    ).where(
        spark_funcs.col('prm_line').substr(1, 3).isin(
            'O14', 'O10', 'P57', 'P59', 'P33', 'P32', 'O12',
        )
    )

    outclaims_mem = outclaims.join(
        elig_memmos,
        on=[
            outclaims.member_id == elig_memmos.member_id,
            spark_funcs.col('prm_fromdate').between(
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

    hi_tec_img = metric_calc(outclaims_mem, risk_score, hcc_risk_trim, 'O14', 'high_tech_imaging')
    obs_stays = metric_calc(outclaims_mem, risk_score, hcc_risk_trim, 'O10', 'observation_stays')
    hi_tec_img_fop = metric_calc(outclaims_mem, risk_score, hcc_risk_trim, 'P57', 'hi_tec_img_fop')
    hi_tec_img_office = metric_calc(outclaims_mem, risk_score, hcc_risk_trim, 'P59', 'hi_tec_img_office')
    urg_care = metric_calc(outclaims_mem, risk_score, hcc_risk_trim, 'P33', 'urgent_care_prof')
    office_vis = metric_calc_ov(outclaims_mem, risk_score, hcc_risk_trim, 'office_visits')
    pref_sens = metric_calc_psp(outclaims_mem)

    outpatient_metrics = hi_tec_img.union(
        obs_stays
    ).union(
        hi_tec_img_fop
    ).union(
        hi_tec_img_office
    ).union(
        urg_care
    ).union(
        office_vis
    ).union(
        pref_sens
    ).coalesce(10)

    sparkapp.save_df(
        outpatient_metrics,
        PATH_OUTPUTS / 'outpatient_metrics.parquet',
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

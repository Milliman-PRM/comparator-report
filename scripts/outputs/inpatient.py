"""
### CODE OWNERS: Umang Gupta, Pierre Cornell
### OBJECTIVE:
  Calculate inpatient metrics used in comparator report.
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
PATH_OUTPUTS = META_SHARED['path_data_comparator_report'] / NAME_MODULE
PATH_REF = META_SHARED[15, 'out']

RUNOUT = 3

# =============================================================================
# LIBRARIES, LOCATIONS, LITERALS, ETC. GO ABOVE HERE
# =============================================================================
def calc_pqi(
        outclaims: "DataFrame"
    ) -> "DataFrame":
    """Calculate metrics for PQIs"""
    outclaims_pqi = outclaims.where(
        (spark_funcs.col('prm_ahrq_pqi') != 'none') &
        (spark_funcs.col('prm_line') != 'I31')
    )

    pqi_gen = outclaims_pqi.select(
        'elig_status',
        'prm_admits',
    ).groupBy(
        'elig_status',
    ).agg(
        spark_funcs.sum('prm_admits').alias('metric_value'),
    )

    pqi_ind = outclaims_pqi.select(
        'elig_status',
        'prm_ahrq_pqi',
        'prm_admits',
    ).groupBy(
        'elig_status',
        'prm_ahrq_pqi',
    ).agg(
        spark_funcs.sum('prm_admits').alias('metric_value')
    )

    pqis = pqi_gen.select(
        'elig_status',
        spark_funcs.lit('pqi').alias('metric_id'),
        'metric_value',
    ).union(
        pqi_ind.withColumn(
            'metric_id',
            spark_funcs.concat(spark_funcs.col('prm_ahrq_pqi'), spark_funcs.lit('_admits'))
        ).select(
            'elig_status',
            'metric_id',
            'metric_value',
        )
    )

    return pqis

def calc_psp(
        outclaims: "DataFrame",
    ) -> "DataFrame":
    """Calculate Preference Sensitive Procedure Metrics"""
    outclaims_psp = outclaims.where(
        (~spark_funcs.col('psp_category').isNull()) &
        (spark_funcs.col('prm_line') != 'I31') &
        (spark_funcs.col('psp_preventable_yn') == 'Y')
    )

    psp_gen = outclaims_psp.select(
        'elig_status',
        'prm_admits',
    ).groupBy(
        'elig_status',
    ).agg(
        spark_funcs.sum('prm_admits').alias('metric_value'),
    )

    psp_ind = outclaims_psp.select(
        'elig_status',
        spark_funcs.col('psp_category').alias('metric_id'),
        'prm_admits',
    ).groupBy(
        'elig_status',
        'metric_id',
    ).agg(
        spark_funcs.sum('prm_admits').alias('metric_value')
    ).select(
        'elig_status',
        spark_funcs.concat(
            spark_funcs.lit('psp_admits_'),
            spark_funcs.col('metric_id'),
        ).alias('metric_id'),
        'metric_value',
    )

    psps = psp_gen.select(
        'elig_status',
        spark_funcs.lit('psp_admits').alias('metric_id'),
        'metric_value',
    ).union(
        psp_ind
    )

    return psps

def calc_one_day(
        outclaims: "DataFrame"
    ) -> "DataFrame":
    """Calculate 1 Day Inpatient Admission Metrics"""
    outclaims_od = outclaims.where(
        spark_funcs.col('prm_line') != 'I31'
    )

    one_day_denom = outclaims_od.select(
        'elig_status',
        spark_funcs.when(
            spark_funcs.col('prm_line').like('I11%'),
            spark_funcs.lit('medical'),
        ).otherwise(
            spark_funcs.lit('other'),
        ).alias('ip_type'),
        'prm_admits'
    ).groupBy(
        'elig_status',
        'ip_type',
    ).agg(
        spark_funcs.sum('prm_admits').alias('metric_value')
    )

    one_day_numer = outclaims_od.where(
        spark_funcs.col('prm_util') == 1
    ).select(
        'elig_status',
        spark_funcs.when(
            spark_funcs.col('prm_line').like('I11%'),
            spark_funcs.lit('medical'),
        ).otherwise(
            spark_funcs.lit('other'),
        ).alias('ip_type'),
        'prm_admits'
    ).groupBy(
        'elig_status',
        'ip_type',
    ).agg(
        spark_funcs.sum('prm_admits').alias('metric_value')
    )

    total = one_day_denom.select(
        'elig_status',
        spark_funcs.lit('Denom_1_Day_LOS').alias('metric_id'),
        'metric_value',
    ).groupBy(
        'elig_status',
        'metric_id',
    ).agg(
        spark_funcs.sum('metric_value').alias('metric_value')
    ).union(
        one_day_numer.select(
            'elig_status',
            spark_funcs.lit('Num_1_Day_LOS').alias('metric_id'),
            'metric_value',
        ).groupBy(
            'elig_status',
            'metric_id',
        ).agg(
            spark_funcs.sum('metric_value').alias('metric_value')
        )
    )

    one_day = total.union(
        one_day_numer.filter(
            'ip_type = "medical"'
        ).select(
            'elig_status',
            spark_funcs.lit('Num_1_Day_LOS_Medical').alias('metric_id'),
            'metric_value',
        ).union(
            one_day_denom.filter(
                'ip_type = "medical"'
            ).select(
                'elig_status',
                spark_funcs.lit('Denom_1_Day_LOS_Medical').alias('metric_id'),
                'metric_value',
            )
        )
    )

    return one_day

def calc_risk_adj(
        outclaims: "DataFrame",
        member_months: "DataFrame",
        hcc_risk_adj: "DataFrame"
    ) -> "DataFrame":
    """Calculated Risk Adjusted versions of Inpatient Metrics"""
    outclaims_ra = outclaims.where(
        spark_funcs.col('prm_line') != 'I31'
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

    outclaims_admits = outclaims_ra.select(
        'elig_status',
        'prm_line',
        'btnumber',
        'prm_admits',
    ).groupBy(
        'elig_status',
        'prm_line',
        'btnumber',
    ).agg(
        spark_funcs.sum('prm_admits').alias('admits')
    ).join(
        risk_score,
        on='elig_status',
        how='left_outer',
    )

    outclaims_util = outclaims_admits.join(
        hcc_risk_adj,
        on='btnumber',
        how='left_outer',
    ).select(
        'elig_status',
        'btnumber',
        'prm_line',
        'admits',
        'risk_score_avg',
        spark_funcs.when(
            spark_funcs.col('admitfactor').isNull(),
            spark_funcs.lit(1)
        ).otherwise(
            spark_funcs.col('admitfactor')
        ).alias('admitfactor'),
        spark_funcs.when(
            spark_funcs.col('risk_score_avg').between(
                spark_funcs.col('hcc_bot_round'),
                spark_funcs.col('hcc_top_round'),
                ),
            spark_funcs.lit('Y'),
        ).otherwise(
            spark_funcs.when(
                spark_funcs.col('admitfactor').isNull(),
                spark_funcs.lit('Y')
            ).otherwise(
                spark_funcs.lit('N')
            )
        ).alias('keep_flag'),
    ).where(
        spark_funcs.col('keep_flag') == 'Y'
    ).withColumn(
        'admits_riskadj',
        spark_funcs.col('admits') / spark_funcs.col('admitfactor'),
    )

    acute = outclaims_util.select(
        'elig_status',
        spark_funcs.lit('acute').alias('metric_id'),
        'admits',
    ).groupBy(
        'elig_status',
        'metric_id',
    ).agg(
        spark_funcs.sum('admits').alias('metric_value')
    ).union(
        outclaims_util.select(
            'elig_status',
            spark_funcs.lit('acute_riskadj').alias('metric_id'),
            'admits_riskadj',
        ).groupBy(
            'elig_status',
            'metric_id',
        ).agg(
            spark_funcs.sum('admits_riskadj').alias('metric_value')
        )
    )

    med_gen = outclaims_util.where(
        spark_funcs.col('prm_line') == 'I11a'
    ).select(
        'elig_status',
        spark_funcs.lit('medical_general').alias('metric_id'),
        'admits',
    ).groupBy(
        'elig_status',
        'metric_id',
    ).agg(
        spark_funcs.sum('admits').alias('metric_value')
    ).union(
        outclaims_util.where(
            spark_funcs.col('prm_line') == 'I11a'
        ).select(
            'elig_status',
            spark_funcs.lit('medical_general_riskadj').alias('metric_id'),
            'admits_riskadj',
        ).groupBy(
            'elig_status',
            'metric_id',
        ).agg(
            spark_funcs.sum('admits_riskadj').alias('metric_value')
        )
    )

    surgery = outclaims_util.where(
        spark_funcs.col('prm_line') == 'I12'
    ).select(
        'elig_status',
        spark_funcs.lit('surgical').alias('metric_id'),
        'admits',
    ).groupBy(
        'elig_status',
        'metric_id',
    ).agg(
        spark_funcs.sum('admits').alias('metric_value')
    ).union(
        outclaims_util.where(
            spark_funcs.col('prm_line') == 'I12'
        ).select(
            'elig_status',
            spark_funcs.lit('surgical_riskadj').alias('metric_id'),
            'admits_riskadj',
        ).groupBy(
            'elig_status',
            'metric_id',
        ).agg(
            spark_funcs.sum('admits_riskadj').alias('metric_value')
        )
    )

    type_union = acute.union(
        med_gen
    ).union(
        surgery
    )

    return type_union

def calc_readmits(
        outclaims: "DataFrame"
    ) -> "DataFrame":
    """Calculate potential and actual readmits"""
    outclaims_readmits = outclaims.where(
        spark_funcs.col('prm_line') != 'I31'
    )

    potential = outclaims_readmits.where(
        'prm_readmit_potential_yn == "Y"'
    ).select(
        'elig_status',
        spark_funcs.lit('cnt_pot_readmits').alias('metric_id'),
        'prm_admits',
    ).groupBy(
        'elig_status',
        'metric_id',
    ).agg(
        spark_funcs.sum('prm_admits').alias('metric_value')
    )

    readmits = outclaims_readmits.where(
        'prm_readmit_all_cause_yn == "Y"'
    ).select(
        'elig_status',
        spark_funcs.lit('cnt_ip_readmits').alias('metric_id'),
        'prm_admits',
    ).groupBy(
        'elig_status',
        'metric_id',
    ).agg(
        spark_funcs.sum('prm_admits').alias('metric_value')
    )

    metrics = potential.union(
        readmits
    )

    return metrics

def main() -> int:
    """Function to pass inpatient claims to other functions that calculate metrics"""
    sparkapp = SparkApp(META_SHARED['pipeline_signature'])

    dfs_input = {
        path.stem: sparkapp.load_df(path)
        for path in [
            PATH_OUTPUTS / 'member_months.parquet',
            PATH_INPUTS / 'time_periods.parquet',
            PATH_INPUTS / 'outclaims.parquet',
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

    outclaims = dfs_input['outclaims'].where(
        spark_funcs.col('prm_fromdate').between(
            min_incurred_date,
            max_incurred_date,
        )
    ).withColumn(
        'month',
        date_as_month(spark_funcs.col('prm_fromdate'))
    ).where(
        spark_funcs.col('prm_line').like('I%')
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

    snf_disch = outclaims_mem.where(
        (spark_funcs.col('prm_line') != 'I31') &
        (spark_funcs.col('dischargestatus') == '03')
    ).select(
        'elig_status',
        spark_funcs.lit('disch_snf').alias('metric_id'),
        'prm_admits'
    ).groupBy(
        'elig_status',
        'metric_id',
    ).agg(
        spark_funcs.sum('prm_admits').alias('metric_value')
    )

    pqi_summary = calc_pqi(outclaims_mem)

    psp_summary = calc_psp(outclaims_mem)

    one_day_summary = calc_one_day(outclaims_mem)

    risk_adj_summary = calc_risk_adj(outclaims_mem, member_months, hcc_risk_trim)

    readmit = calc_readmits(outclaims_mem)

    inpatient_metrics = pqi_summary.union(
        psp_summary
    ).union(
        one_day_summary
    ).union(
        risk_adj_summary
    ).union(
        snf_disch
    ).union(
        readmit
    ).coalesce(10)

    sparkapp.save_df(
        inpatient_metrics,
        PATH_OUTPUTS / 'inpatient_metrics.parquet',
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

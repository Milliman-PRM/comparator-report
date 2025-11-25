"""
### CODE OWNERS: Umang Gupta, Pierre Cornell
### OBJECTIVE:
  Calculate inpatient metrics used in comparator report.
### DEVELOPER NOTES:
  None
"""
# pylint: disable=no-member
from databricks.sdk.runtime import *

import pyspark.sql.functions as F

INPUT = 'premier_data_feed'
OUTPUT = 'premier_data_feed'
PATH_REF = 'comparator_references'

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

def calc_pqi(
        outclaims: "DataFrame"
    ) -> "DataFrame":
    """Calculate metrics for PQIs"""
    outclaims_pqi = outclaims.where(
        (F.col('prm_ahrq_pqi') != 'none') &
        (F.col('prm_line') != 'I31')
    )

    pqi_gen = outclaims_pqi.select(
        'elig_status',
        'prm_admits',
    ).groupBy(
        'elig_status',
    ).agg(
        F.sum('prm_admits').alias('metric_value'),
    )

    pqi_ind = outclaims_pqi.select(
        'elig_status',
        'prm_ahrq_pqi',
        'prm_admits',
    ).groupBy(
        'elig_status',
        'prm_ahrq_pqi',
    ).agg(
        F.sum('prm_admits').alias('metric_value')
    )

    pqis = pqi_gen.select(
        'elig_status',
        F.lit('pqi').alias('metric_id'),
        'metric_value',
    ).union(
        pqi_ind.withColumn(
            'metric_id',
            F.concat(F.col('prm_ahrq_pqi'), F.lit('_admits'))
        ).select(
            'elig_status',
            'metric_id',
            'metric_value',
        )
    )

    return pqis

def calc_one_day(
        outclaims: "DataFrame"
    ) -> "DataFrame":
    """Calculate 1 Day Inpatient Admission Metrics"""
    outclaims_od = outclaims.where(
        F.col('prm_line') != 'I31'
    )

    one_day_denom = outclaims_od.select(
        'elig_status',
        F.when(
            F.col('prm_line').like('I11%'),
            F.lit('medical'),
        ).otherwise(
            F.lit('other'),
        ).alias('ip_type'),
        'prm_admits'
    ).groupBy(
        'elig_status',
        'ip_type',
    ).agg(
        F.sum('prm_admits').alias('metric_value')
    )

    one_day_numer = outclaims_od.where(
        F.col('prm_util') == 1
    ).select(
        'elig_status',
        F.when(
            F.col('prm_line').like('I11%'),
            F.lit('medical'),
        ).otherwise(
            F.lit('other'),
        ).alias('ip_type'),
        'prm_admits'
    ).groupBy(
        'elig_status',
        'ip_type',
    ).agg(
        F.sum('prm_admits').alias('metric_value')
    )

    total = one_day_denom.select(
        'elig_status',
        F.lit('Denom_1_Day_LOS').alias('metric_id'),
        'metric_value',
    ).groupBy(
        'elig_status',
        'metric_id',
    ).agg(
        F.sum('metric_value').alias('metric_value')
    ).union(
        one_day_numer.select(
            'elig_status',
            F.lit('Num_1_Day_LOS').alias('metric_id'),
            'metric_value',
        ).groupBy(
            'elig_status',
            'metric_id',
        ).agg(
            F.sum('metric_value').alias('metric_value')
        )
    )

    one_day = total.union(
        one_day_numer.filter(
            'ip_type = "medical"'
        ).select(
            'elig_status',
            F.lit('Num_1_Day_LOS_Medical').alias('metric_id'),
            'metric_value',
        ).union(
            one_day_denom.filter(
                'ip_type = "medical"'
            ).select(
                'elig_status',
                F.lit('Denom_1_Day_LOS_Medical').alias('metric_id'),
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
        F.col('prm_line') != 'I31'
    )

    risk_score = member_months.select(
        'elig_status',
        'memmos',
        'risk_score',
    ).groupBy(
        'elig_status',
    ).agg(
        F.format_number((F.sum(F.col('memmos')*F.col('risk_score')) / F.sum(F.col('memmos'))), 3).alias('risk_score_avg')
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
        F.sum('prm_admits').alias('admits')
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
        F.when(
            F.col('admitfactor').isNull(),
            F.lit(1)
        ).otherwise(
            F.col('admitfactor')
        ).alias('admitfactor'),
        F.when(
            F.col('risk_score_avg').between(
                F.col('hcc_bot_round'),
                F.col('hcc_top_round'),
                ),
            F.lit('Y'),
        ).otherwise(
            F.when(
                F.col('admitfactor').isNull(),
                F.lit('Y')
            ).otherwise(
                F.lit('N')
            )
        ).alias('keep_flag'),
    ).where(
        F.col('keep_flag') == 'Y'
    ).withColumn(
        'admits_riskadj',
        F.col('admits') / F.col('admitfactor'),
    )

    acute = outclaims_util.select(
        'elig_status',
        F.lit('acute').alias('metric_id'),
        'admits',
    ).groupBy(
        'elig_status',
        'metric_id',
    ).agg(
        F.sum('admits').alias('metric_value')
    ).union(
        outclaims_util.select(
            'elig_status',
            F.lit('acute_riskadj').alias('metric_id'),
            'admits_riskadj',
        ).groupBy(
            'elig_status',
            'metric_id',
        ).agg(
            F.sum('admits_riskadj').alias('metric_value')
        )
    )

    med_gen = outclaims_util.where(
        F.col('prm_line') == 'I11a'
    ).select(
        'elig_status',
        F.lit('medical_general').alias('metric_id'),
        'admits',
    ).groupBy(
        'elig_status',
        'metric_id',
    ).agg(
        F.sum('admits').alias('metric_value')
    ).union(
        outclaims_util.where(
            F.col('prm_line') == 'I11a'
        ).select(
            'elig_status',
            F.lit('medical_general_riskadj').alias('metric_id'),
            'admits_riskadj',
        ).groupBy(
            'elig_status',
            'metric_id',
        ).agg(
            F.sum('admits_riskadj').alias('metric_value')
        )
    )

    surgery = outclaims_util.where(
        F.col('prm_line') == 'I12'
    ).select(
        'elig_status',
        F.lit('surgical').alias('metric_id'),
        'admits',
    ).groupBy(
        'elig_status',
        'metric_id',
    ).agg(
        F.sum('admits').alias('metric_value')
    ).union(
        outclaims_util.where(
            F.col('prm_line') == 'I12'
        ).select(
            'elig_status',
            F.lit('surgical_riskadj').alias('metric_id'),
            'admits_riskadj',
        ).groupBy(
            'elig_status',
            'metric_id',
        ).agg(
            F.sum('admits_riskadj').alias('metric_value')
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
        F.col('prm_line') != 'I31'
    )

    potential = outclaims_readmits.select(
        'elig_status',
        F.lit('cnt_pot_readmits').alias('metric_id'),
        'prm_admits',
    ).groupBy(
        'elig_status',
        'metric_id',
    ).agg(
        F.sum('prm_admits').alias('metric_value')
    )

    readmits = outclaims_readmits.where(
        'prm_readmit_all_cause_yn == "Y"'
    ).select(
        'elig_status',
        F.lit('cnt_ip_readmits').alias('metric_id'),
        'prm_admits',
    ).groupBy(
        'elig_status',
        'metric_id',
    ).agg(
        F.sum('prm_admits').alias('metric_value')
    )

    metrics = potential.union(
        readmits
    )

    return metrics

def Inpatient() -> int:
    """Function to pass inpatient claims to other functions that calculate metrics"""

    dfs_input = {
        path: spark.table(f"{INPUT}.{path}")
        for path in [
            'member_time_windows',
            'outclaims_full',

        ]
    }

    dfs_input["member_months"] = spark.table(f"{OUTPUT}.member_months")
    dfs_input["ref_link_hcg_researcher_line"] = spark.table(f"{INPUT}.ref_link_hcg_researcher_line")
    dfs_input["ref_hcg_bt"] = spark.table(f"{INPUT}.ref_hcg_bt")

    mr_line_map = dfs_input['ref_link_hcg_researcher_line'].where(
        F.col('lob') == 'Medicare'
    )

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

    rolled_up_days = dfs_input['outclaims_full'].where(
        F.col('prm_line').like('I%')
    ).groupBy('caseadmitid').agg(F.sum('prm_util').alias('prm_util_agg'))
    
    outclaims = dfs_input['outclaims_full'].withColumn(
        'month',
        date_as_month(F.col('prm_fromdate'))
    ).where(
        F.col('prm_line').like('I%')
    ).drop(
        'prm_util'
    ).join(rolled_up_days,
        on=[(dfs_input['outclaims_full'].caseadmitid == rolled_up_days.caseadmitid) & (dfs_input['outclaims_full'].prm_admits != 0)],
        how='left_outer'
    ).withColumn(
        'prm_util',
        F.col('prm_util_agg')
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
    ).join(
        mr_line_map,
        on=(outclaims.prm_line == mr_line_map.researcherline),
        how='left_outer',
    )

    hcc_risk_trim = dfs_input['ref_hcg_bt'].where(
        (F.col('lob') == 'Medicare')
        & (F.col('basis') == 'RS')
    ).withColumn(
        'hcc_bot_round',
        F.when(
            F.col('riskband') == '<0.50',
            F.lit(-1.0)
        ).when(
            F.col('riskband') == '0.50-0.59',
            F.lit(.5)
        ).when(
            F.col('riskband') == '0.60-0.69',
            F.lit(.6)
        ).when(
            F.col('riskband') == '0.70-0.79',
            F.lit(.7)
        ).when(
            F.col('riskband') == '0.80-0.89',
            F.lit(.8)
        ).when(
            F.col('riskband') == '0.90-0.99',
            F.lit(.9)
        ).when(
            F.col('riskband') == '1.00-1.09',
            F.lit(1.0)
        ).when(
            F.col('riskband') == '1.10-1.19',
            F.lit(1.1)
        ).when(
            F.col('riskband') == '1.20-1.34',
            F.lit(1.2)
        ).when(
            F.col('riskband') == '1.35-1.49',
            F.lit(1.35)
        ).when(
            F.col('riskband') == '1.50-1.74',
            F.lit(1.5)
        ).when(
            F.col('riskband') == '1.75-2.00',
            F.lit(1.75)
        ).otherwise(
            F.lit(2.0)
        )
    ).withColumn(
        'hcc_top_round',
        F.when(
            F.col('riskband') == '<0.50',
            F.lit(.499)
        ).when(
            F.col('riskband') == '0.50-0.59',
            F.lit(.599)
        ).when(
            F.col('riskband') == '0.60-0.69',
            F.lit(.699)
        ).when(
            F.col('riskband') == '0.70-0.79',
            F.lit(.799)
        ).when(
            F.col('riskband') == '0.80-0.89',
            F.lit(.899)
        ).when(
            F.col('riskband') == '0.90-0.99',
            F.lit(.999)
        ).when(
            F.col('riskband') == '1.00-1.09',
            F.lit(1.099)
        ).when(
            F.col('riskband') == '1.10-1.19',
            F.lit(1.199)
        ).when(
            F.col('riskband') == '1.20-1.34',
            F.lit(1.349)
        ).when(
            F.col('riskband') == '1.35-1.49',
            F.lit(1.499)
        ).when(
            F.col('riskband') == '1.50-1.74',
            F.lit(1.749)
        ).when(
            F.col('riskband') == '1.75-2.00',
            F.lit(1.999)
        ).otherwise(
            F.lit(500.0)
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
        (F.col('prm_line') != 'I31') &
        (F.col('dischargestatus') == '03')
    ).select(
        'elig_status',
        F.lit('disch_snf').alias('metric_id'),
        'prm_admits'
    ).groupBy(
        'elig_status',
        'metric_id',
    ).agg(
        F.sum('prm_admits').alias('metric_value')
    )

    pqi_summary = calc_pqi(outclaims_mem)

    one_day_summary = calc_one_day(outclaims_mem)

    risk_adj_summary = calc_risk_adj(outclaims_mem, member_months, hcc_risk_trim)

    readmit = calc_readmits(outclaims_mem)

    inpatient_metrics = pqi_summary.union(
        one_day_summary
    ).union(
        risk_adj_summary
    ).union(
        snf_disch
    ).union(
        readmit
    ).coalesce(10)

    inpatient_metrics.write.mode("overwrite").saveAsTable(f'{OUTPUT}.inpatient_metrics')

    return 0

if __name__ == '__main__':
    # pylint: disable=wrong-import-position, wrong-import-order, ungrouped-imports
    Inpatient()
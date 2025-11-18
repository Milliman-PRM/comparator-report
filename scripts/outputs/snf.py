"""
### CODE OWNERS: Umang Gupta, Pierre Cornell
### OBJECTIVE:
  Calculate SNF metrics used in comparator report.
### DEVELOPER NOTES:
  None
"""# pylint: disable=no-member
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
def SNF() -> int:
    """Calculate SNF metrics"""
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

    risk_score = member_months.select(
        'elig_status',
        'memmos',
        'risk_score',
    ).groupBy(
        'elig_status',
    ).agg(
        F.format_number((F.sum(F.col('memmos')*F.col('risk_score')) / F.sum(F.col('memmos'))), 3).alias('risk_score_avg')
    )

    outclaims = dfs_input['outclaims_full'].withColumn(
        'month',
        date_as_month(F.col('prm_fromdate'))
    ).where(
        F.col('prm_line').substr(1, 3) == 'I31'
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

    admits = outclaims_mem.select(
        'elig_status',
        'btnumber',
        F.lit('SNF').alias('metric_id'),
        'prm_admits',
    ).groupBy(
        'elig_status',
        'btnumber',
        'metric_id',
    ).agg(
        F.sum('prm_admits').alias('metric_value')
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
        F.col('risk_score_avg').between(
            F.col('hcc_bot_round'),
            F.col('hcc_top_round'),
        )
    ).withColumn(
        'admits_riskadj',
        F.col('metric_value') / F.col('admitfactor')
    )

    over_21 = outclaims_mem.where(
        F.col('prm_util') > 21
    ).select(
        'elig_status',
        F.lit('number_SNF_over_21_days').alias('metric_id'),
        'prm_admits',
    ).groupBy(
        'elig_status',
        'metric_id',
    ).agg(
        F.sum('prm_admits').alias('metric_value')
    )

    distinct_snfs = outclaims_mem.select(
        'elig_status',
        F.lit('distinct_SNFs').alias('metric_id'),
        'providerid',
    ).groupBy(
        'elig_status',
        'metric_id',
    ).agg(
        F.countDistinct('providerid'),
    )

    ip_readmits = dfs_input['outclaims_full'].where(
        F.col('prm_line') == 'i1'
    ).groupBy(
        'member_id',
        'caseadmitid',
    ).agg(
        F.min('prm_fromdate').alias('readmit_start')
    ).withColumn(
        'window_start',
        F.date_add(
            F.col('readmit_start'),
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
        F.max('prm_fromdate').alias('snf_start_date')
    )
    
    ip_max_disch = dfs_input['outclaims_full'].where(
        F.col('prm_line') == 'i1'
    ).groupBy(
        'member_id',
        'caseadmitid',
    ).agg(
        F.max('prm_todate').alias('index_end')
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
        F.col('snf_start_date').between(
            F.col('window_start'),
            F.col('readmit_start'),
        )
    ).where(
        F.col('index_end').between(
            F.col('window_start'),
            F.col('snf_start_date'),
        )
    ).select(
        'caseadmitid'
    ).distinct()
        
    snf_readmits = outclaims_mem.filter(
        outclaims_mem.snfrm_numer_yn == 'Y'
    ).select(
        'elig_status',
        F.lit('SNF_Readmits').alias('metric_id'),
        'prm_admits',
    ).groupBy(
        'elig_status',
        'metric_id',
    ).agg(
        F.sum('prm_admits').alias('metric_value')
    )
    
    snf_readmits_denom = outclaims_mem.filter(
        outclaims_mem.snfrm_denom_yn == 'Y'
    ).select(
        'elig_status',
        F.lit('SNF_Readmits_denom').alias('metric_id'),
        'prm_admits',
    ).groupBy(
        'elig_status',
        'metric_id',
    ).agg(
        F.sum('prm_admits').alias('metric_value')
    )
            
    snf_metrics = admits.select(
        'elig_status',
        'metric_id',
        'metric_value',
    ).union(
        admits_riskadj.select(
            'elig_status',
            F.lit('SNF_rskadj').alias('metric_id'),
            F.col('admits_riskadj').alias('metric_value'),
        )
    ).union(
        over_21
    ).union(
        distinct_snfs
    ).union(
        snf_readmits
    ).union(
        snf_readmits_denom
    ).coalesce(10)

    snf_metrics.write.mode("overwrite").saveAsTable(f'{OUTPUT}.snf_metrics')


    return 0

if __name__ == '__main__':
    SNF()
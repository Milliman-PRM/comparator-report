"""
### CODE OWNERS: Umang Gupta, Pierre Cornell
### OBJECTIVE:
  Calculate ED metrics used in comparator report.
### DEVELOPER NOTES:
  None
"""
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
def nyu_measures(
        outclaims: "DataFrame",
        prm_col: "String",
        metric_id: "String",
    ) -> "DataFrame":
    """Calculate NYU Metrics by category and Elig Status"""
    metric = outclaims.select(
        'elig_status',
        F.lit(metric_id).alias('metric_id'),
        'prm_util',
        F.col(prm_col).alias('nyu_cat'),
    ).groupBy(
        'elig_status',
        'metric_id',
    ).agg(
        F.sum(F.col('prm_util') * F.col('nyu_cat')).alias('metric_value')
    )

    return metric

def ER() -> int:
    """Function to set up NYU measure calculation and calculate risk adjusted metrics"""

    dfs_input = {
        path: spark.table(f"{INPUT}.{path}")
        for path in [
            'member_months',
            'member_time_windows',
            'outclaims_full',                    
        ]
    }
    dfs_input["ref_link_hcg_researcher_line"] = spark.table(f"{INPUT}.ref_link_hcg_researcher_line")
    dfs_input["ref_hcg_bt"] = spark.table(f"{INPUT}.ref_hcg_bt")
    
    mr_line_map = dfs_input['ref_link_hcg_researcher_line'].where(
        F.col('lob') == 'Medicare'
    )

    member_months = dfs_input['member_months'].where(
        F.col('cover_medical') == 'Y'
    )


    risk_score = member_months.select(
        'elig_status',
        'memmos',
        'risk_score',
    ).groupBy(
        'elig_status',
    ).agg(
        F.format_number((F.sum(F.col('memmos')*F.col('risk_score')) / F.sum(F.col('memmos'))), 2).alias('risk_score_avg')
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
    ).where(
        F.col('prm_line').substr(1, 3) == 'O11'
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

    util = outclaims_mem.select(
        'elig_status',
        'btnumber',
        F.lit('ED').alias('metric_id'),
        'prm_util',
    ).groupBy(
        'elig_status',
        'btnumber',
        'metric_id',
    ).agg(
        F.sum('prm_util').alias('metric_value')
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
        F.col('risk_score_avg').between(
            F.col('hcc_bot_round'),
            F.col('hcc_top_round'),
        )
    ).withColumn(
        'util_riskadj',
        F.col('metric_value') / F.col('utilfactor')
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
            F.lit('ED_rskadj').alias('metric_id'),
            F.col('util_riskadj').alias('metric_value'),
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

    er_metrics.write.mode("overwrite").saveAsTable(f'{OUTPUT}.er_metrics')

    return 0

if __name__ == '__main__':
    ER()
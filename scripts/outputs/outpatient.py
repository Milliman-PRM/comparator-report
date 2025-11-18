"""
### CODE OWNERS: Umang Gupta, Pierre Cornell
### OBJECTIVE:
  Calculate outpatient metrics used in comparator report
### DEVELOPER NOTES:
  None
"""
# pylint: disable=no-member

import pyspark.sql.functions as F

from databricks.sdk.runtime import *

PATH_REF = 'comparator_references'
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

def metric_calc(
        outclaims: "DataFrame",
        risk_score: "DataFrame",
        hcc_risk_adj: "DataFrame",
        mr_line: "String",
        metric_id: "String",
    ) -> "DataFrame":
    """Calculate risk adjusted and non-risk adjusted metrics"""
    outclaims_mcrm = outclaims.where(
        F.col('prm_line').startswith(mr_line)
    )

    util = outclaims_mcrm.select(
        'elig_status',
        'btnumber',
        'prm_util',
    ).groupBy(
        'elig_status',
        'btnumber',
    ).agg(
        F.sum('prm_util').alias('util')
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
        F.col('risk_score_avg').between(
            F.col('hcc_bot_round'),
            F.col('hcc_top_round')
        )
    ).withColumn(
        'util_riskadj',
        F.col('util') / F.col('utilfactor')
    )

    metric = util_adj.select(
        'elig_status',
        F.lit(metric_id).alias('metric_id'),
        'util'
    ).groupBy(
        'elig_status',
        'metric_id',
    ).agg(
        F.sum('util').alias('metric_value')
    ).union(
        util_adj.select(
            'elig_status',
            F.lit(metric_id + '_riskadj').alias('metric_id'),
            'util_riskadj',
        ).groupBy(
            'elig_status',
            'metric_id',
        ).agg(
            F.sum('util_riskadj').alias('metric_value')
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
        F.col('prm_line').isin(
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
        F.sum('prm_util').alias('util')
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
        F.col('risk_score_avg').between(
            F.col('hcc_bot_round'),
            F.col('hcc_top_round')
        )
    ).withColumn(
        'util_riskadj',
        F.col('util') / F.col('utilfactor')
    )

    metric = util_adj.select(
        'elig_status',
        F.lit(metric_id).alias('metric_id'),
        'util'
    ).groupBy(
        'elig_status',
        'metric_id',
    ).agg(
        F.sum('util').alias('metric_value')
    ).union(
        util_adj.select(
            'elig_status',
            F.lit(metric_id + '_riskadj').alias('metric_id'),
            'util_riskadj',
        ).groupBy(
            'elig_status',
            'metric_id',
        ).agg(
            F.sum('util_riskadj').alias('metric_value')
        )
    )

    return metric


def Outpatient() -> int:
    """Pass outclaims through to outpatient metric calculation functions"""
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
    ).fillna({
        'risk_score_avg': 0
    })

    outclaims = dfs_input['outclaims_full'].withColumn(
        'month',
        date_as_month(F.col('prm_fromdate'))
    ).where(
        F.col('prm_line').substr(1, 3).isin(
            'O14', 'O10', 'P57', 'P59', 'P33', 'P32', 'O12',
        )
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

    hi_tec_img = metric_calc(outclaims_mem, risk_score, hcc_risk_trim, 'O14', 'high_tech_imaging')
    obs_stays = metric_calc(outclaims_mem, risk_score, hcc_risk_trim, 'O10', 'observation_stays')
    hi_tec_img_fop = metric_calc(outclaims_mem, risk_score, hcc_risk_trim, 'P57', 'hi_tec_img_fop')
    hi_tec_img_office = metric_calc(outclaims_mem, risk_score, hcc_risk_trim, 'P59', 'hi_tec_img_office')
    urg_care = metric_calc(outclaims_mem, risk_score, hcc_risk_trim, 'P33', 'urgent_care_prof')
    office_vis = metric_calc_ov(outclaims_mem, risk_score, hcc_risk_trim, 'office_visits')

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
    ).coalesce(10)

    outpatient_metrics.write.mode("overwrite").saveAsTable(f'{OUTPUT}.outpatient_metrics')

    return 0

if __name__ == '__main__':
    # pylint: disable=wrong-import-position, wrong-import-order, ungrouped-imports
    Outpatient()
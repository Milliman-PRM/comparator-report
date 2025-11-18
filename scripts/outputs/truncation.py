"""
### CODE OWNERS: Umang Gupta, Pierre Cornell
### OBJECTIVE:
  Calculate costs after truncation by eligibility status
### DEVELOPER NOTES:
  None
"""
from databricks.sdk.runtime import *
import pyspark.sql.functions as F
from dateutil.relativedelta import relativedelta

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

def truncation_summary(
        outclaims: "DataFrame",
        member_months: "DataFrame",
        to_from_dt: str,
        dt_dict: dict
    ) -> "DataFrame":
    """Summarize IME, DSC, UCC and calculate truncation for high cost claimants"""
    outclaims_dt_trim = outclaims.where(
        F.col(to_from_dt).between(
            dt_dict['min_incurred_date'],
            dt_dict['max_incurred_date'],
        )    
    ).withColumn(
        'elig_month',
        date_as_month(F.col(to_from_dt)),
    )
        
    outclaims_mem = outclaims_dt_trim.join(
        member_months,
        on=['member_id', 'elig_month'],
        how='inner'
    ).withColumn(
        'ime_sum',
        F.when(
            F.col('prm_costs') < 0,
            -1 * (F.col('prm_capital_ime_amount') + F.col('prm_operational_ime_amount'))
        ).otherwise(
            F.col('prm_capital_ime_amount') + F.col('prm_operational_ime_amount')
        )
    ).withColumn(
        'dsh_sum',
        F.when(
            F.col('prm_costs') < 0,
            -1 * (F.col('prm_capital_ds_amount') + F.col('prm_operational_ds_amount'))
        ).otherwise(
            F.col('prm_capital_ds_amount') + F.col('prm_operational_ds_amount')
        )
    ).withColumn(
        'ucc_sum',
        F.when(
            F.col('prm_costs') < 0,
            -1 * F.col('prm_hipps_dsh_amount')
        ).otherwise(
            F.col('prm_hipps_dsh_amount')
        )
    )        
    
    distinct_ime_dsh_ucc = outclaims_mem.select(
        'member_id',
        'elig_status',
        'claimid',
        'ime_sum',
        'dsh_sum',
        'ucc_sum',
        *[
            F.when(
                F.col('paiddate') <= dt_dict['qexpu_runout_14'],
                F.col(ime_dsh_ucc),
            ).otherwise(
                F.lit(0)
            ).alias('{col}_14'.format(col=ime_dsh_ucc))
            for ime_dsh_ucc in ['ime_sum', 'dsh_sum', 'ucc_sum']
        ],
        *[
            F.when(
                F.col('paiddate') <= dt_dict['qexpu_runout_21'],
                F.col(ime_dsh_ucc),
            ).otherwise(
                F.lit(0)
            ).alias('{col}_21'.format(col=ime_dsh_ucc))
            for ime_dsh_ucc in ['ime_sum', 'dsh_sum', 'ucc_sum']
        ],
        *[
            F.when(
                F.col('paiddate') <= dt_dict['qexpu_runout_3mos'],
                F.col(ime_dsh_ucc),
            ).otherwise(
                F.lit(0)
            ).alias('{col}_3mos'.format(col=ime_dsh_ucc))
            for ime_dsh_ucc in ['ime_sum', 'dsh_sum', 'ucc_sum']
        ],
        *[
            F.when(
                F.col('paiddate') <= dt_dict['qexpu_runout_3mos_7'],
                F.col(ime_dsh_ucc),
            ).otherwise(
                F.lit(0)
            ).alias('{col}_3mos_7'.format(col=ime_dsh_ucc))
            for ime_dsh_ucc in ['ime_sum', 'dsh_sum', 'ucc_sum']
        ],
    ).distinct()

    ime_dsh_ucc_summary = distinct_ime_dsh_ucc.groupBy(
        'member_id',
        'elig_status',
    ).agg(
        *[
            F.sum(ime_dsh_ucc).alias(ime_dsh_ucc.replace('sum', 'costs'))
            for ime_dsh_ucc in distinct_ime_dsh_ucc.columns if 'sum' in ime_dsh_ucc
        ]
    )

    cost_summary = outclaims_mem.groupBy(
        'member_id',
        'elig_status',
    ).agg(
        F.sum('prm_costs').alias('costs'),
        F.sum(
            F.when(
                F.col('paiddate') <= dt_dict['qexpu_runout_14'],
                F.col('prm_costs')
            ).otherwise(
                F.lit(0)
            )
        ).alias('costs_14'),
        F.sum(
            F.when(
                F.col('paiddate') <= dt_dict['qexpu_runout_21'],
                F.col('prm_costs')
            ).otherwise(
                F.lit(0)
            )
        ).alias('costs_21'),
        F.sum(
            F.when(
                F.col('paiddate') <= dt_dict['qexpu_runout_3mos'],
                F.col('prm_costs')
            ).otherwise(
                F.lit(0)
            )
        ).alias('costs_3mos'),   
        F.sum(
            F.when(
                F.col('paiddate') <= dt_dict['qexpu_runout_3mos_7'],
                F.col('prm_costs')
            ).otherwise(
                F.lit(0)
            )
        ).alias('costs_3mos_7'),             
    )
    
    memmos_summary = member_months.groupBy(
        'member_id',
        'elig_status'
    ).agg(
        F.sum('memmos').alias('memmos')
    )

    mem_all_costs = memmos_summary.join(
        cost_summary,
        on=['member_id', 'elig_status'],
        how='left_outer',
    ).join(
        ime_dsh_ucc_summary,
        on=['member_id', 'elig_status'],
        how='left_outer',
    ).fillna({
        col: 0
        for col in cost_summary.columns + ime_dsh_ucc_summary.columns if 'costs' in col
    }).withColumn(
        'truncation_threshold',
        F.when(
            F.col('elig_status') == 'Aged Non-Dual',
            147189 * F.col('memmos') / 12,
        ).when(
            F.col('elig_status') == 'Aged Dual',
            236991 * F.col('memmos') / 12,
        ).when(
            F.col('elig_status') == 'Disabled',
            177548 * F.col('memmos') / 12,
        ).when(
            F.col('elig_status') == 'ESRD',
            507077 * F.col('memmos') / 12,
        ).otherwise(
            99999999999
        )
    )

    for suffix in ['costs', 'costs_14', 'costs_21','costs_3mos','costs_3mos_7']:
        mem_all_costs = mem_all_costs.withColumn(
            '{col}_reduced'.format(col=suffix),
            F.col(suffix)
            - F.col('ime_{col}'.format(col=suffix))
            - F.col('dsh_{col}'.format(col=suffix))
            - F.col('ucc_{col}'.format(col=suffix))
        ).withColumn(
            '{col}_truncated'.format(col=suffix),
            F.least(
                F.col('truncation_threshold'),
                F.col('{col}_reduced'.format(col=suffix))
            )
        )

    truncation_summary = mem_all_costs.groupBy(
        'elig_status',
        F.lit(to_from_dt).alias('date_flag'),
    ).agg(
        *[
            F.sum(col).alias(col)
            for col in mem_all_costs.columns if col not in ['member_id', 'elig_status', 'truncation_threshold']
        ]
    )

    return truncation_summary
    
def Truncation() -> int:
    """Calculate costs after truncation by eligibility status"""

    dfs_input = {
        path: spark.table(f"{OUTPUT}.{path}")
        for path in [
            'time_periods',
            'outclaims_full',
            'member_months'
        ]
    }

    min_incurred_date, max_incurred_date = dfs_input['time_periods'].where(
        F.col('months_of_claims_runout') == RUNOUT
    ).select(
        F.add_months(F.to_date(F.concat('time_period_id',F.lit('01')),'yyyyMMdd'),-11).alias('min_incurred_date'),
        F.last_day(F.to_date(F.concat('time_period_id',F.lit('01')),'yyyyMMdd')).alias('max_incurred_date')
    ).collect()[0]
    
    qexpu_runout_14 = max_incurred_date + relativedelta(days=14)
    qexpu_runout_21 = max_incurred_date + relativedelta(days=21)
    qexpu_runout_3mos = max_incurred_date + relativedelta(months=3)
    qexpu_runout_3mos_7 = max_incurred_date + relativedelta(months=3, days=7)
    
    member_months = dfs_input['member_months'].where(
        F.col('cover_medical') == 'Y'
    )

    dt_dict = {
        'min_incurred_date' : min_incurred_date,
        'max_incurred_date' : max_incurred_date,
        'qexpu_runout_14' : qexpu_runout_14,
        'qexpu_runout_21' : qexpu_runout_21,
        'qexpu_runout_3mos' : qexpu_runout_3mos,
        'qexpu_runout_3mos_7' : qexpu_runout_3mos_7,
    }
    
    trunc_fromdt = truncation_summary(
        dfs_input['outclaims_full'],
        member_months,
        'prm_fromdate',
        dt_dict
    )

    trunc_todt = truncation_summary(
        dfs_input['outclaims_full'],
        member_months,
        'prm_todate',
        dt_dict
    )    
    
    trunc_summary = trunc_fromdt.union(
        trunc_todt
    ).coalesce(15)

    trunc_summary.write.mode("overwrite").saveAsTable(f'{OUTPUT}.truncation')


    return 0

if __name__ == '__main__':
    Truncation()
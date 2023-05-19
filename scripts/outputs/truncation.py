"""
### CODE OWNERS: Umang Gupta, Pierre Cornell
### OBJECTIVE:
  Calculate costs after truncation by eligibility status
### DEVELOPER NOTES:
  None
"""
# pylint: disable=no-member
import logging

from prm.spark.app import SparkApp
import pyspark.sql.functions as spark_funcs
from prm.dates.utils import date_as_month
from datetime import timedelta

import comparator_report.meta.project

LOGGER = logging.getLogger(__name__)
META_SHARED = comparator_report.meta.project.gather_metadata()

NAME_MODULE = 'outputs'
PATH_INPUTS = META_SHARED['path_data_nyhealth_shared'] / NAME_MODULE
PATH_OUTPUTS = META_SHARED['path_data_comparator_report'] / NAME_MODULE

RUNOUT = 3

# =============================================================================
# LIBRARIES, LOCATIONS, LITERALS, ETC. GO ABOVE HERE
# =============================================================================

def truncation_summary(
        outclaims: "DataFrame",
        member_months: "DataFrame",
        to_from_dt: str,
        dt_dict: dict
    ) -> "DataFrame":
    """Summarize IME, DSC, UCC and calculate truncation for high cost claimants"""
    outclaims_dt_trim = outclaims.where(
        spark_funcs.col(to_from_dt).between(
            dt_dict['min_incurred_date'],
            dt_dict['max_incurred_date'],
        )    
    ).withColumn(
        'elig_month',
        date_as_month(spark_funcs.col(to_from_dt)),
    )
        
    outclaims_mem = outclaims_dt_trim.join(
        member_months,
        on=['member_id', 'elig_month'],
        how='inner'
    ).withColumn(
        'ime_sum',
        spark_funcs.when(
            spark_funcs.col('prm_costs') < 0,
            -1 * (spark_funcs.col('prm_capital_ime_amount') + spark_funcs.col('prm_operational_ime_amount'))
        ).otherwise(
            spark_funcs.col('prm_capital_ime_amount') + spark_funcs.col('prm_operational_ime_amount')
        )
    ).withColumn(
        'dsh_sum',
        spark_funcs.when(
            spark_funcs.col('prm_costs') < 0,
            -1 * (spark_funcs.col('prm_capital_ds_amount') + spark_funcs.col('prm_operational_ds_amount'))
        ).otherwise(
            spark_funcs.col('prm_capital_ds_amount') + spark_funcs.col('prm_operational_ds_amount')
        )
    ).withColumn(
        'ucc_sum',
        spark_funcs.when(
            spark_funcs.col('prm_costs') < 0,
            -1 * spark_funcs.col('prm_hipps_dsh_amount')
        ).otherwise(
            spark_funcs.col('prm_hipps_dsh_amount')
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
            spark_funcs.when(
                spark_funcs.col('paiddate') <= dt_dict['qexpu_runout_14'],
                spark_funcs.col(ime_dsh_ucc),
            ).otherwise(
                spark_funcs.lit(0)
            ).alias('{col}_14'.format(col=ime_dsh_ucc))
            for ime_dsh_ucc in ['ime_sum', 'dsh_sum', 'ucc_sum']
        ],
        *[
            spark_funcs.when(
                spark_funcs.col('paiddate') <= dt_dict['qexpu_runout_21'],
                spark_funcs.col(ime_dsh_ucc),
            ).otherwise(
                spark_funcs.lit(0)
            ).alias('{col}_21'.format(col=ime_dsh_ucc))
            for ime_dsh_ucc in ['ime_sum', 'dsh_sum', 'ucc_sum']
        ]        
    ).distinct()

    ime_dsh_ucc_summary = distinct_ime_dsh_ucc.groupBy(
        'member_id',
        'elig_status',
    ).agg(
        *[
            spark_funcs.sum(ime_dsh_ucc).alias(ime_dsh_ucc.replace('sum', 'costs'))
            for ime_dsh_ucc in distinct_ime_dsh_ucc.columns if 'sum' in ime_dsh_ucc
        ]
    )

    cost_summary = outclaims_mem.groupBy(
        'member_id',
        'elig_status',
    ).agg(
        spark_funcs.sum('prm_costs').alias('costs'),
        spark_funcs.sum(
            spark_funcs.when(
                spark_funcs.col('paiddate') <= dt_dict['qexpu_runout_14'],
                spark_funcs.col('prm_costs')
            ).otherwise(
                spark_funcs.lit(0)
            )
        ).alias('costs_14'),
        spark_funcs.sum(
            spark_funcs.when(
                spark_funcs.col('paiddate') <= dt_dict['qexpu_runout_21'],
                spark_funcs.col('prm_costs')
            ).otherwise(
                spark_funcs.lit(0)
            )
        ).alias('costs_21'),
    )
    
    memmos_summary = member_months.groupBy(
        'member_id',
        'elig_status'
    ).agg(
        spark_funcs.sum('memmos').alias('memmos')
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
        spark_funcs.when(
            spark_funcs.col('elig_status') == 'Aged Non-Dual',
            138974 * spark_funcs.col('memmos') / 12,
        ).when(
            spark_funcs.col('elig_status') == 'Aged Dual',
            225780 * spark_funcs.col('memmos') / 12,
        ).when(
            spark_funcs.col('elig_status') == 'Disabled',
            167346 * spark_funcs.col('memmos') / 12,
        ).when(
            spark_funcs.col('elig_status') == 'ESRD',
            483440 * spark_funcs.col('memmos') / 12,
        ).otherwise(
            99999999999
        )
    )

    for suffix in ['costs', 'costs_14', 'costs_21']:
        mem_all_costs = mem_all_costs.withColumn(
            '{col}_reduced'.format(col=suffix),
            spark_funcs.col(suffix)
            - spark_funcs.col('ime_{col}'.format(col=suffix))
            - spark_funcs.col('dsh_{col}'.format(col=suffix))
            - spark_funcs.col('ucc_{col}'.format(col=suffix))
        ).withColumn(
            '{col}_truncated'.format(col=suffix),
            spark_funcs.least(
                spark_funcs.col('truncation_threshold'),
                spark_funcs.col('{col}_reduced'.format(col=suffix))
            )
        )

    truncation_summary = mem_all_costs.groupBy(
        'elig_status',
        spark_funcs.lit(to_from_dt).alias('date_flag'),
    ).agg(
        *[
            spark_funcs.sum(col).alias(col)
            for col in mem_all_costs.columns if col not in ['member_id', 'elig_status', 'truncation_threshold']
        ]
    )

    return truncation_summary
    
def main() -> int:
    """Calculate costs after truncation by eligibility status"""
    sparkapp = SparkApp(META_SHARED['pipeline_signature'])

    dfs_input = {
        path.stem: sparkapp.load_df(path)
        for path in [
            PATH_OUTPUTS / 'member_months.parquet',
            PATH_INPUTS / 'time_periods.parquet',
            PATH_INPUTS / 'outclaims.parquet',
        ]
    }

    min_incurred_date, max_incurred_date = dfs_input['time_periods'].where(
        spark_funcs.col('months_of_claims_runout') == RUNOUT
    ).select(
        spark_funcs.col('reporting_date_start').alias('min_incurred_date'),
        spark_funcs.col('reporting_date_end').alias('max_incurred_date'),
    ).collect()[0]
    
    qexpu_runout_14 = max_incurred_date + timedelta(days=14)
    qexpu_runout_21 = max_incurred_date + timedelta(days=21)

    member_months = dfs_input['member_months'].where(
        spark_funcs.col('cover_medical') == 'Y'
    )

    dt_dict = {
        'min_incurred_date' : min_incurred_date,
        'max_incurred_date' : max_incurred_date,
        'qexpu_runout_14' : qexpu_runout_14,
        'qexpu_runout_21' : qexpu_runout_21,
    }

    trunc_fromdt = truncation_summary(
        dfs_input['outclaims'],
        member_months,
        'prm_fromdate',
        dt_dict
    )

    trunc_todt = truncation_summary(
        dfs_input['outclaims'],
        member_months,
        'prm_todate',
        dt_dict
    )    
    
    trunc_summary = trunc_fromdt.union(
        trunc_todt
    ).coalesce(15)

    sparkapp.save_df(
        trunc_summary,
        PATH_OUTPUTS / 'truncation.parquet',
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

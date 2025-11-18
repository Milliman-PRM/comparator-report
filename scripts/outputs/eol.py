"""
### CODE OWNERS: Umang Gupta, Pierre Cornell
### OBJECTIVE:
  Calculate end of life metrics used in comparator report
### DEVELOPER NOTES:
  None
"""
# pylint: disable=no-member
from databricks.sdk.runtime import *
from datetime import date
import pyspark.sql.functions as F

INPUT = 'premier_data_feed'
OUTPUT = 'premier_data_feed'

RUNOUT = 3

# =============================================================================
# LIBRARIES, LOCATIONS, LITERALS, ETC. GO ABOVE HERE
# =============================================================================

def calc_metrics(
        members: "DataFrame",
        metric_name: "String",
        metric_id: "String"
    ) -> "DataFrame":
    """Calculate EOL metrics by ID Name"""
    metric_val = members.where(
        F.col('death_flag') == 1
    ).select(
        'elig_status',
        F.lit(metric_name).alias('metric_id'),
        F.col(metric_id)
    ).groupBy(
        'elig_status',
        'metric_id'
    ).agg(
        F.sum(metric_id).alias('metric_value')
    )

    return metric_val

def EOL(YTD_Only) -> int:
    """A function to pass arguments to calc_metrics"""

    dfs_input = {
        path: spark.table(f"{OUTPUT}.{path}")
        for path in [
            'member_months',
            'members',
            'time_periods',
            'eol'
        ]
    }

    min_incurred_date, max_incurred_date = dfs_input['time_periods'].where(
        F.col('months_of_claims_runout') == RUNOUT
    ).select(
        F.add_months(F.to_date(F.concat('time_period_id',F.lit('01')),'yyyyMMdd'),-11).alias('min_incurred_date'),
        F.last_day(F.to_date(F.concat('time_period_id',F.lit('01')),'yyyyMMdd')).alias('max_incurred_date')
    ).collect()[0]

    if YTD_Only:
        min_incurred_date = date(
            max_incurred_date.year,
            1,
            1
        )

    member_months = dfs_input['member_months'].where(F.col("memmos")!=0.0)

    mem_elig_distinct = member_months.select(
        'member_id',
        'elig_status',
    ).distinct()

    mem_distinct = member_months.select(
        'member_id',
    ).distinct()

    mem_death = mem_distinct.join(
        dfs_input['members'],
        on='member_id',
        how='left_outer',
    ).select(
        mem_distinct.member_id,
        'death_date',
        F.when(
            F.col('death_date').between(
                min_incurred_date,
                max_incurred_date,
            ),
            F.lit(1)
        ).otherwise(
            F.lit(0)
        ).alias('death_flag'),
    )

    mem_decor = mem_death.join(
        dfs_input['eol'],
        on='member_id',
        how='inner'
    ).select(
        'member_id',
        F.col('endoflife_paid_last_30_days').alias('total_cost'),
        F.col('endoflife_died_in_hospital').alias('cnt_death_in_hosp'),
        F.col('endoflife_numer_yn_hospice12months').alias('hosp_12months'),
        F.col('endoflife_numer_yn_hospicelt3day').alias('hosp_lt3'),
        'death_flag'
    )

    mem_elig_death = mem_decor.join(
        mem_elig_distinct,
        on='member_id',
        how='inner',
    ).select(
        'member_id',
        'elig_status',
        'death_flag',
        'total_cost',
        'cnt_death_in_hosp',
        'hosp_12months',
        'hosp_lt3',
    )

    non_esrd_eol = mem_elig_death.where(
        F.col('elig_status') != 'ESRD'
    ).select(
        'member_id',
        F.lit('Non-ESRD').alias('elig_status'),
        'death_flag',
        'total_cost',
        'cnt_death_in_hosp',
        'hosp_12months',
        'hosp_lt3',
    ).distinct()

    mem_decor_stack = mem_elig_death.union(
        non_esrd_eol
    )

    decedent_count = calc_metrics(mem_decor_stack, 'decedent_count', 'death_flag')
    tot_cost = calc_metrics(mem_decor_stack, 'tot_cost_final_30days', 'total_cost')
    death_hosp = calc_metrics(mem_decor_stack, 'cnt_death_in_hosp', 'cnt_death_in_hosp')
    hosp_12months = calc_metrics(mem_decor_stack, 'cnt_hospice_12months', 'hosp_12months')
    hosp_lt3 = calc_metrics(mem_decor_stack, 'cnt_hospice_lt3days', 'hosp_lt3')

    decedent_count_all = mem_elig_death.where(
        F.col('death_flag') == 1
    ).select(
        F.lit('All').alias('elig_status'),
        F.lit('decedent_count').alias('metric_id'),
        'member_id'
    ).groupBy(
        'elig_status',
        'metric_id'
    ).agg(
        F.countDistinct('member_id').alias('metric_value')
    )

    eol_metrics = decedent_count.union(
        tot_cost
    ).union(
        death_hosp
    ).union(
        hosp_12months
    ).union(
        hosp_lt3
    ).union(
        decedent_count_all
    ).coalesce(10)

    eol_metrics.write.mode("overwrite").saveAsTable(f'{OUTPUT}.eol_metrics')

    return 0

if __name__ == '__main__':
    EOL(False)
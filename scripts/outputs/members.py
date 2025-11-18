"""
### CODE OWNERS: Umang Gupta, Pierre Cornell
### OBJECTIVE:
  Create Membership basis for all other queries
### DEVELOPER NOTES:
  None
"""


# pylint: disable=no-member
#import logging
from databricks.sdk.runtime import *
import os

from datetime import date
import pyspark.sql.functions as F
from pyspark.sql import Window

INPUT = 'premier_data_feed'
OUTPUT = 'premier_data_feed'
RUNOUT = 3

# =============================================================================
# LIBRARIES, LOCATIONS, LITERALS, ETC. GO ABOVE HERE
# =============================================================================

def Members(YTD_Only,Currently_Assigned_Enabled) -> int:
    """Create a member month table for assigned members"""
    dfs_input = {
        path: spark.table(f"{INPUT}.{path}")
        for path in [
            'member_time_windows',
            'time_periods',
            'members',
            'risk_scores',
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

    if Currently_Assigned_Enabled:
        memmos_filter = F.col('elig_month').between(
                            min_incurred_date,
                            max_incurred_date,
                        )
    else:
        memmos_filter = (F.col('elig_month').between(
                            min_incurred_date,
                            max_incurred_date
                        )) & (F.col('assignment_indicator') == 'Y')

    member_months = dfs_input['member_time_windows'].filter(
        memmos_filter
    ).groupBy(
        'member_id',
        'elig_month',
    ).agg(
        F.sum('memmos_medical').alias('memmos')
    )

    recent_info_window = Window().partitionBy(
        'member_id',
        'elig_month',
    ).orderBy(
        F.desc('date_end'),
    )

    recent_info = dfs_input['member_time_windows'].filter(
        memmos_filter
    ).select(
        '*',
        F.row_number().over(recent_info_window).alias('order'),
    ).filter(
        'order = 1'
    )

    risk_scores = dfs_input['risk_scores'].join(
        dfs_input['time_periods'].where(F.col('months_of_claims_runout') == RUNOUT),
        on='time_period_id',
        how='inner'
    )

    if Currently_Assigned_Enabled:
        current_assigned = dfs_input['members'].filter(
            F.col('assignment_indicator') == 'Y'
        ).select(
            'member_id',
        )        
        
        member_join = member_months.join(
            recent_info,
            on=['member_id', 'elig_month'],
            how='inner'
        ).join(
            risk_scores,
            on='member_id',
            how='left_outer'
        ).join(
            current_assigned,
            on='member_id',
            how='inner',
        ).select(
            'member_id',
            'elig_month',
            F.col('elig_status_1').alias('elig_status'),
            member_months.memmos,
            'risk_score',
            F.when(
                member_months.memmos > 0,
                F.lit('Y'),
            ).otherwise(
                F.lit('N')
            ).alias('cover_medical'),
        ).where(
            F.col('elig_status') != 'Unknown'
        )
        member_join = member_months.join(
            recent_info,
            on=['member_id', 'elig_month'],
            how='inner'
        ).join(
            risk_scores,
            on='member_id',
            how='left_outer'
        ).join(
            current_assigned,
            on='member_id',
            how='inner',
        ).select(
            'member_id',
            'elig_month',
            F.col('elig_status_1').alias('elig_status'),
            member_months.memmos,
            'risk_score',
            F.when(
                member_months.memmos > 0,
                F.lit('Y'),
            ).otherwise(
                F.lit('N')
            ).alias('cover_medical'),
        ).where(
            F.col('elig_status') != 'Unknown'
        )            
    else:
        member_join = member_months.join(
            recent_info,
            on=['member_id', 'elig_month'],
            how='inner'
        ).join(
            risk_scores,
            on='member_id',
            how='left_outer'
        ).select(
            'member_id',
            'elig_month',
            F.col('elig_status_1').alias('elig_status'),
            member_months.memmos,
            'risk_score',
            F.when(
                member_months.memmos > 0,
                F.lit('Y'),
            ).otherwise(
                F.lit('N')
            ).alias('cover_medical'),
        ).where(
            F.col('elig_status') != 'Unknown'
        )

    print(f"Time Period: {min_incurred_date} to {max_incurred_date}")
    print(f"Currently Assigned: {Currently_Assigned_Enabled}, YTD: {YTD_Only}")
    display(member_join.groupBy('elig_status').agg((F.sum('memmos')/ 12).alias('personyears')))
    member_join.write.mode("overwrite").saveAsTable(f'{OUTPUT}.member_months')

    return 0

    

if __name__ == '__main__':
    YTD_Only = False
    Currently_Assigned_Enabled = True
    RETURN_CODE = Members(YTD_Only,Currently_Assigned_Enabled)
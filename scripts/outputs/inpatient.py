"""
### CODE OWNERS: Umang Gupta, Pierre Cornell
### OBJECTIVE:
  Calculate inpatient metrics used in comparator report.
### DEVELOPER NOTES:
  None
"""
import logging

from prm.spark.app import SparkApp
import pyspark.sql.functions as spark_funcs
from prm.dates.utils import date_as_month
from prm.spark.io_sas import read_sas_data

import comparator_report.meta.project

LOGGER = logging.getLogger(__name__)
META_SHARED = comparator_report.meta.project.gather_metadata()

NAME_MODULE = 'outputs'
PATH_INPUTS = META_SHARED['path_data_nyhealth_shared'] / NAME_MODULE
PATH_OUTPUTS = META_SHARED['path_data_comparator_report'] / NAME_MODULE
PATH_RISKADJ = META_SHARED[15, 'out']

runout = 3

# =============================================================================
# LIBRARIES, LOCATIONS, LITERALS, ETC. GO ABOVE HERE
# =============================================================================
def calc_pqi(
        outclaims: "DataFrame"
        ) -> "DataFrame":
    
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

def calc_psa(
        outclaims: "DataFrame",
        ref_psa: "DataFrame"
        ) -> "DataFrame":
    
    outclaims_psa = outclaims.where(
            (spark_funcs.col('prm_pref_sensitive_category') != 'Not PSA') &
            (spark_funcs.col('prm_line') != 'I31')
            )
             
    psa_gen = outclaims_psa.select(
            'elig_status',
            'prm_admits',
            ).groupBy(
                'elig_status',
            ).agg(
                spark_funcs.sum('prm_admits').alias('metric_value'),
            )

    psa_ind = outclaims_psa.select(
            'elig_status',
            'prm_pref_sensitive_category',
            'prm_admits',
            ).groupBy(
                'elig_status',
                'prm_pref_sensitive_category',
            ).agg(
                spark_funcs.sum('prm_admits').alias('metric_value')
            ).join(
                ref_psa,
                on='prm_pref_sensitive_category',
                how='left_outer',
            ).select(
                'elig_status',
                spark_funcs.concat(
                    spark_funcs.lit('psa_admits_'),
                    spark_funcs.col('metric_id'),
                ).alias('metric_id'),
                'metric_value',
            )
    
    psas = psa_gen.select(
            'elig_status',
            spark_funcs.lit('pref_sens').alias('metric_id'),
            'metric_value',
            ).union(
                psa_ind
            )
            
    return psas

def calc_one_day(
        outclaims: "DataFrame"
        ) -> "DataFrame":
    
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
        
    outclaims_ra = outclaims.where(
            spark_funcs.col('prm_line') != 'I31'
            ).withColumn(
                'mcrm_line',
                spark_funcs.col('prm_line').substr(1, 3)
            )

    risk_score = member_months.select(
                'elig_status',
                'memmos',
                'risk_score',
            ).groupBy(
                'elig_status',
            ).agg(
                spark_funcs.format_number((spark_funcs.sum(spark_funcs.col('memmos')*spark_funcs.col('risk_score')) / spark_funcs.sum(spark_funcs.col('memmos'))), 2).alias('risk_score_avg')
            )
   
    outclaims_admits = outclaims_ra.select(
                'elig_status',
                'prm_line',
                'mcrm_line',
                'prm_admits',
            ).groupBy(
                'elig_status',
                'prm_line',
                'mcrm_line',
            ).agg(
                spark_funcs.sum('prm_admits').alias('admits')
            ).join(
                risk_score,
                on='elig_status',
                how='left_outer',
            )
    
    outclaims_util = outclaims_admits.join(
                hcc_risk_adj,
                on='mcrm_line',
                how='left_outer',
            ).select(
                'elig_status',
                'mcrm_line',
                'prm_line',
                'admits',
                'risk_score_avg',
                spark_funcs.when(
                    spark_funcs.col('factor_util').isNull(),
                    spark_funcs.lit(1)
                ).otherwise(
                    spark_funcs.col('factor_util')
                ).alias('factor_util'),
                spark_funcs.when(
                    spark_funcs.col('risk_score_avg').between(
                        spark_funcs.col('hcc_range_bottom'),
                        spark_funcs.col('hcc_range_top'),
                        ),
                    spark_funcs.lit('Y'),
                ).otherwise(
                    spark_funcs.when(
                        spark_funcs.col('factor_util').isNull(),
                        spark_funcs.lit('Y')
                    ).otherwise(
                        spark_funcs.lit('N')
                    )
                ).alias('keep_flag'),
            ).where(
                spark_funcs.col('keep_flag') == 'Y'
            ).withColumn(
                'admits_riskadj',
                spark_funcs.col('admits') / spark_funcs.col('factor_util'),
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
    
    med = outclaims_util.where(
            spark_funcs.col('mcrm_line') == 'I11'
            ).select(
                'elig_status',
                spark_funcs.lit('medical').alias('metric_id'),
                'admits',
            ).groupBy(
                'elig_status',
                'metric_id',
            ).agg(
                spark_funcs.sum('admits').alias('metric_value')
            ).union(
                outclaims_util.where(
                    spark_funcs.col('mcrm_line') == 'I11'
                ).select(
                    'elig_status',
                    spark_funcs.lit('medical_riskadj').alias('metric_id'),
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
            spark_funcs.col('mcrm_line') == 'I12'
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
                    spark_funcs.col('mcrm_line') == 'I12'
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
                med
            ).union(
                med_gen
            ).union(
                surgery
            )
    
    return type_union
                
def calc_readmits(
       outclaims: "DataFrame"
       ) -> "DataFrame":
        
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
    
    metrics = potential.union(readmits)
    
    return metrics
    
def main() -> int:
    sparkapp = SparkApp(META_SHARED['pipeline_signature'])
    
    dfs_input = {
            path.stem: sparkapp.load_df(path)
            for path in [
                    PATH_OUTPUTS / 'member_months.parquet',
                    PATH_INPUTS / 'time_periods.parquet',
                    PATH_INPUTS / 'outclaims.parquet',
                    PATH_INPUTS / 'decor_case.parquet',
                    ]
            }
    
    min_incurred_date, max_incurred_date = dfs_input['time_periods'].where(
            spark_funcs.col('months_of_claims_runout') == runout
            ).select(
                    spark_funcs.col('reporting_date_start').alias('min_incurred_date'),
                    spark_funcs.col('reporting_date_end').alias('max_incurred_date'),
            ).collect()[0]
    
    member_months = dfs_input['member_months'].where(
                spark_funcs.col('cover_medical') == 'Y'
            )
    
    decor_limited = dfs_input['decor_case'].select(
            'member_id',
            'caseadmitid',
            *[
                column
                for column in dfs_input['decor_case'].columns
                if column.startswith('prm_pref_')
            ],
            )
    
    ref_psa = sparkapp.session.createDataFrame(
        [('Knee Replacement', 'kneereplacement'),
         ('CABG/PTCA (DRG)', 'cabgptcadrg'),
         ('TURP', 'turp'),
         ('Bariatric Surgery', 'bariatricsurgery'),
         ('TURP (DRG)', 'turpdrg'),
         ('Hip Replacement', 'hipreplacement'),
         ('PTCA', 'ptca'),
         ('Laminectomy/Spinal Fusion', 'laminectomyspinalfusi'),
         ('Hysterectomy', 'hysterectomy'),
         ('CABG', 'cabg'),
         ('Hip/Knee Replacement (DRG)', 'hipkneereplacementdrg'),
         ('Uterine & Adnexa (DRG)', 'uterineadnexadrg')],
        schema=['prm_pref_sensitive_category', 'metric_id']
    )
    
    outclaims = dfs_input['outclaims'].where(
            spark_funcs.col('prm_fromdate').between(
                    min_incurred_date,
                    max_incurred_date,)
            ).withColumn(
                'month',
                date_as_month(spark_funcs.col('prm_fromdate'))
            ).join(
                decor_limited,
                on=['member_id', 'caseadmitid'],
                how='left_outer'
            ).where(
                spark_funcs.col('prm_line').like('I%')
            )
            
    outclaims_mem = outclaims.join(
                    member_months,
                    on=(outclaims.member_id == member_months.member_id)
                       & (outclaims.month == member_months.elig_month),
                    how = 'inner'
            )
    
    hcc_risk_adj = read_sas_data(
            sparkapp,
            PATH_RISKADJ / 'mcrm_hcc_calibrations.sas7bdat',
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
    
    psa_summary = calc_psa(outclaims_mem, ref_psa)
    
    one_day_summary = calc_one_day(outclaims_mem)
           
    risk_adj_summary = calc_risk_adj(outclaims_mem, member_months, hcc_risk_adj)
       
    readmit = calc_readmits(outclaims_mem)
    
    inpatient_metrics = pqi_summary.union(
            psa_summary
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

    hcc_risk_adj.unpersist()

    return 0

if __name__ == '__main__':
    import sys
    import prm.utils.logging_ext
    import prm.spark.defaults_prm
    
    prm.utils.logging_ext.setup_logging_stdout_handler()
    SPARK_DEFAULTS_PRM = prm.spark.defaults_prm.get_spark_defaults(META_SHARED)
    
    with SparkApp(META_SHARED['pipeline_signature'], **SPARK_DEFAULTS_PRM):
        RETURN_CODE = main()
    
    sys.exit(RETURN_CODE)

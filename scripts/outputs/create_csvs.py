"""
### CODE OWNERS: Umang Gupta, Pierre Cornell
### OBJECTIVE:
  Output all metrics and PAC DRG Summary to pipe delimited txt files.
### DEVELOPER NOTES:
  None
"""
# pylint: disable=no-member
import logging

from prm.spark.app import SparkApp
import pyspark.sql.functions as spark_funcs
from prm.spark.io_txt import export_csv

import comparator_report.meta.project

LOGGER = logging.getLogger(__name__)
META_SHARED = comparator_report.meta.project.gather_metadata()

NAME_MODULE = 'outputs'
PATH_OUTPUTS = META_SHARED['path_data_comparator_report'] / NAME_MODULE
PATH_INPUTS = META_SHARED['path_data_nyhealth_shared'] / NAME_MODULE
RUNOUT = 3
NONESRD = ['Aged Non-Dual', 'Aged Dual', 'Disabled']
# =============================================================================
# LIBRARIES, LOCATIONS, LITERALS, ETC. GO ABOVE HERE
# =============================================================================

def main() -> int:
    """Output all calculated metrics to CSVs"""
    sparkapp = SparkApp(META_SHARED['pipeline_signature'])

    dfs_input = {
        path.stem: sparkapp.load_df(path)
        for path in PATH_OUTPUTS.glob('*.parquet')
    }

    time_periods = sparkapp.load_df(PATH_INPUTS / 'time_periods.parquet')

    min_incurred_date, max_incurred_date = time_periods.where(
        spark_funcs.col('months_of_claims_runout') == RUNOUT
    ).select(
        spark_funcs.col('reporting_date_start').alias('min_incurred_date'),
        spark_funcs.col('reporting_date_end').alias('max_incurred_date'),
    ).collect()[0]

    time_period = str(min_incurred_date.year) + 'Q' + str((min_incurred_date.month - 1) // 3 + 1) + '_' + str(max_incurred_date.year) + 'Q' + str((max_incurred_date.month - 1) // 3 + 1)

    metrics_stack = dfs_input['basic_metrics'].union(
        dfs_input['inpatient_metrics']
    ).union(
        dfs_input['outpatient_metrics']
    ).union(
        dfs_input['snf_metrics']
    ).union(
        dfs_input['er_metrics']
    ).union(
        dfs_input['pac_metrics']
    ).union(
        dfs_input['mr_line_metrics']
    )

    nonesrd_metrics = metrics_stack.where(
        spark_funcs.col('elig_status').isin(NONESRD)
    ).select(
        spark_funcs.lit('Non-ESRD').alias('elig_status'),
        'metric_id',
        'metric_value',
    ).groupBy(
        'elig_status',
        'metric_id',
    ).agg(
        spark_funcs.sum('metric_value').alias('metric_value')
    )

    metrics_out = metrics_stack.union(
        nonesrd_metrics
    ).union(
        dfs_input['eol_metrics']
    ).select(
        spark_funcs.lit(META_SHARED['name_client']).alias('name_client'),
        spark_funcs.lit(time_period).alias('time_period'),
        'elig_status',
        spark_funcs.lit('').alias('metric_category'),
        'metric_id',
        spark_funcs.lit('').alias('metric_name'),
        'metric_value',
    ).withColumn(
        'idx',
        spark_funcs.regexp_replace(
            spark_funcs.concat(
                spark_funcs.col('name_client'),
                spark_funcs.lit('_'),
                spark_funcs.col('time_period'),
                spark_funcs.lit('_'),
                spark_funcs.col('elig_status'),
                spark_funcs.lit('_'),
                spark_funcs.col('metric_id'),
            ),
            ' ',
            ''
        )
    ).coalesce(10)

    pac_drg = dfs_input['pac_drg_summary'].select(
        spark_funcs.lit(META_SHARED['name_client']).alias('name_client'),
        spark_funcs.lit(time_period).alias('time_period'),
        'elig_status',
        'prm_drg',
        'pac_count',
        'pac_acute_count',
        'pac_rehab_count',
        'pac_snf_count',
        'pac_hh_count',
        'pac_death_count',
    )

    betos_summary = dfs_input['betos'].select(
        spark_funcs.lit(META_SHARED['name_client']).alias('name_client'),
        spark_funcs.lit(time_period).alias('time_period'),
        '*',
    )

    sparkapp.save_df(
        metrics_out,
        PATH_OUTPUTS / 'metrics.parquet',
    )

    export_csv(
        metrics_out,
        PATH_OUTPUTS / 'metrics.txt',
        sep='|',
        header=True,
        single_file=True,
    )

    export_csv(
        pac_drg,
        PATH_OUTPUTS / 'pac_drg_summary.txt',
        sep='|',
        header=True,
        single_file=True,
    )

    export_csv(
        betos_summary,
        PATH_OUTPUTS / 'betos_summary.txt',
        sep='|',
        header=True,
        single_file=True,
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

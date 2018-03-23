"""
### CODE OWNERS: Umang Gupta, Pierre Cornell
### OBJECTIVE:
  Create Membership basis for all other queries
### DEVELOPER NOTES:
  None
"""
import logging

from prm.spark.app import SparkApp
from prm.spark.io_txt import export_csv

import comparator_report.meta.project

LOGGER = logging.getLogger(__name__)
META_SHARED = comparator_report.meta.project.gather_metadata()

NAME_MODULE = 'outputs'
PATH_INPUTS = META_SHARED['path_data_comparator_report'] / NAME_MODULE

# =============================================================================
# LIBRARIES, LOCATIONS, LITERALS, ETC. GO ABOVE HERE
# =============================================================================

def main() -> int:
    sparkapp = SparkApp(META_SHARED['pipeline_signature'])
    
    dfs_input = {
            path.stem: sparkapp.load_df(path)
            for path in PATH_INPUTS.glob('*.parquet')
            }
    
    metrics_stack = dfs_input['basic_metrics'].union(
                dfs_input['inpatient_metrics']
            ).union(
                dfs_input['outpatient_metrics']
            ).union(
                dfs_input['eol_metrics']
            ).union(
                dfs_input['snf_metrics']
            ).union(
                dfs_input['er_metrics']
            )
    
    sparkapp.save_df(
            metrics_stack,
            PATH_INPUTS / 'metrics.parquet',
            )
    
    export_csv(
        metrics_stack,
        PATH_INPUTS / 'metrics.csv',
        sep='|',
        header=True,
        single_file=True,
    )
    
    export_csv(
        dfs_input['costmodel'],
        PATH_INPUTS / 'cm_exp.csv',
        sep='|',
        header=True,
        single_file=True,
    )
    
    export_csv(
        dfs_input['mem_out'],
        PATH_INPUTS / 'mem.csv',
        sep='|',
        header=True,
        single_file=True,
    )

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

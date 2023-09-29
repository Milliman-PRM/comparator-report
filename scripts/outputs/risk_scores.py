"""
### CODE OWNERS: Giang Vu, Ocean Liu
### OBJECTIVE:
  Add count of HCC markers for each member
### DEVELOPER NOTES:
  None
"""
import logging
import os

from datetime import date
from prm.dates.utils import date_as_month
from prm.spark.app import SparkApp
import pyspark.sql.functions as spark_funcs
from pyspark.sql import Window
import comparator_report.meta.project

LOGGER = logging.getLogger(__name__)
META_SHARED = comparator_report.meta.project.gather_metadata()

NAME_MODULE = 'outputs'
PATH_INPUTS = META_SHARED['path_data_nyhealth_shared'] / NAME_MODULE
PATH_OUTPUTS = META_SHARED['path_data_comparator_report'] / NAME_MODULE

HCC_COLS = [
    "hcc1",
    "hcc2",
    "hcc6",
    "hcc8",
    "hcc9",
    "hcc10",
    "hcc11",
    "hcc12",
    "hcc17",
    "hcc18",
    "hcc19",
    "hcc21",
    "hcc22",
    "hcc23",
    "hcc27",
    "hcc28",
    "hcc29",
    "hcc33",
    "hcc34",
    "hcc35",
    "hcc39",
    "hcc40",
    "hcc46",
    "hcc47",
    "hcc48",
    "hcc51",
    "hcc52",
    "hcc54",
    "hcc55",
    "hcc56",
    "hcc57",
    "hcc58",
    "hcc59",
    "hcc60",
    "hcc70",
    "hcc71",
    "hcc72",
    "hcc73",
    "hcc74",
    "hcc75",
    "hcc76",
    "hcc77",
    "hcc78",
    "hcc79",
    "hcc80",
    "hcc82",
    "hcc83",
    "hcc84",
    "hcc85",
    "hcc86",
    "hcc87",
    "hcc88",
    "hcc96",
    "hcc99",
    "hcc100",
    "hcc103",
    "hcc104",
    "hcc106",
    "hcc107",
    "hcc108",
    "hcc110",
    "hcc111",
    "hcc112",
    "hcc114",
    "hcc115",
    "hcc122",
    "hcc124",
    "hcc135",
    "hcc136",
    "hcc137",
    "hcc138",
    "hcc157",
    "hcc158",
    "hcc159",
    "hcc161",
    "hcc162",
    "hcc166",
    "hcc167",
    "hcc169",
    "hcc170",
    "hcc173",
    "hcc176",
    "hcc186",
    "hcc188",
    "hcc189",
]

# =============================================================================
# LIBRARIES, LOCATIONS, LITERALS, ETC. GO ABOVE HERE
# =============================================================================

def main() -> int:
    """Create a table with count of HCC markers for each member"""
    sparkapp = SparkApp(META_SHARED['pipeline_signature'])

    dfs_input = {
        path.stem: sparkapp.load_df(path)
        for path in [
            PATH_INPUTS / 'member_time_windows.parquet',
            PATH_INPUTS / 'time_periods.parquet',
            PATH_INPUTS / 'members.parquet',
            PATH_INPUTS / 'risk_scores.parquet',
            PATH_INPUTS / 'outclaims.parquet',
        ]
    }
    
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
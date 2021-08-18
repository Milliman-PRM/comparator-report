"""
### CODE OWNERS: Umang Gupta, Pierre Cornell

### OBJECTIVE:
  Run the comaprator report pipeline

### DEVELOPER NOTES:
  Uses shared metadata from PRM
"""
import logging
import luigi

from indypy.nonstandard.ext_luigi import mutate_config

import prm_ny_data_share.meta.project
import comparator_report.meta.project

from comparator_report.pipeline.definitions.outputs import CreateCSVs, Members, EOL

LOGGER = logging.getLogger(__name__)

# =============================================================================
# LIBRARIES, LOCATIONS, LITERALS, ETC. GO ABOVE HERE
# =============================================================================



def main() -> int:
    """A function to enclose the execution of business logic."""
    LOGGER.info('Running Comparator Report with Luigi')

    prm_ny_data_share.meta.project.setup_project()
    comparator_report.meta.project.setup_project()
    META_SHARED = comparator_report.meta.project.gather_metadata()

    mutate_config()

    _comparator_kwargs = {
        'YTD_Only': 'True',
        'Currently_Assigned_Enabled': 'True',
        '7_1ACOFlag_Enabled': 'False',
        'Custom_Min_Incurred': 'None',
        'Custom_Max_Incurred': 'None',
    }

    Members.kwargs_passthru = _comparator_kwargs
    EOL.kwargs_passthru = _comparator_kwargs
    CreateCSVs.kwargs_passthru = _comparator_kwargs

    return int(not luigi.build([CreateCSVs(META_SHARED['pipeline_signature'])]))


if __name__ == '__main__':
    # pylint: disable=wrong-import-position, wrong-import-order, ungrouped-imports
    import sys
    import prm.utils.logging_ext
    import prm.spark.defaults_prm

    prm.utils.logging_ext.setup_logging_stdout_handler()
    RETURN_CODE = main()

    sys.exit(RETURN_CODE)

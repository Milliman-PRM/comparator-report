"""
### CODE OWNERS: Ben Copeland, Jason Altieri

### OBJECTIVE:
  Prepare ACO insights for processing

### DEVELOPER NOTES:
  Uses shared metadata from PRM
"""
import logging

import comparator_report.meta.project

LOGGER = logging.getLogger(__name__)

# =============================================================================
# LIBRARIES, LOCATIONS, LITERALS, ETC. GO ABOVE HERE
# =============================================================================



def main() -> int:
    """A function to enclose the execution of business logic."""
    LOGGER.info('Preparing project for shared data warehouse processing')

    comparator_report.meta.project.setup_project()
    return 0


if __name__ == '__main__':
    # pylint: disable=wrong-import-position, wrong-import-order, ungrouped-imports
    import sys
    import prm.utils.logging_ext
    import prm.spark.defaults_prm

    prm.utils.logging_ext.setup_logging_stdout_handler()
    RETURN_CODE = main()

    sys.exit(RETURN_CODE)

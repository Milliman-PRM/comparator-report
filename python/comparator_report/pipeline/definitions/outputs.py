"""
## CODE OWNERS: Ben Copeland, Chas Busenburg, Jason Altieri

### OBJECTIVE:
  Define a Luigi pipeline to run output collection programs in aco-insight

### DEVELOPER NOTES:

"""
from indypy.nonstandard.ext_luigi import build_logfile_name, IndyPyLocalTarget
from prm.ext_luigi.base_tasks import PRMPythonTask, RequirementsContainer

import prm_ny_data_share.pipeline.definitions.outputs as outputs_data_share
import comparator_report.meta.project
from prm.meta.utils import get_output_filename, PATH_PUBLIC_TRIGGERS

NAME_MODULE = 'outputs'
PATH_SCRIPTS = comparator_report.meta.project.PATH_SCRIPTS
META_SHARED = comparator_report.meta.project.gather_metadata()

# =============================================================================
# LIBRARIES, LOCATIONS, LITERALS, ETC. GO ABOVE HERE
# =============================================================================


class Members(PRMPythonTask): # pragma: no cover
    """Run members.py"""

    requirements = RequirementsContainer(
        outputs_data_share.Validate,
    )

    def output(self):
        names_output = {
            'member_months.parquet',
        }
        return [
            IndyPyLocalTarget(META_SHARED['path_data_comparator_report'] / NAME_MODULE / name)
            for name in names_output
        ]

    def run(self):  # pylint: disable=arguments-differ
        """Run the Luigi job"""
        program = PATH_SCRIPTS / NAME_MODULE / "members.py"
        super().run(
            program,
            path_log=build_logfile_name(
                program,
                META_SHARED['path_logs_comparator_report'] / NAME_MODULE
            )
        )
        # pylint: enable=arguments-differ


class Inpatient(PRMPythonTask): # pragma: no cover
    """Run inpatient.py"""

    requirements = RequirementsContainer(
        Members,
    )

    def output(self):
        names_output = {
            'inpatient_metrics.parquet',
        }
        return [
            IndyPyLocalTarget(META_SHARED['path_data_comparator_report'] / NAME_MODULE / name)
            for name in names_output
        ]

    def run(self):  # pylint: disable=arguments-differ
        """Run the Luigi job"""
        program = PATH_SCRIPTS / NAME_MODULE / "inpatient.py"
        super().run(
            program,
            path_log=build_logfile_name(
                program,
                META_SHARED['path_logs_comparator_report'] / NAME_MODULE
            )
        )
        # pylint: enable=arguments-differ

class PAC(PRMPythonTask): # pragma: no cover
    """Run pac.py"""

    requirements = RequirementsContainer(
        Members,
    )

    def output(self):
        names_output = {
            'pac_metrics.parquet',
        }
        return [
            IndyPyLocalTarget(META_SHARED['path_data_comparator_report'] / NAME_MODULE / name)
            for name in names_output
        ]

    def run(self):  # pylint: disable=arguments-differ
        """Run the Luigi job"""
        program = PATH_SCRIPTS / NAME_MODULE / "pac.py"
        super().run(
            program,
            path_log=build_logfile_name(
                program,
                META_SHARED['path_logs_comparator_report'] / NAME_MODULE
            )
        )
        # pylint: enable=arguments-differ

class Outpatient(PRMPythonTask): # pragma: no cover
    """Run outpatient.py"""

    requirements = RequirementsContainer(
        Members,
    )

    def output(self):
        names_output = {
            'outpatient_metrics.parquet',
        }
        return [
            IndyPyLocalTarget(META_SHARED['path_data_comparator_report'] / NAME_MODULE / name)
            for name in names_output
        ]

    def run(self):  # pylint: disable=arguments-differ
        """Run the Luigi job"""
        program = PATH_SCRIPTS / NAME_MODULE / "outpatient.py"
        super().run(
            program,
            path_log=build_logfile_name(
                program,
                META_SHARED['path_logs_comparator_report'] / NAME_MODULE
            )
        )
        # pylint: enable=arguments-differ


class SNF(PRMPythonTask): # pragma: no cover
    """Run snf.py"""

    requirements = RequirementsContainer(
        Members,
    )

    def output(self):
        names_output = {
            'snf_metrics.parquet',
        }
        return [
            IndyPyLocalTarget(META_SHARED['path_data_comparator_report'] / NAME_MODULE / name)
            for name in names_output
        ]

    def run(self):  # pylint: disable=arguments-differ
        """Run the Luigi job"""
        program = PATH_SCRIPTS / NAME_MODULE / "snf.py"
        super().run(
            program,
            path_log=build_logfile_name(
                program,
                META_SHARED['path_logs_comparator_report'] / NAME_MODULE
            )
        )
        # pylint: enable=arguments-differ


class ER(PRMPythonTask): # pragma: no cover
    """Run er.py"""

    requirements = RequirementsContainer(
        Members,
    )

    def output(self):
        names_output = {
            'er_metrics.parquet',
        }
        return [
            IndyPyLocalTarget(META_SHARED['path_data_comparator_report'] / NAME_MODULE / name)
            for name in names_output
        ]

    def run(self):  # pylint: disable=arguments-differ
        """Run the Luigi job"""
        program = PATH_SCRIPTS / NAME_MODULE / "er.py"
        super().run(
            program,
            path_log=build_logfile_name(
                program,
                META_SHARED['path_logs_comparator_report'] / NAME_MODULE
            )
        )
        # pylint: enable=arguments-differ


class EOL(PRMPythonTask): # pragma: no cover
    """Run eol.py"""

    requirements = RequirementsContainer(
        Members,
    )

    def output(self):
        names_output = {
            'eol_metrics.parquet',
        }
        return [
            IndyPyLocalTarget(META_SHARED['path_data_comparator_report'] / NAME_MODULE / name)
            for name in names_output
        ]

    def run(self):  # pylint: disable=arguments-differ
        """Run the Luigi job"""
        program = PATH_SCRIPTS / NAME_MODULE / "eol.py"
        super().run(
            program,
            path_log=build_logfile_name(
                program,
                META_SHARED['path_logs_comparator_report'] / NAME_MODULE
            )
        )
        # pylint: enable=arguments-differ


class BasicMetrics(PRMPythonTask): # pragma: no cover
    """Run basic.py"""

    requirements = RequirementsContainer(
        Members,
    )

    def output(self):
        names_output = {
            'basic_metrics.parquet',
        }
        return [
            IndyPyLocalTarget(META_SHARED['path_data_comparator_report'] / NAME_MODULE / name)
            for name in names_output
        ]

    def run(self):  # pylint: disable=arguments-differ
        """Run the Luigi job"""
        program = PATH_SCRIPTS / NAME_MODULE / "basic.py"
        super().run(
            program,
            path_log=build_logfile_name(
                program,
                META_SHARED['path_logs_comparator_report'] / NAME_MODULE
            )
        )
        # pylint: enable=arguments-differ


class CostModel(PRMPythonTask): # pragma: no cover
    """Run costmodel.py"""

    requirements = RequirementsContainer(
        Members,
    )

    def output(self):
        names_output = {
            'costmodel.parquet',
        }
        return [
            IndyPyLocalTarget(META_SHARED['path_data_comparator_report'] / NAME_MODULE / name)
            for name in names_output
        ]

    def run(self):  # pylint: disable=arguments-differ
        """Run the Luigi job"""
        program = PATH_SCRIPTS / NAME_MODULE / "costmodel.py"
        super().run(
            program,
            path_log=build_logfile_name(
                program,
                META_SHARED['path_logs_comparator_report'] / NAME_MODULE
            )
        )
        # pylint: enable=arguments-differ

class MemOut(PRMPythonTask): # pragma: no cover
    """Run mem_out.py"""

    requirements = RequirementsContainer(
        Members,
    )

    def output(self):
        names_output = {
            'mem_out.parquet',
        }
        return [
            IndyPyLocalTarget(META_SHARED['path_data_comparator_report'] / NAME_MODULE / name)
            for name in names_output
        ]

    def run(self):  # pylint: disable=arguments-differ
        """Run the Luigi job"""
        program = PATH_SCRIPTS / NAME_MODULE / "mem_out.py"
        super().run(
            program,
            path_log=build_logfile_name(
                program,
                META_SHARED['path_logs_comparator_report'] / NAME_MODULE
            )
        )
        # pylint: enable=arguments-differ

class CreateCSVs(PRMPythonTask): # pragma: no cover
    """Run create_csvs.py"""

    requirements = RequirementsContainer(
        CostModel,
        MemOut,
        BasicMetrics,
        EOL,
        Inpatient,
        PAC,
        Outpatient,
        ER,
        SNF,
    )

    def output(self):
        names_output = {
            'metrics.csv',
            'cm_exp.csv',
            'mem.csv',
        }
        return [
            IndyPyLocalTarget(META_SHARED['path_data_comparator_report'] / NAME_MODULE / name)
            for name in names_output
        ]

    def run(self):  # pylint: disable=arguments-differ
        """Run the Luigi job"""
        program = PATH_SCRIPTS / NAME_MODULE / "create_csvs.py"
        super().run(
            program,
            path_log=build_logfile_name(
                program,
                META_SHARED['path_logs_comparator_report'] / NAME_MODULE
            )
        )
        # pylint: enable=arguments-differ        



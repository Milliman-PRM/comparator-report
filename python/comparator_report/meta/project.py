"""
### CODE OWNERS: Ben Copeland, Umang Gupta

### OBJECTIVE:
  Tools for managing metadata and project structure
  
### DEVELOPER NOTES:
  Metadata will leverage PRM analytics-pipeline metadata
"""
import logging
import typing
import datetime
import os
from pathlib import Path

import prm_ny_data_share.meta.project

LOGGER = logging.getLogger(__name__)

PATH_SCRIPTS = Path(os.environ['COMPARATOR_REPORT_HOME']) / 'scripts'

# =============================================================================
# LIBRARIES, LOCATIONS, LITERALS, ETC. GO ABOVE HERE
# =============================================================================


def _derive_additional_metadata(
        prm_meta: typing.Mapping,
    ) -> typing.Mapping:
    """Collect metadata that will be added to PRM metadata"""
    meta_new = dict()
    meta_new['path_data_comparator_report'] = prm_meta['path_project_data'] / 'comparator_report'
    meta_new['path_logs_comparator_report'] = prm_meta['path_project_logs'] / 'comparator_report'
    return meta_new


def gather_metadata() -> typing.Mapping:
    """Collect PRM metadata and component metadata"""
    meta = prm_ny_data_share.meta.project.gather_metadata()
    meta.update(
        _derive_additional_metadata(meta)
        )
    return meta


def _convert_map_to_sas_macro(
        key: str,
        value: typing.Any,
    ) -> str:
    """Convert a key-value pair to a string to write to SAS"""
    format_str_default = '%let {} = {};\n'
    format_str_date = '%let {} = %sysfunc(inputn({}, YYMMDD10.));\n'

    assert len(key) <= 32, 'SAS macro variable names must be less than 32 characters'
    if isinstance(value, datetime.date):
        chosen_format_str = format_str_date
    else:
        chosen_format_str = format_str_default
    output = chosen_format_str.format(
        key,
        value,
        )
    return output


def _write_metadata_to_sas(
        path_output: Path,
        meta: typing.Mapping,
    ) -> None:
    """Create a script for converting new metadata into SAS macro variables"""
    try:
        _filename = __file__
    except NameError:
        _filename = 'comparator_report.meta.project._write_metadata_to_sas'

    with path_output.open('w', newline='') as fh_out:
        fh_out.write('/*Define macro variables for PRM/NYHealth data share*/\n')
        fh_out.write('/*Codegened by {} on {}*/\n'.format(
            _filename,
            datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            ))
        fh_out.write('/*Do not modify directly*/\n\n')
        for name, value in meta.items():
            macro_output = _convert_map_to_sas_macro(
                name,
                value,
                )
            fh_out.write(macro_output)


def _generate_module_names(
        path_scripts: Path=PATH_SCRIPTS,
    ) -> typing.Generator[Path, None, None]:
    """Collect all of the module names in the repository"""
    for path in path_scripts.iterdir():
        if path.is_dir():
            yield path.name


def _make_project_folders(
        meta_new: typing.Mapping,
    ) -> None:
    """Create project folders for comparator report work"""
    for value in meta_new.values():
        if isinstance(value, Path):
            value.mkdir(
                exist_ok=True,
                parents=True,
                )


    for module in _generate_module_names():
        (meta_new['path_data_comparator_report'] / module).mkdir(exist_ok=True)
        (meta_new['path_logs_comparator_report'] / module).mkdir(exist_ok=True)


def setup_project() -> None:
    """Prepare a PRM project for comparator report"""
    prm_meta = prm_ny_data_share.meta.project.parse_project_metadata()
    meta_new = _derive_additional_metadata(prm_meta)

    _make_project_folders(meta_new)

    _write_metadata_to_sas(
        meta_new['path_data_comparator_report'] / 'meta_comparator_report.sas',
        meta_new,
        )
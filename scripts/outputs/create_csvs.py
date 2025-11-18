"""
### CODE OWNERS: Umang Gupta, Pierre Cornell
### OBJECTIVE:
  Output all metrics and PAC DRG Summary to pipe delimited txt files.
### DEVELOPER NOTES:
  None
"""
from databricks.sdk.runtime import *

from datetime import date
import pyspark.sql.functions as F
from pyspark.sql import Window

INPUT = 'premier_data_feed'
OUTPUT = 'premier_data_feed'
RUNOUT = 3
NONESRD = ['Aged Non-Dual', 'Aged Dual', 'Disabled']

# =============================================================================
# LIBRARIES, LOCATIONS, LITERALS, ETC. GO ABOVE HERE
# =============================================================================

def export_csv(CATALOG, NAME, table, file_name):

    output_dir = f"/Volumes/{CATALOG}/{OUTPUT}/exports/{file_name}"
    download_folder = f"/Volumes/{CATALOG}/{OUTPUT}/exports/downloads"
    download_file = f"/Volumes/{CATALOG}/{OUTPUT}/exports/downloads/{NAME}_{file_name}.txt"

    try:
        dbutils.fs.mkdirs(download_folder)
    except:
        pass

    table.coalesce(1).write.mode("overwrite").option("delimiter", "|").csv(output_dir, header=True)
    files = dbutils.fs.ls(output_dir)
    part_file = [f.path for f in files if f.name.startswith("part-") and f.name.endswith(".csv")]

    # Move the part file to the desired location
    dbutils.fs.mv(part_file[0], download_file)

    return 0

def CreateCSVs(CATALOG, NAME) -> int:
    """Output all calculated metrics to CSVs"""

    dfs_input = {
        path: spark.table(f"{INPUT}.{path}")
        for path in [
            'basic_metrics',
            'inpatient_metrics',
            'outpatient_metrics',
            'snf_metrics',
            'er_metrics',
            'truncation',
            'eol_metrics',
            'pac_metrics',
            'pac_drg_summary',
            'mr_line_metrics',
            'time_periods'
        ]
    }

    min_incurred_date, max_incurred_date = dfs_input['time_periods'].where(
        F.col('months_of_claims_runout') == RUNOUT
    ).select(
        F.add_months(F.to_date(F.concat('time_period_id',F.lit('01')),'yyyyMMdd'),-11).alias('min_incurred_date'),
        F.last_day(F.to_date(F.concat('time_period_id',F.lit('01')),'yyyyMMdd')).alias('max_incurred_date')
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
        F.col('elig_status').isin(NONESRD)
    ).select(
        F.lit('Non-ESRD').alias('elig_status'),
        'metric_id',
        'metric_value',
    ).groupBy(
        'elig_status',
        'metric_id',
    ).agg(
        F.sum('metric_value').alias('metric_value')
    )

    metrics_out = metrics_stack.union(
        nonesrd_metrics
    ).union(
        dfs_input['eol_metrics']
    ).select(
        F.lit(NAME).alias('name_client'),
        F.lit(time_period).alias('time_period'),
        'elig_status',
        F.lit('').alias('metric_category'),
        'metric_id',
        F.lit('').alias('metric_name'),
        'metric_value',
    ).withColumn(
        'idx',
        F.regexp_replace(
            F.concat(
                F.col('name_client'),
                F.lit('_'),
                F.col('time_period'),
                F.lit('_'),
                F.col('elig_status'),
                F.lit('_'),
                F.col('metric_id'),
            ),
            ' ',
            ''
        )
    ).coalesce(10)

    pac_drg = dfs_input['pac_drg_summary'].select(
        F.lit(NAME).alias('name_client'),
        F.lit(time_period).alias('time_period'),
        'elig_status',
        'prm_drg',
        'pac_count',
        'pac_acute_count',
        'pac_rehab_count',
        'pac_snf_count',
        'pac_hh_count',
        'pac_death_count',
    )

    # betos_summary = dfs_input['betos'].select(
    #     F.lit(META_SHARED['name_client']).alias('name_client'),
    #     F.lit(time_period).alias('time_period'),
    #     '*',
    # )

    truncation_summary = dfs_input['truncation'].select(
        F.lit(NAME).alias('name_client'),
        F.lit(time_period).alias('time_period'),
        '*',
    )

    metrics_out.write.mode("overwrite").saveAsTable(f'{OUTPUT}.metrics')

    export_csv(CATALOG, NAME, metrics_out, 'metrics')
    export_csv(CATALOG, NAME, truncation_summary, 'truncation_summary')
    export_csv(CATALOG, NAME, pac_drg, 'pac_drg')


    # metrics_out.coalesce(1).write.mode("overwrite").option("delimiter", "|").csv(f"/Volumes/{CATALOG}/{OUTPUT}/exports/metrics_txt", header=True)

    # truncation_summary.coalesce(1).write.mode("overwrite").option("delimiter", "|").csv(f"/Volumes/{CATALOG}/{OUTPUT}/exports/truncation_summary_txt", header=True)

    # pac_drg.coalesce(1).write.mode("overwrite").option("delimiter", "|").csv(f"/Volumes/{CATALOG}/{OUTPUT}/exports/pac_drg_summary_txt", header=True)


    # export_csv(
    #     pac_drg,
    #     PATH_OUTPUTS / 'pac_drg_summary.txt',
    #     sep='|',
    #     header=True,
    #     single_file=True,
    # )

    # export_csv(
    #     betos_summary,
    #     PATH_OUTPUTS / 'betos_summary.txt',
    #     sep='|',
    #     header=True,
    #     single_file=True,
    # )

    # export_csv(
    #     truncation_summary,
    #     PATH_OUTPUTS / 'truncation_summary.txt',
    #     sep='|',
    #     header=True,
    #     single_file=True,
    # )       

    return 0

if __name__ == '__main__':

### Example
    RETURN_CODE = CreateCSVs("acohfh_processing","Henry Ford Physician Accountable Care Organization")


## Release Notes

A non-exhaustive list of what has changed in a more readable form than a commit history.
### V1.12.2
  - Updated truncation thresholds to 2022 Settlement AEXPU's Expenditure Truncation Thresholds/Factors (Including Covid episodes).
  - Changed the runout to a variable so the variable could be set in client_env.bat.
  - Added the 3_mos and 3_mos_7 runout options in the truncation.py for the purpose of creating 2022 settlement E&U report.

### V1.12.1
  - Updated prm_line of hospice in eol.py based on updated HCG Grouper v2023 codes
### V1.12.0
  - Updated truncation thresholds to 2022Q4 QEXPU's Expenditure Truncation Thresholds/Factors, Including COVID-19 Episodes (Based on National Assignable FFS Population).
  - Replaced costs_7 with costs_21 to fit the E&U comparator reports when claims cut off date changes each quarter
  - Added 7 days follow up vists denominator and numerator of medical and surgical claims into pac metrics
  
### v1.11.4
  - Add annual wellness visit denominator and numerator into comparator metrics
  - remove members that have no member months in the basics scripts

### v1.11.3
  - Fix the issue of counting decedents
  
### v1.11.2
  - Update truncation thresholds to 2020

### v1.11.1
  - Fix issue for elig statuses with risk scores between 1.99 and 2.0
  
### v1.11.0
  - Add functionality to run on 7/1 ACO based on `mem_report_hier_2`
  
### v1.10.0
  - Began sourcing PRM reference data from the reference-data repository specified by environment variable reference_data_pathref
  - Count `mr_cases_admits` instead of `mr_procs` for outpatient PSPs
  - Require membership eligibility on day of claim, similar to ACOI
  - Limit PAC claims to those with DRGs with benchmarks
  - Update SNF fromdate to be `prm_fromdate_case` instead of `prm_fromdate`
  
### v1.9.0
  - Make risk-adjusted metrics compatible with HCG 2019 changes

### v1.8.0
  - Allow user to toggle between `Currently Assigned` and `Assigned During Selected Period`
  - Export BETOS summary with varied runouts
  - Export IME, DSH, UCC summary along with Truncated costs by Elig Status
  
### v1.7.1
  - Fill in NULL risk scores with 0 when aggregated by Eligibility Status to facilitate join to risk adjustment table

### v1.7.0
  - Add metric to calculate truncated dollars by elig status
  - Update calculation for following metrics to align with interface changes
    - Pre-calculate Non-ESRD EOL metrics
    - Update `prm_util` to `mr_procs` when calculating outpatient PSP metrics
    - Remove member months with `Unknown` elig_status
  - Add Betos summary to outputs.
    - Summarize by Betos Code, PRM_line, and Elig_Status

### v1.6.0
  - Add an optional argument that allows the metrics to be calculated YTD instead of year rolling.
  
### v1.5.0
  - Change metrics to be calculated off of Currently Assigned = 'Y' instead of Assigned During Selected Period = 'Y'
  
### v1.4.0
  - Add util, costs, and admits for all prm_lines in addition to rolled up metrics previously shown.
  
### v1.3.0
  - Update prm_lines to match E&U Report
  
### v1.2.0
  - Add logic to calculate SNF Readmission rate
  
### v1.1.0
  - Update logic for preference sensitive admissions to preference sensitive procedures
  - Pylinting Updates
  
### v1.0.1
  - Update total age metric calculation to include memmos as weight
  
### v1.0.0
  - Calculate Metrics for Comparator Report
  - Align discharge metrics to PAC metrics instead of discharge status
  - Remove Mem_Table and CM_Exp tables used in comparator-report-legacy
  - Output metrics to pipe-delimited text files
  
### v0.1.0
  - Initial release of product component
    - Created python library and metadata structure
    - Added script to assist with promoting new releases



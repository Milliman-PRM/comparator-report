## Release Notes

A non-exhaustive list of what has changed in a more readable form than a commit history.
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



## Release Notes

A non-exhaustive list of what has changed in a more readable form than a commit history.
### v1.7.0
  - Add metric to calculated truncated dollars by rating category

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



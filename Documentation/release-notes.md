## Release Notes

A non-exhaustive list of what has changed in a more readable form than a commit history.

### v1.6.0

  - Total risk scores now match those listed in the comparator report. 
  - Added the dual function to the ref_prm_line and ref_leakage_line
  - Added variables to be cleared when clicking the Readme link 
  - Made DRG Descriptions an expression instead of a dimension 
  - updated the service line filter table to filter to 1 row when a selection is made. This stops current selections from clearing when a new service type is chosen
  - Removed the scrolling from the PAC DRG Benchmark table
  - Added in PMPY Benchmarks to Cost Summary and Cost Model
  - Updated PAC Summary table to be read-only
  - Removed leakage_desc from tables that have vIngoreServiceDims in the expression
  - Updated sort order in middle leakage table on the 2nd leakage tab to be sorted by Paid PBPY
  - Fixed filters from clearing when navigating from DRG Detail to Service line detail
  - Added in benchmark button and cleared all benchmarks when aged non-dual eligibility is not selected to Cost Summary
  - Replaced the number of episodes with readmissions with the number of readmissions in PAC Readmissions
  - Added risk scores to cost summary and chronic conditions dashboards
  - Changed SNF readmissions to calculate off admits rather than days
  
### v1.5.2
  - Anonymized demos have real beneficiary statuses as of v1.5 of NYHealth/prm-shared-data-warehouse so now we need to only show Aged Non-Dual benchmarks on demos in this solution

### v1.5.1
  - Fixed the scroll bar functionality in cost model and inpatient discharge tables
  - Removed mem_report_hier_1 and prv_hier_1_align_timeline from default member dimensions in benchmark creation 
  - Added clearing for pac_index_drg between dashboards
  
### v1.5.0
  - Renamed the Product title from "Premier ACO Intelligence Platform" to "Premier ACO Intelligence Solution"
  - Added a new Sub Cost table called "Detailed Cost table". This gives a more detailed table by choosing a service category
  - Pull in ref_prm_line from the shared data warehouse to ACOI dashboard 

### v1.4.0
  - Changed ACO Insight QVW naming to use `NYHealth/aco-insight-dashboard` version instead of `PRM/analytics-pipeline` version. This relies on tooling available in `PRM/analytics-pipeline` v7.2.0
  - Enable ability to turn on the enhanced tabs. This will flow in through the `metaproject`.
  - Created a konami countdown to allow developers to quick toggle variables on and off.
  - Bring CCW-based condition information into the ACO Insight interface
  - Provide ability to toggle to "And" or "Or" selections on Conditions where "And" selections are for patients who have all of the conditions selected and "Or" selections are for patients who have at least one of the conditions selected
  - Added a list box for `mem_report_hier_1` and added the variable to the konami code view for enabling 
  - Added Waste Calculator fields to `services` table and `ref_waste_calculator`
  - Add a macro variable for enabling Prv Hier Align in the qlikview interface
  - Add list box for `PrvHier1AlignTimeline` and allow to toggle this on and off in the Set Variables view (using Konami code). 
  - Add waste calculator table to the Waste Calculator tab
  - Added `prv_hier_1_align_timeline` as a dimension of the HCG and DRG benchmarks
  - Provide the option to clear conditions when viewing benchmarks otherwise benchmarks would not be shown as the data would no longer make sense
  - Add a drill down table in the Waste Calculator tab to view the data by provider when a single Waste Measure is selected
  - Added a short `time.sleep()` before ExportToSqlite to ensure sqlite export is run on the driver machine
  - Added in a member table and summary statistics for Conditions dashboard
  - Add in Leakage reports
  - replaced DRG family with individual DRGs
  - updated % of total PBPY formula in waste calculator to be total expeditures, stopped waste services from persisting across dashboards and in summary statistics 
  - Added member name to cost distribution member table
  - Made refinements to conditions dashboard - updated titles and added addl member table columns
  - Made member table in conditions summary read only
  - Reordered PBPY and PPPY columns in conditions summary and added in avg age to both conditions and cost dist summary mem tables
  - stopped selections in first leakage tab to persist in second tab
  - fixed percent of total formula in waste calculator
  - Fixed an issue where member groups without any eligibility would cause null values in the benchmarks table

### v1.3.2
  - renamed cost summary table to remove reference to trend graph

### v1.3.1
  - renamed PAC summary table to remove reference to member table 

### v1.3.0
  - updated grand total formula in risk score column in the high level summary dashboard to match grand total risk score in comparator report
  - replaced the 6 bar charts in demographics with a singluar PBPY distribution chart
  - updated 'service' from 'mapping' labels in service line detail
  - added in a yearly option for trend summary
  - Add variable into QlikView for case when Premier is not the end client to dynamically change title to ACO Insight
  - Enable dynamic metavariables to control enabling of enhanced features and Premier whitelabelling
  - Updated sort order of filter box to be Y then N
  - Added Help text to SNF and HH detail dashboards
  - switched the member month and paid columns in cost distribution summary member table
  - Added trend graph to cost distribution summary
  - increased widths of filter boxes and created separate variable for PAC filter boxes because they are on a different basis than the poteintially avoidable ones
  - set null values to 'n/a' in EOL metrics
  - summary statistics were cut off for larger clients, increased widths
  - Enabled member-level drill down on PAC Summary dashboard
  - Updated column names for % of total population to include (Filtered Population) to be consistent 
  - changed util adjustments summary table so total population would reflect the currently assigned selection
  - changed wording in last column of service line detail filter chart to be consistent across tabs
  - Added in % of total population column to DRG Detail and reformatted
  - Fixed denominator in summary statistics factors (1.0x and 100%) to include currently assigned = 'Y' 
  - updated summary statistics PBPY so it does not change when a DRG Family is selected
  - added in additional % of PBPY column in PAC Summary and updated formats
  - set null values to 'n/a' in EOL metrics
  - added in % of total pop PBPY columns to ACSA, ED, and PSA dashboards
  - Added revenue code to `services` table for lines which are displayed in the `Service Line Detail` dashboard. Also added a reference table for merging on descriptions
  - removed blank util types from service line detail
  - updated PAC target formula so if home health is lower leave as is - if home health is higher bring down to benchmark
  - removed unknown eligibility category
  - renamed paid pbpy per pac episode to PAC paid pbby
  - removed blank columns for formatting in tables
  - changed service table so that rev codes are listed where there was a null hcpcs. 
  - added in % of filtered population column and resized columns and dividers
  - updated formulas in util adjustment summary so that % of reduction for each service line added up to the grand total
  - added in 'filtered population' to column headers and added new % of total population column in service line detail

  - renamed '% of paid PBPY' to '% of Paid PBPY (Filtered Population)' in cost model & inc header height to 4
  - added in color coding for the service line names in the cost model based on DoHM
  - added in % of total pop calc for cost summary and added 'filtered' to the label of the existing column for transparency and consistency
  - added back in risk scores to summary statistics
  - changed the potentially avoidable filter boxes to reflect actual visits and not visits per 1000
  - fixed % of total calculation to exclude filters from both the costs and the member counts
  - fixed formats to be compatible across browsers
  - update images to be used on PRM for accessing reports
  - Changed PQI 11's name to Community-Acquired Pneumonia
  - right align text in EOL summary
  - fixed spelling error in cost model - population
  - stopped the grouped service category filter in cost summary from persisting across tabs
  - restructured summary statistics box
  - removed the PAC member detail table in PAC summary and updated % reduction formula to add to the total % reduction
  - changed population comparison in summary statistics to assigned during selected period rather than currently assigned 
  - removed all references to total population and renamed columns with 'filtered population' to exclude that wording
  - Removed the Cost Summary Trend Graph


### v1.2.6

  - Flipped the expected `procs` column type to `integer` to match HCG grouper and PRM/analytics-pipeline v7.1.0 format 


### v1.2.5
  - Enable ability to show apostrophe in `vname_client` field 

### v1.2.4
  - Use `memmos_medical` from the `prm-shared-data-warehouse` calculation, rather than duplicating the logic here
  - Added back in photo gif into repository for PRM display
  - Make the ACO Insight QVW loading process stop hanging on errors
  - fixed util adj formulas in bottom table to reconcile to summary table numbers (made formulas additive) and fixed dohm target calc
  - nulled out urgent care wm benchmark and dohm in the cost model, nulled out dohm and utils for benefits glasses/contacts

### v1.2.3
  - made risk score title and value invisible in summary statistics boc (upper right corner) by whiting the text
  - blanked columns referencing PBPY benchmarks in 'Cost Model' tab
  - hid all columns referencing benchmark PBPYs in the 'Cost Summary' tab and removed the button "push to show benchmarks"

### v1.2.2
  - fixed adjustment in paid pbpy calc for the utilization adjustments summary 
  - On the PAC summary dashboard relabeled "Reduction as a % of paid PBPY" as "Reduction in Paid PBPY - Actual vs. Target

### v1.2.1
  - fixed % never admitted to hospice EOL calc by removing '-1' from expression 

### v1.2.0

  - Ensure the default time period has ~3 months of runout
  - Add a warning when a time period with only a few months of runout is selected
  - Correct truncation of Client Name in PAC sheet (was only wrong on last sheet)
  - Remove out-dated PDF version of the user guide (left outdated Markdown version as a hope and a prayer)

### v1.1.0

 - Secondary release of product component
    - want to make it more clear as to what the difference between the last 2 columns in the summary table in Utilization Adjusments. Needed to differentiate the total population with the filtered population. Had to resize some objects due to the name change
    - Updated Readmission titles to include 30 days
    - Updated the ED, PSA , and ASCA Detail Tables with PBPY Percentages 
- Updated Readmission titles to include 30 days
- Added the words "Per Episode" to paid PBPY(target) in PAC Summary
- want to make it more clear as to what the difference between the last 2 columns in the summary table in Utilization Adjusments. Needed to differentiate the total population with the filtered population. Had to resize some objects due to the name change
- made all headers a light blue shade to differentiate
- Added a description of DoHM on the locations when DoHM is listed within several tables within the Utilization and Cost by Service Category Dashboards
- Updated the Cost Summary Detail Table with "Opportunity PBPY/total(average paid PBPY)"
- allow decimal inputs into util adjustment box (like 1.5%)
- added [$(lbl_cost_range_rolling)] and [$(lbl_age_gender_bucket)]to risk score set analysis to stop those selections from affecting the risk score
- added in line separators in DRG Detail and Cost Model tables
- renamed mcrm_line_hier_1 and reporting_service_detail
- removed service line detail dropdown and replaced with table so that current selections would work better
- renamed mcrm_line_hier_1 and reporting_service_detail
- updated 'FOP' to 'OF' abbreviations for outpatient facility in util adj input box and move home health to under snf in the list
- fixed spacing in trend summary between charts and legends
- put back missing util adj input box and changed shading color. Fixed some header shading in the DRG tab
- updated summary statistics risk score calc to be similar to that in the high level summary data table
- updated tab name of Cost Summary Detail to just 'Cost Summary'
- added formatting changing and additional avg service cost column to service line detail tab. Also added a clear option to fields selected in this dashboard so that selections do not carry over
- added average length of stay in hidden DRG table and updated hover comments
- exclude mcrm lines from the set analysis in IP Discharge to make totals tie to other PAC tabs
- added back in DRG fields to psa expressions to tie to comparator report
- added in dividers to tabs with headers
- added in clear filters for certain tabs where we do not want filters to leave the tab. Also made some tables readonly
- fixed some labels via Kate feedback
- added clear filters functionality to PAC tabs - needed to use different syntax than in the other dashboards
- updated dimension expression in Key Metrics PSA summary to tie to the combination of columns in PSA Detail tab
- added in hover text for disabled tabs to say that its available with the expanded reports
- updated label in IP Discharge and increased the widths of the filter boxes in PAC 
- updated readme formatting and added in premier logo
- made shade color a brighter blue and changed to a variable to make easier for updates in the future
-fixed sorting to be by descending pbpy in service line detail tab 
fixed unselected colors in service mapping filter by removing background formula from dimension
- Stop suppression of null CCN's in QlikView and instead bucket them into a value of "Facility Unknown"
- Add a date-time stamp (on hover of Milliman logo) so that it easy to see when the report was generated. (This is especially helpful for publishing purposes)
- Add image used on prm.milliman.com to the repository
- Fixed an issue where release notes would not automatically post to the tag upon promotion
- Want to make formats conisitent across the interface. In PAC DRG Detail the top left filter table showed PBPY with 2 decimals. We removed the decimals from the format
- fixed expression in service line mapping filter table (missing 'total' in denominator)
- removed hcg_setting filter from set analysis in service line utilization formulas and forced utils to procedures for drugs
- - removed (timeline) from beneficiary status filter

### v1.0.0

  - Initial release of product component
    - Created python library and metadata structure
    - Defined target `DataMart` specifications
    - Created pipeline to generate the `DataMart`:
      - Pulls relevant data from `prm-shared-data-warehouse`
      - Summarizes `prm-shared-data-warehouse` detail datamart into reporting-ready format
      - Assembles benchmark information for cost model, DRG experience, and post-acute care DRG experience
      - Generates SQLite database
    - Created initial version of the Qlikview report:
      - Designed to replace the existing Tableau report
      - Contains four major dashboard views, `Summary Dashboards`, `Utilization and Cost by Service Category Dashboards`, `Potentially Avoidable Services Dashboards` and `Post-Acute Care Dashboards`
      - Setup to allow member filtering based on current, timeline, or rolling dimensions
      - Can be generated programmatically through PRM Qlikview trigger framework
      - Also accepts data from PRM anonymization process
    - Created Luigi pipeline definitions to assist with pipeline execution
      - Can currently be implemented by placing `shim_aco_insight.py` in the `01_Programs` folder with a "post_" prefix
    - Added scripts to assist with reconciliation of SQLite database and Qlikview expressions
    - Added script to assist with promoting new releases



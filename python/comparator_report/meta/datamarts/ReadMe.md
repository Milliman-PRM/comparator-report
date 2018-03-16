### OBJECTIVE:
 * Store all datamart template definintions for datamarts used by the pipeline

### DEVELOPER NOTES:
  * The template files should be CSV files with headers.
    * All the CSV files in this module should render nicely on GitHub.
    * When directly editing them with Excel, beware of implicit data conversions upon open. 

Each data mart definition should be in its own sub-directory with the following files:

| File | Contents |
| :--- | :------- |
| `Readme.md` | Brief description of data mart and each table therein. |
| `_Fields.csv` | One record per unique field present in the data mart; additional fields in this file will contain metadata such as data types, labels and null constraints. |
| `_Tables.csv` | One record per field per table; additional fields in this file will contain metadata such as local and global uniqueness indicators. |

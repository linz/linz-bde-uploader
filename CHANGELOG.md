# Change Log

All notable changes for the LINZ BDE Uploader are documented in this file.

## [2.5.1] - 2019-05-08
### Fixed
- Race condition in automated test
- Full upload error runnin `pg_sleep` (#194)

## [2.5.0] - 2019-01-10
### Changed
- `linz_bd_uploader` now exits with a success code when no datasets
  are available for upload (#153)
### Fixed
- Unpredictable exit code (use of uninitialized $exitcode variable)
- Swapped insert/delete counts in upload stats (#165)
### Enhanced
- Improve documentation of `bde_TablesAffected` (#173)
- Add stdout support in `linz-bde-schema-load` (#175)
- Add support for `table_version` 1.6.0 (#180)

## [2.4.0] - 2017-12-11
### Changed
- LOL stopping updates of `crs_map_grid`
- LOL stopping updates of `crs_statist_area`
- LOL 3.14 remove meshblock and electoral place tables and unloads
### Added
- linz-bde-uploader-schema-load script (#123)
- Expose git revision in functions description (#111)
- Add a testsuite
- Add `pg_error_level` configuration support
- BdeDatabase::streamDataToTempTable (#133)
### Enhanced
- Allow running `bde_ApplyLevel0Update` without having SUPERUSER
  privileges (ie: do not require access to `pg_authid`)
- Avoid creation of temporary BDE file copy when possible (#135)
- Review IMMUTABLE/STABLE/VOLATILE status of each function
- Improve documentation (#57, #85, #89, #118)
- Improve user feedback (#97, #106)
- Provide defaults for most configuration items (#78, #103)
### Fixed
- Ensure install base prefix paths are correctly set
- Avoid duplicated error messages (#59, #69, #75)
### Removed
- BdeDatabase::UploadDataToTempTable function removed,
  use BdeDatabase::streamDataToTempTable instead

## [2.3.0] - 2016-12-22
### Added
- Support changes for the Landonline release 3.15

## [2.2.0] - 2016-09-13
### Added
- Support changes for the Landonline release 3.14

### Fixed
- Explicitly set the temp file permissions to have global read rights

## [2.1.1] - 2016-08-31
### Fixed
- zombied job option and added better logging for this option

## [2.1.0] - 2016-08-31
### Added
- Added option to delete zombied jobs (-remove-zombie)

### Fixed
- Remove the need for the level 5 process to gain an exclusive lock for the
  table to be updated

## [2.0.3] - 2016-08-15
### Added
- Fixes #34. Renaming of `pg_stat_activity` procpid to pid (following Postgresql 9.2 change)
- Fixes #30 - missed regex extended flag

## [2.0.2] - 2016-06-01
### Added
- Added support for overriding log level with -level-log CLI option

### Fixed
-  Made error exception handling more robust

## [2.0.1] - 2016-05-31
### Fixed
- No changes

## 2.0.0 - 2016-05-16
### Added
- New `bde_control.bde_version()` function
### Changed
- Packaging changes to account for dependency changes
- Move dbpatch and table version source code from project to external projects
- Move BDE schema files to external project
- Move LDS schema file to external project
- Move polygon grid functions to external project
- Removed debian postinst script. The install and configuration is now managed externally
- Moved version Build.PL and added SQL version number function

## [1.5.8] - 2016-04-13
### Changed
- Improve logging of event hooks.

## [1.5.7] - 2016-04-01
### Fixed
- Fix missing required package in build script

### Added
  * Added pending parcels import to LDS schema
  * Removed unnecessary pending parcels layers

## [1.5.6] - 2015-11-04
### Fixed
- fix bde primary key fix that was not picked up in the migration

### Changed
- Move readme to markdown version. Updated a few install notes
- Updating lds.geodetic_network_marks layer to fix CHN, CVN, and NHN networks

## [1.5.5] - 2015-09-15
### Fixed
- Ensure bad parcel statuses can not flow through to product generation

## [1.5.4] - 2015-09-10
### Added
  - Added debian packaging for Trusty
  - Added event hooks functionality. Also reworked error handling to clean-up legacy exception handling put in place for Win32 under perl 5.8

### Fixed
  - Include Try::Tiny in Debian packaging

### Changed
 - Changing owners field from VARCHAR to TEXT to prevent data overflows
 - Improve patch from cbd1015 to use table version API
 - Improve event handling if config does not define the events

## [1.5.3] - 2015-05-21
### Changed
 - made changes for LOL 3.11 release

### Fixed
 - fixed bug for LOL release 3.11 where function in bde_functions.sql referenced a column that wouldnt be created until patches.sql was run - variable type is now hardcoded to workaround this

## [1.5.2] - 2015-04-13
### Fixed
- Fix for electoral layer creation when duplicate SUFIs are found

## [1.5.1] - 2015-04-09
### Fixed
 - Revert debian control to standard config

## [1.5.0] - 2015-04-09
### Changed
 - Added more information about process holding table locks

### Added
 - Added offshore Island support to simplified parcels layers
 - Added support for UTF-8 support from Landonline data
 - Increase field width of locality_utf8 for the road_centre_line, road_centre_line_subsection, street_address2 tables

## [1.4.4] - 2014-11-10
### Added
- Added check for unique sufi records in electoral tables

## [1.4.3] - 2014-10-21
### Added
- Added crs_image_history to config
- Added support for PostgreSQL/PostGIS extensions

### Fixed
- Cleaned up debian post install functions to correct report errors.
- Fix early drop of temp table during LDS layer generation

## [1.4.2]  - 2014-10-17
### Changed
- Increase field width for street address and road layers

## [1.4.1]  - 2014-10-13
### Changed

-  Update lds_layer_functions.sql to fix typo in meshblock layer check

## [1.4.0]  - 2014-10-09
### Added
- Support for full landonline tables that require a filter
- Added sufi (INT) to street_address
- Add crs_image_history table

### Changed
- Refactored title exclusion and protection code
- Clean-up usage of temp tables, remove some dead code
- Make parcel polygons OGC valid

## [1.3.7] - 2014-06-17
### Changed
- Updated table config to switch to new road name and street address data files

## [1.3.6] - 2014-06-16
### Added
- Added support for adding columns to versioned tables
- Add street addressing columns for Landonline 3.10

### Changed
- Temporarily turn off electoral layer updates until TA script is in place

## [1.3.5] - 2014-05-16
### Changed
- Ensure patch checks that table is versioned
- Ensure logging package is imported into BdeUploaderDataDef class
- Ensure that training tiles are removed from the simple title memorial tables
- update name_and_date column from varchar(100) to varchar(200) on tables bde.crs_statute and table_version.bde_crs_statute_revision

## [1.3.4] - 2013-09-18
### Fixed
 - Ensure SQL RAISE messages have the correct number of parameters

## [1.3.3] - 2013-09-06
### Fixed
- Fix performance and syntax errors for title memorial creation sql

## [1.3.2] - 2013-09-18
### Added
- Add title memorials dataset

## [1.3.1] - 2013-06-23
### Changed
- Update observation layers to contain the start and end mark name
- Improve observation query performance. Make note about data quality

## [1.3.0] - 2013-06-21
### Fixed
- Ensure that control characters within BDE file that are not printable in XML are not carried through to PostgreSQL
- Updated BDE processor to support bad control character and to fix current problems in the database

## [1.2.9] - 2013-05-16
### Fixed
- Ensure upload process stops when bde_copy error is reported

## [1.2.8] - 2013-05-15
### Fixed
- Fixed bug relating to 286468ae09 where schema prefix was not being explicitly used in patch

## [1.2.7] - 2013-05-10
### Changed
- Rebuild BDE table primary keys using versioned table column key
- remove debugging messages

## [1.2.6] - 2013-05-03
### Changed
-  Remove dropping of connections by default

### Added
- Add new street address layer schema for NZPost

## [1.2.5] - 2013-01-09
### Fixed
- Fixed table version diff bug [issue #19]

### Changed
- Included first unit tests for table version functions using pgTAP

## [1.2.4] - 2012-12-19
### Added
- Add status column to title estate and owner tables

### Fixed
- Apply patch to upgrade table version functions. Relates to 85ee4a219a [issue #10]
- Improved patch from 85ee4a2 to ensure existing functions with modified signatures are cleaned up

## [1.2.3] - 2012-12-14
### Fixed
- Fix bug in observation patch [issue #9]

## [1.2.2] - 2012-12-13
### Changed
- Set new All Linear Parcels calc area to NULL

## [1.2.1] - 2012-12-12
### Changed
- Fixed spatial table patch
- Fixed Observation table change patch
- Fix error in title table SQL generation

## [1.2.0] - 2012-12-10
### Added
- Support for LDS simplified aspatial tables
- Include all parcel parcels layer. This contains parcels that are not pending. i.e Current, Approved, Historic and Survey Historic

### Fixed
- Fix observation shape vertex order to be the same as observation direction

## [1.1.6] - 2012-11-01
### Changed
- Changed permissions for table versioning functio ns

## [1.1.5] - 2012-10-31
### Changed

- Upgrade to fix user permission problems identified with issue #8

## [1.1.4] - 2012-10-30
### Changed

- fix extra 'inserts' in diff output, from rows that were created and then deleted during the diff window.

## [1.1.3] - 2012-09-25
### Fixed
- Fix for duplication Geodetic code error message  (again)

## [1.1.2] - 2012-09-18
### Fixed
- Fix for duplication Geodetic code error message

## [1.1.1] - 2012-03-20
### Fixed
- Fixed bug with creation of LDS schema.

## [1.1.0] - 2012-03-13
### Added
- Added 3 new layers. WACA and SPI adjustments and Mesh Blocks
- Added rna_id to street_address layer
- Added accuracy values observation related layers

### Fixed
- Fixes to generation of Road centreline datasets

### Changed
- Defined all fields width to better support FGBD exports on LDS
- Improved the performance of the survey observation differencing
- Improved logging to allow for 99 rotated files
- Rounded calc_area to whole number in parcels tables

## [1.0.2] - 2011-07-14
### Added
- Added new logging system based on log4perl. This removes all logging to db.

## [1.0.1] - 2011-06-22
### Added
- Added logging to email output

### Fixed
- Fixed run_bde_upload logging
- Fixed issue with logging before database connection is made

### Changed
- Refactor handling of incremental data application.

## [1.0.0] - 2011-05-06
### Added
- Initial release.


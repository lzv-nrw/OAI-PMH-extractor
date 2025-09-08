# Changelog

## [3.5.0] - 2025-09-08

### Added

- added support for `dcm-common` v4

## [3.4.0] - 2025-07-25

### Added

- added optional `_max_resumption_tokens` argument to `list_identifiers_exhaustive` and `list_identifiers_exhaustive_multiple_sets` methods of `RepositoryInterface` class
- added method `list_identifiers_exhaustive_multiple_sets` to support multiple sets
- added `filter_by_regex_with_xpath_query` in `TransferUrlFilters` class
- added `transfer_url_filters` keyword argument in `PayloadCollector` class

### Fixed

- improved logging-behavior of `PayloadCollector`

## [3.0.0] - 2024-09-05

### Changed

- **Breaking:** migrated to dcm-common (version 3) (`2f194a87`)

## [2.0.0] - 2024-04-25

### Changed

- refactored for a simplified usage of RepositoryInterface and PayloadCollector (`ea27d18d`, `0af03e82`)
- **Breaking:** switched to new implementation of `lzvnrw_supplements.Logger` (`3b871f54`)
- **Breaking:** renamed `report` attributes to `log` and exposed them (`77a65c03`, `0fddee96`)

### Added

- added optional timeout-argument to `RepositoryInterface` and `PayloadCollector` (`ec3876af`, `f1f1e58e`)
- added `log` property in `PayloadCollector` (`0fddee96`)
- added method `list_identifiers_exhaustive` which automatically consumes resumption tokens (`7d290584`)
- added py.typed marker to package (`684d405a`)

### Fixed

- fixed problematic implementation in `PayloadCollector`'s `download_file` when automatically generating filename (`c8f9044b`)
- fixed extraction of urls from xml path (`eb23a9f4`, `ebcd2033`)

## [1.0.0] - 2023-11-27

### Changed
- switch to `requests`-library for HTTP-requests (`RepositoryInterface`) and `urllib` for downloading files (`PayloadCollector`); handle corresponding errors in `ExtractionManager` (`57318e5c`, `d4c6f79f`, `fa2906a4`, `ed1c048d`)
- handle OAI-PMH-errors during harvest in `ExtractionManager` (`76873035`)
- **Breaking:** make more use of functionalities from `lzvnrw-supplements` (`b4037f93`, `357e4917`, `143be98a`)

### Added
- added optional filename-override argument for `PayloadCollector.download_file` (`fa2906a4`)
- handle OAI-PMH-errors in `RepositoryInterface` (`6d5883d0`, `357e4917`)

### Fixed
- make use of resumptionToken during ListSets-request in `RepositoryInterface` (`0ce45080`, `92b7b2fb`, `e19ebcae`)

## [0.3.8] - 2023-10-26

### Changed

- make lzvnrw-dependencies less restrictive (`3865f9db`)

## [0.3.7] - 2023-10-24

### Changed

- OAIPMHRecords now have the additional properties identifier_hash and path  (`f305fdb2`)
- directories intended for record payload now consist of hashes (`f305fdb2`)

### Fixed

- fix oai-identifiers as directory-names making OSErrors possible (`f305fdb2`)

## [0.3.6] - 2023-10-23

### Changed

- use pathlib's Path.write_bytes to write requests-Response.content to file (`370a3712`)

### Fixed

- fix use of resumptionToken in RepositoryInterface.list_identifiers (`39e8443e`, `5af81afa`)
- update metadata for File-objects during extraction-job (`88e8018f`, `8f315292`)

## [0.3.2] - 2023-10-20

### Fixed

- fix errors when trying to extract filename from content header (`f678a2f6`)
- fix encoding-issue when download certain file types (`c6576e03`)

## [0.3.0] - 2023-10-19

### Changed

- **Breaking:** initial release of the refactored extraction library

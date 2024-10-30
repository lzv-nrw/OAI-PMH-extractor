## Setup

Install this package and its dependencies by issuing `pip install .` .

## General Package Structure

This package consists of three major components, i.e. the classes
`RepositoryInterface`, `PayloadCollector`, and `ExtractionManager`.
Additionally, there are two minor helper or record-classes `OAIPMHRecord`
and `Job` defined. Their relationship can be sketched by the following
diagram:
```
               ┌───────────────────┐                 ┌───────────────┐
           ┌───┤ ExtractionManager ├───┐             │ OAIPMHRecord  │
           │   └───────────────────┘   │             └───────────────┘
 controls  │                           │  controls   ┌───────────────┐
           │                           │             │ Job           │
           │                           │             └───────────────┘
┌──────────▼──────────┐        ┌───────▼──────────┐
│ RepositoryInterface │        │ PayloadCollector │
└─────────────────────┘        └──────────────────┘
```

The `ExtractionManager` is used by an external application to perform
harvest-/extraction-jobs. In order to do this, the `ExtractionManager` uses
an `RepositoryInterface`-object (and optionally, if a payload extraction is
needed, a `PayloadCollector`-object) as provided at instantiation. Note, that
the two subordinate classes can be used independently, e.g. a
`RepositoryInterface`-object can be used to determine available metadata-prefixes
for a given repository.

The two remaining classes, `OAIPMHRecord` and `Job`, are less integral to the
functionality of this package. The `OAIPMHRecord` is used to store and exchange
information on individual records of a repository between the main components.
Whereas the `Job`-objects are used to collect information regarding a single job
and as a minimal means to enable customizable io (for an external application).
This is achieved by making the harvest/extraction-progress associated with a
job accessible from the `ExtractionManager`'s parent scope.

### Details on the class ExtractionManager
The `ExtractionManager` is initialized with a `RepositoryInterface`- and,
if needed, a `PayloadCollector`-object. Afterwards, the method `harvest(..)` can
be used to initiate a harvest-job. It returns a `Job`-identifier which can be
used to get a reference to the associated `Job` (with `get_job(identifier)`)
or cancel a running job (with `abort_job(identifier)`). After this command is
issued, the task is executed asynchronously [1].

Similarly, the `extract(..)`-method can be used to first perform a harvest
followed by the download of the corresponding payload.

[1] For the threaded execution of jobs, the threading-library is used.
  Although the Job-class has settings like pause and resume, the threaded
  execution is independent of this class, i.e. the Job-object and
  `Thread`-object are controlled independently. As of now, a job can only be
  started and aborted.

Usage example:
```
>>> from oai_pmh_extractor import ExtractionManager, PayloadCollector, RepositoryInterface, TransferUrlFilters
>>> interface=RepositoryInterface("https://repositorium.uni-muenster.de/oai/miami")
>>> collector=PayloadCollector(TransferUrlFilters.filter_by_regex("<dc:identifier>(https:\/\/repositorium.uni-muenster.de\/transfer\/.*)</dc:identifier>"))
>>> manager=ExtractionManager(interface, collector)
>>> from pathlib import Path
>>> manager.extract(path=Path("test_oai_pmh_extractor/file_storage"), metadata_prefix="oai_dc", _from="2024-02-10", _until="2024-03-10")
'<jobid>'
>>> print(manager.get_job('<jobid>').log)
INFO
 * [12:55:59.532591] Job <jobid> created.
 * [12:55:59.532721] Start Job.
   ...
```

### Details on the class RepositoryInterface
The `RepositoryInterface` can be viewed as a means of communication with an
OAIPMH-interface. All but one `verb` of the OAI-PMH-protocol are supported.
The existing `verb` of `ListRecords` has been replaced by a `ListIdentifiers`-
request followed by a series of `GetRecord`-requests. An instance of an interface
requires the base url for the OAI-harvest.

### Details on the class PayloadCollector
The `PayloadCollector` provides the functionality to download files that are
associated with an `OAIPMHRecord` and store those files in the local file
system. To this end, the initialization requires a suitable (i.e. repository-
specific) transfer url filter function. The class `TransferUrlFilters` contains a
small collection of pre-defined filter function factories.

### Details on the class OAIPMHRecord
The `OAIPMHRecord` is a record-class storing information regarding a single
OAIPMH-item (`identifier`, `identifier_hash`, `path`, `status`, `complete`,
`metadata_raw`, `files`/file_urls). Here, the `complete`-property is used as
an indicator whether the metadata (harvest) or the metadata and payload
(extraction) are loaded.

### Details on the class Job
The `Job` is a record-class storing information regarding a single OAIPMH-job
(harvest and/or extract).

In order to generate additional metadata for a job, the methods `start()`,
`pause()`, `resume()`, and `end()` can be used. Available properties are:
`identifier`, `complete`, `records` (list of `OAIPMHRecords`), `description`,
`running`, `creation_datetime`, `start_datetime`, `complete_datetime`, and
`omitted_records` (list of `OAIPMHRecords`). The boolean properties of
`complete` (i.e. all records are loaded) and `running` and their associated
datetime-metadata-properties are set by the aforementioned methods. Records
are intended to be added/moved using the methods `add_record(..)`,
`add_omitted_record(..)`, or `omit_record(..)`, respectively.

Note also the comment regarding the methods `start()`, `pause()`, .. made
[above](#details-on-the-class-extractionmanager).

A `Job` has an `dcm-common`-type `Logger`-object `log` with keys for
information, summary, warnings, and errors.

Summary:
* control job-state and generate metadata:
    `start()`, `pause()`, `resume()`, and `end()`
* available properties:
    `identifier`, `complete`, `records`, `description`, `running`,
    `creation_datetime`, `start_datetime`, `complete_datetime`, and
    `omitted_records`, `log`
* add records with: `add_record(record)`, `add_omitted_record(record)`
* omit already listed records with: `omit_record(record)`

# Contributors
* Sven Haubold
* Orestis Kazasidis
* Stephan Lenartz
* Kayhan Ogan
* Michael Rahier
* Steffen Richters-Finger
* Malte Windrath

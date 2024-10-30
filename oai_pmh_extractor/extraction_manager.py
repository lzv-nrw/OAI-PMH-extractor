"""
The OAI-PMH-extractor is structured as sketched in this diagram:

               ┌───────────────────┐                 ┌───────────────┐
           ┌───┤ ExtractionManager ├───┐             │ OAIPMHRecord  │
           │   └───────────────────┘   │             └───────────────┘
 controls  │                           │  controls   ┌───────────────┐
           │                           │             │ Job           │
           │                           │             └───────────────┘
┌──────────▼──────────┐        ┌───────▼──────────┐
│ RepositoryInterface │        │ PayloadCollector │
└─────────────────────┘        └──────────────────┘

The ExtractionManager can be viewed as the controller of an OAI-PMH-
harvest and -extraction process. Upon being requested (via harvest or
extract) a new Job-object is created and a reference is returned. Then,
this job is processed asynchronously [1].
The ExtractionManager operates using two sub-components:
An object of a RepositoryInterface, which serves as a means to communicate
with a repository-server, and an object of a PayloadCollector, which
allows to handle file download. The latter utilizes a function defined
at instantiation to extract transfer-urls from the harvested metadata.
Furthermore, the container-class OAIPMHRecord is defined. Objects of this
class are used to exchange information between modules.


[1] For the threaded execution of jobs, the threading-library is used.
  Although the Job-class supports settings like pause, the threaded
  execution is independent of this class, i.e. the Job-object and
  Thread-object are controlled independently.
"""

import os
from typing import Optional, Callable, TextIO
from pathlib import Path
import threading
from urllib import request

import requests
from dcm_common import LoggingContext as Context, Logger

from oai_pmh_extractor.oaipmh_record import OAIPMHRecord
from oai_pmh_extractor.repository_interface import RepositoryInterface
from oai_pmh_extractor.payload_collector import PayloadCollector
from oai_pmh_extractor.job import Job


class ExtractionManager():
    """
    Controller for the OAI-PMH- harvest and -extraction processes.

    Keyword arguments:
    repository_interface -- object of a RepositoryInterface
    payload_collector -- object of a PayloadCollector; required for
                         extraction-jobs
                         (default None)
    """

    def __init__(
        self,
        repository_interface: RepositoryInterface,
        payload_collector: Optional[PayloadCollector] = None
    ) -> None:
        self._repository_interface = repository_interface
        self._payload_collector = payload_collector
        self._jobs: dict[str, Job] = {}
        self._running_threads_lock = threading.Lock()
        self._running_threads: dict[str, tuple[threading.Thread, Callable[[], None]]] = {}
        self.log: Logger = Logger(default_origin="OAI Extraction Manager")

    def get_job(self, identifier: str) -> Optional[Job]:
        """
        Returns the job for a given job-identifier or None if not
        existent.

        Keyword arguments:
        identifier -- job identifier
        """

        if identifier in self._jobs:
            return self._jobs[identifier]
        return None

    def abort_job(self, identifier: str) -> None:
        """
        Abort the job with a given job-identifier.

        Keyword arguments:
        identifier -- job identifier
        """

        if identifier in self._running_threads:
            self.log.log(
                Context.INFO,
                body=f"Aborted Job {identifier}."
            )
            self._running_threads[identifier][1]()
            del self._running_threads[identifier]

    def _generate_unique_job_identifier(self):
        """Generate a unique job identifier."""
        for retries in range(0, 100):
            new_id = Job.generate_identifier(seed=str(retries))
            if new_id not in self._jobs:
                return new_id
        # raise error on failure
        self.log.log(
            Context.ERROR,
            body="Unable to generate unique job identifier."
        )
        raise IndexError("Unable to generate unique job identifier.")

    def _dispatch_job(
        self,
        job: Job,
        job_task: Optional[Callable] = None
    ) -> tuple[threading.Thread, Callable[[], None]]:
        """
        Generate threading.Thread with given job-function.

        Returns the Thread and a callable-abort function (which also sets
        the state of the associated job).
        """

        # register job
        self._jobs[job.identifier] = job

        # setup abort-event
        abort = threading.Event()
        # define abort-callback
        def abort_job():
            abort.set()

        # instantiate worker thread
        worker = threading.Thread(target=job_task, daemon=True, args=(abort,))
        self._running_threads[job.identifier] = worker, abort_job
        worker.start()
        self.log.log(
            Context.INFO,
            body=f"Started Job {job.identifier}."
        )
        return worker, abort_job

    def harvest(
        self,
        metadata_prefix: str,
        _identifiers: Optional[list[str]] = None,
        _from: Optional[str] = None,
        _until: Optional[str] = None,
        _set_spec: Optional[str] = None,
        _filter: Callable[[OAIPMHRecord], bool] = lambda record: True,
        _progress_callback: Callable[[Job], None] = lambda job: None,
        _post_harvest_callback: Callable[[Job, threading.Event], None] \
            = lambda job, event: None,
        _final_callback: Callable[[Job, threading.Event], None] \
            = lambda job, event: None,
        _verbose_file: TextIO = open(os.devnull, "w")
    ) -> str:
        """
        Uses the RepositoryInterface to perform a metadata-harvest.

        Returns a reference to a Job-object. If _identifiers is given,
        the options for a selective harvest are ignored.

        Keyword arguments:
        metadata_prefix -- required argument for oai-pmh; only identifiers
                           are listed that can satisfy the given format
        _identifiers -- list of specific record-identifiers for harvest
                        (default None)
        _from -- lower datestamp in daterange for selective harvest
                 (default None)
        _until -- upper datestamp in daterange for selective harvest
                  (default None)
        _set_spec -- colon-separated list of path in set hierarchy
                     (default None)
        _filter -- filter function discerning whether record is returned;
                   if False for some record, this record is only listed
                   in Job's omitted_records
                   (default lambda identifier: True)
        _progress_callback -- callback for progress indication
                              (default lambda job: None)
        _post_harvest_callback -- callback that is executed after harvest
                                  is complete but before the job is ended
                                  (default lambda job, event: None)
        _final_callback -- callback for completed job
                           (default lambda job, event: None)
        _verbose_file -- file for verbose output
                         (default open(os.devnull, "w"))
        """

        self.log.log(
            Context.INFO,
            body="Setting up new harvest Job.."
        )

        # set default _progress_callback and _final_callback
        if _progress_callback is None:
            _progress_callback = lambda job, event: None
        if _final_callback is None:
            _final_callback = lambda job, event: None

        # generate job-id
        job_id = self._generate_unique_job_identifier()

        # instantiate Job
        harvest_job = Job(
            identifier=job_id
        )
        harvest_job.description=f"[{harvest_job.creation_datetime}] harvest job"

        # build task for this job
        def job_task(abort_event: threading.Event) -> None:
            short_job_id = harvest_job.get_abbreviated_identifier()
            print(f"[{short_job_id}] Starting harvest..", file=_verbose_file)
            harvest_job.start()
            resumption_token = None
            _progress_callback(harvest_job)
            # get identifiers
            if _identifiers is None:
                while True:
                    # make request
                    http_ok = True
                    try:
                        response_list_of_identifiers, resumption_token = \
                            self._repository_interface.list_identifiers(
                                _metadata_prefix=metadata_prefix,
                                _from=_from,
                                _until=_until,
                                _set_spec=_set_spec,
                                _resumption_token=resumption_token
                            )
                    except requests.RequestException as exc_info:
                        # prepare error msg for log in case of httperror
                        http_ok = False
                        error_msg = str(exc_info)
                    else:
                        # check for and prepare error msg in case of
                        # oai-error
                        # (ListIdentifiers-verb yielding an OAIPMHerror
                        # implies the occurrence of a badResumptionToken)
                        oai_ok = resumption_token is None \
                            or len(response_list_of_identifiers) > 0
                        error_msg = \
                            str(self._repository_interface.log).replace("\n", " ")
                    # handle exception
                    if not http_ok or not oai_ok:
                        harvest_job.end(abort=True)
                        print(
                            f"[{short_job_id}] A problem occurred while trying" \
                                + f" to execute ListIdentifiers: '{error_msg}'",
                            file=_verbose_file
                        )
                        print(f"[{short_job_id}] Aborted job.", file=_verbose_file)
                        return
                    # process result
                    # add dummy records into list of records in job (for progress)
                    for identifier in response_list_of_identifiers:
                        harvest_job.add_record(OAIPMHRecord(identifier))
                    _progress_callback(harvest_job)
                    # exit loop if no token is returned; id-harvest complete
                    if resumption_token is None:
                        break
                    print(
                        f"[{short_job_id}] " \
                            + f"Got {len(response_list_of_identifiers)} identifiers," \
                            + f" continuing with token {resumption_token}.",
                        file=_verbose_file
                    )
                    # exit-point: abort if requested
                    if abort_event.is_set():
                        harvest_job.end(abort=True)
                        print(f"[{short_job_id}] Aborted job.", file=_verbose_file)
                        return
                # check for log of RepositoryInterface
                ri_log = \
                    str(self._repository_interface.log).replace("\n", " ")
                if ri_log != "":
                    print(
                        f"[{short_job_id}] Repository reported a problem:" \
                            + f" '{ri_log}'",
                        file=_verbose_file
                    )
            else:
                # add dummy records into list of records in job (for progress)
                for identifier in _identifiers:
                    harvest_job.add_record(OAIPMHRecord(identifier))
                _progress_callback(harvest_job)
                print(
                    f"[{short_job_id}] " \
                        + "Using given list of identifiers..",
                    file=_verbose_file
                )
            msg = f"Job is associated with {len(harvest_job.records)} identifier(s)."
            harvest_job.log.log(
                Context.INFO,
                body=msg
            )
            print(
                f"[{short_job_id}] " \
                    + f"Total number of identifiers: {len(harvest_job.records)}",
                file=_verbose_file
            )
            print(f"[{short_job_id}] Collecting metadata..", file=_verbose_file)
            # get records
            for record in harvest_job.records:
                # make request
                http_ok = True
                full_record = None
                try:
                    full_record = self._repository_interface.get_record(
                        metadata_prefix=metadata_prefix,
                        identifier=record.identifier
                    )
                except requests.RequestException as exc_info:
                    # prepare error msg for log in case of httperror
                    http_ok = False
                    error_msg = str(exc_info)
                if full_record is not None:
                    # process result by copying data into dummy record
                    record.status = full_record.status
                    record.metadata_raw = full_record.metadata_raw
                    record.metadata_prefix = full_record.metadata_prefix
                    # mark as complete
                    record.complete = True
                    if not _filter(record):
                        print(
                            f"[{short_job_id}] " \
                                + f"Omit record {record.identifier} due to filter.",
                            file=_verbose_file
                        )
                        harvest_job.omit_record(record, "Filter")
                    else:
                        harvest_job.log.log(
                            Context.INFO,
                            body=f"Record {record.identifier} marked complete."
                        )
                        print(
                            f"[{short_job_id}] " \
                                + f"Collected metadata for record {record.identifier}.",
                            file=_verbose_file
                        )
                else:
                    record.complete = False
                    if not http_ok:
                        harvest_job.log.log(
                            Context.ERROR,
                            body="A problem occurred while trying to execute"
                            + f" GetRecord for {record.identifier}: '{error_msg}'"
                        )
                        print(
                            f"[{short_job_id}] " \
                                + "A problem occurred while trying to execute" \
                                + f" GetRecord for {record.identifier}: '{error_msg}'",
                            file=_verbose_file
                        )
                    else:
                        harvest_job.log.log(
                            Context.ERROR,
                            body=f"GetRecord for {record.identifier} returned error. \n"
                                + str(self._repository_interface.log)
                        )
                        print(
                            f"[{short_job_id}] " \
                                + f"GetRecord for {record.identifier} returned error. \n" \
                                + str(self._repository_interface.log),
                            file=_verbose_file
                        )
                _progress_callback(harvest_job)
                # exit-point: abort if requested
                if abort_event.is_set():
                    harvest_job.end(abort=True)
                    print(f"[{short_job_id}] Aborted job.", file=_verbose_file)
                    return
            harvest_job.log.log(
                Context.INFO,
                body="Harvest of metadata complete."
            )
            _post_harvest_callback(harvest_job, abort_event)
            if abort_event.is_set():
                harvest_job.end(abort=True)
                print(f"[{short_job_id}] Aborted job.", file=_verbose_file)
                return
            # job is finished
            harvest_job.end()
            self.log.log(
                Context.INFO,
                body=f"Completed Job {harvest_job.identifier}."
            )
            _progress_callback(harvest_job)
            _final_callback(harvest_job, abort_event)
            if abort_event.is_set():
                harvest_job.end(abort=True)
                print(f"[{short_job_id}] Aborted job.", file=_verbose_file)
                return
            # remove job reference from list of running threads
            with self._running_threads_lock:
                del self._running_threads[harvest_job.identifier]
            print(f"[{short_job_id}] Done.", file=_verbose_file)

        self._dispatch_job(harvest_job, job_task=job_task)

        return job_id

    def extract(
        self,
        path: Path,
        metadata_prefix: str,
        _identifiers: Optional[list[str]] = None,
        _from: Optional[str] = None,
        _until: Optional[str] = None,
        _set_spec: Optional[str] = None,
        _filter: Callable[[OAIPMHRecord], bool] = lambda record: True,
        _progress_callback: Callable[[Job], None] = lambda job: None,
        _final_callback: Callable[[Job, threading.Event], None] \
            = lambda job, event: None,
        _verbose_file: TextIO = open(os.devnull, "w")
    ) -> str:
        """
        Uses the RepositoryInterface to perform a metadata-harvest
        followed by a download of the associated payload using the
        PayloadCollector.

        Returns a reference to a Job-object. If _identifiers is given,
        the options for a selective harvest are ignored.

        The data is stored in a subdirectory of the given path. In path
        a new directory named after the job identifier and the data for
        individual records is put into subfolders named after the record-
        identifier:
            path/
            ├── job-identifier/
            │   ├── record-identifier/
            │   ├── record-identifier/
            │   └── ...
            ├── job-identifier/
            │   ├── record-identifier/
            │   └── ...
            └── ...

        Keyword arguments:
        path -- path to directory in local filesystem
        metadata_prefix -- required argument for oai-pmh; only identifiers
                           are listed that can satisfy the given format
        _identifiers -- list of specific record-identifiers for harvest
                        (default None)
        _from -- lower datestamp in daterange for selective harvest
                 (default None)
        _until -- upper datestamp in daterange for selective harvest
                  (default None)
        _set_spec -- colon-separated list of path in set hierarchy
                     (default None)
        _filter -- filter function discerning whether record is returned
                   (default lambda identifier: True)
        _progress_callback -- callback for progress indication
                              (default lambda job: None)
        _final_callback -- callback for completed job
                           (default lambda job: None)
        _verbose_file -- file for verbose output
                         (default open(os.devnull, "w"))
        """

        self.log.log(
            Context.INFO,
            body="Setting up new extraction Job.."
        )

        # exit if no PayloadCollector registered during instantiation
        if self._payload_collector is None:
            msg = "No PayloadCollector available, cannot execute " \
                + "requested extraction."
            self.log.log(
                Context.ERROR,
                body=msg
            )
            raise ValueError(msg)

        # define extraction as _post_harvest_callback
        def extract_job(
            extraction_job: Job,
            abort_event: threading.Event
        ) -> None:
            short_job_id = extraction_job.get_abbreviated_identifier()
            # iterate records to get transfer urls
            found_anything = False
            print(f"[{short_job_id}] Extracting payload..", file=_verbose_file)
            for record in extraction_job.records:
                # mypy - hint
                assert self._payload_collector is not None
                # collect urls
                self._payload_collector.download_record_payload(
                    record=record,
                    renew_urls=True,
                    skip_download=True
                )
                if record.files:
                    found_anything = True
                extraction_job.log.log(
                    Context.INFO,
                    body=f"Registered {len(record.files)} file(s) for record "
                        + f"{record.identifier}."
                )
                print(
                    f"[{short_job_id}] Record {record.identifier} has " \
                        + f"{len(record.files)} associated "\
                        + f"file{'s'[:len(record.files)^1]}.",
                    file=_verbose_file
                )
                _progress_callback(extraction_job)
                if abort_event.is_set():
                    extraction_job.end(abort=True)
                    print(f"[{short_job_id}] Aborted job.", file=_verbose_file)
                    return
            extraction_job.log.log(
                Context.INFO,
                body="Collected all transfer-urls."
            )
            _progress_callback(extraction_job)

            # prepare directories
            if found_anything:
                (path / extraction_job.identifier).mkdir(parents=True)

                # iterate records to download files
                for record in extraction_job.records:
                    if isinstance(record.files, list):
                        record.path = None
                        seed = 0
                        while record.path is None or record.path.is_dir():
                            record.path = \
                                path \
                                    / extraction_job.identifier \
                                    / (f"{record.identifier_hash}-" \
                                        + f"{Job.generate_identifier(str(seed))[0:9]}")
                            seed += 1
                        if len(record.files) > 0:
                            record.path.mkdir(parents=True, exist_ok=False)
                        for file in record.files:
                            print(
                                f"[{short_job_id}] Downloading file {file['url']}",
                                file=_verbose_file
                            )
                            # mypy - hint
                            assert self._payload_collector is not None
                            # perform download
                            file_ok = True
                            error_msg = ""
                            try:
                                file["path"] = self._payload_collector.download_file(
                                    record.path, file["url"]
                                )
                            except (request.URLError, OSError, FileExistsError) \
                                    as exc_info:
                                # prepare error msg for log in case of urlerror
                                file_ok = False
                                error_msg = str(exc_info)
                            if file_ok:
                                file["complete"] = file["path"].is_file()
                                if file["complete"]:
                                    extraction_job.log.log(
                                        Context.INFO,
                                        body=f"Downloaded file {file['url']} associated "
                                            + f"with record {record.identifier}."
                                    )
                                else:
                                    extraction_job.log.log(
                                        Context.INFO,
                                        body="A problem occurred while getting file "
                                            + f"{file['url']} associated with"
                                            + f" record {record.identifier}."
                                    )
                            else:
                                file["complete"] = False
                                extraction_job.log.log(
                                    Context.INFO,
                                    body=f"Download failed: {error_msg}."
                                )
                                print(
                                    f"[{short_job_id}] " \
                                        + f"Failed to download {file['url']} associated " \
                                        + f"with record {record.identifier}: " \
                                        + error_msg,
                                    file=_verbose_file
                                )
                        _progress_callback(extraction_job)
                    if abort_event.is_set():
                        extraction_job.end(abort=True)
                        print(f"[{short_job_id}] Aborted job.", file=_verbose_file)
                        return
            extraction_job.log.log(
                Context.INFO,
                body="Extraction complete."
            )
            print(f"[{short_job_id}] Extraction complete.", file=_verbose_file)
            _progress_callback(extraction_job)
            return

        thread_id = self.harvest(
            metadata_prefix=metadata_prefix,
            _identifiers=_identifiers,
            _from=_from,
            _until=_until,
            _set_spec=_set_spec,
            _filter=_filter,
            _progress_callback=_progress_callback,
            _post_harvest_callback=extract_job,
            _final_callback=_final_callback,
            _verbose_file=_verbose_file
        )

        return thread_id

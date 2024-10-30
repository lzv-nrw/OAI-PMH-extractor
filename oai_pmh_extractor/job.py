"""
This module contains the definition of a record-class storing information
regarding a single OAIPMH-job (harvest, extract, ..).

Summary:
* control job-state and generate metadata:
    start(), pause(), resume(), and end()
* available properties:
    identifier, complete, records, description, running, creation_datetime,
    start_datetime, complete_datetime, and omitted_records
* add records with: add_record(record), add_omitted_record(record)
* omit already listed records with: omit_record(record)

In order to generate additional metadata, the methods start(), pause(),
resume(), and end() can be used. Available properties are: identifier,
complete, records (list of OAIPMHRecords), description, running,
creation_datetime, start_datetime, complete_datetime, and omitted_records
(list of OAIPMHRecords). The boolean properties of complete (i.e. all
records are loaded) and running and their associated datetime-metadata-
properties are set by the afore-mentioned methods. Records are intended
to be added/moved using the methods add_record(..), add_omitted_record(..),
or omit_record(..), respectively.
"""

from typing import Optional
import sys
from datetime import datetime

from dcm_common import LoggingContext as Context, Logger

from oai_pmh_extractor.oaipmh_record import OAIPMHRecord


class Job():
    """
    Record-class storing information regarding a harvest Job.

    Keyword arguments:
    identifier -- identifier for Job; can be generated from class'
                  staticmethod `generate_job_identifier`
    description -- job description
                   (default "")
    """

    _DATETIME_FORMAT = "%Y-%m-%d %H:%M:%S"

    JOB_TAG = "OAI JOB"

    def __init__(self, identifier: str, description: str = "") -> None:
        self._identifier = identifier
        self._complete = False
        self._records: list[OAIPMHRecord] = []

        # displayed description
        self._description = description
        # more job metadata
        self._running = False
        self._paused = False
        self._creation_datetime = datetime.utcnow().strftime(self._DATETIME_FORMAT)
        self._start_datetime = "not started"
        self._complete_datetime = "not completed"
        # track records that are filtered out during harvest
        self._omitted_records: list[OAIPMHRecord] = []

        self._log: Logger = Logger(default_origin=self.JOB_TAG)
        msg = f"Job {self._identifier} created."
        self._log.log(
            Context.INFO,
            body=msg
        )

    @property
    def identifier(self) -> str:
        """Job identifier property."""
        return self._identifier

    def get_abbreviated_identifier(self) -> str:
        """Returns first 6 letters of Job identifier."""
        return self._identifier[:6]

    @property
    def complete(self) -> bool:
        """Job complete property."""
        return self._complete

    def end(self, abort: bool = False) -> None:
        """Set Job to completed-state."""

        self._complete_datetime = datetime.utcnow().strftime(self._DATETIME_FORMAT)
        self._running = False
        self._complete = not abort
        msg = "Job ended. " + ("(Reason: Abort)" if abort else "(Reason: Done)")
        self._log.log(
            Context.INFO,
            body=msg
        )

    @property
    def records(self) -> list[OAIPMHRecord]:
        """Job records property."""
        return self._records

    def add_record(self, record: OAIPMHRecord) -> bool:
        """
        Add record to job.

        Keyword arguments:
        record -- OAIPMHRecord-object to be added to the job
        """

        # check whether record id already exists
        for _record in self._records:
            if _record.identifier == record.identifier:
                msg = f"Tried to add existing record ({record.identifier})."
                self._log.log(
                    Context.ERROR,
                    body=msg
                )
                print(
                    f"Job {self._identifier}: " + msg,
                    file=sys.stderr
                )
                return False
        self._log.log(
            Context.INFO,
            body=f"Add record {record.identifier}."
        )
        self._records.append(record)
        return True

    @property
    def description(self) -> str:
        """Job description property."""
        return self._description

    @description.setter
    def description(self, value: str) -> None:
        """Setter for job description property."""
        self._description = value

    @property
    def running(self) -> bool:
        """Job running property."""
        return self._running

    def start(self) -> None:
        """Set Job to running-state."""

        if not self._running and not self._paused:
            self._start_datetime = datetime.utcnow().strftime(self._DATETIME_FORMAT)
            self._running = True
            self._complete = False
            self._log.log(
                Context.INFO,
                body="Start Job."
            )
        else:
            self._log.log(
                Context.ERROR,
                body="Attempt at starting Job while already in running state."
            )
            print(f"Job {self._identifier} has already been started:" \
                + "Cannot start. (Use resume() instead.)", file=sys.stderr)

    def pause(self) -> None:
        """Set Job to paused-state."""

        if self._running:
            self._paused = True
            self._running = False
            self._complete = False
            self._log.log(
                Context.INFO,
                body="Paused Job."
            )
        else:
            self._log.log(
                Context.ERROR,
                body="Attempt at pausing Job which has not been started."
            )
            print(f"Job {self._identifier} has not been started:" \
                + "Cannot pause.", file=sys.stderr)

    def resume(self) -> None:
        """Reset Job to running-state."""

        if self._paused:
            self._paused = False
            self._running = True
            self._complete = False
            self._log.log(
                Context.INFO,
                body="Resumed Job."
            )
        else:
            self._log.log(
                Context.ERROR,
                body="Attempt at resuming Job which has not been paused."
            )
            print(f"Job {self._identifier} has not been started:" \
                + "Cannot resume. (Use start() instead.)", file=sys.stderr)

    @property
    def creation_datetime(self) -> str:
        """Job creation_datetime property."""
        return self._creation_datetime

    @property
    def start_datetime(self) -> Optional[str]:
        """Job start_datetime property."""
        return self._start_datetime

    @property
    def complete_datetime(self) -> Optional[str]:
        """Job complete_datetime property."""
        return self._complete_datetime

    @property
    def omitted_records(self) -> list[OAIPMHRecord]:
        """Job omitted_records property."""
        return self._omitted_records

    def add_omitted_record(
        self,
        record: OAIPMHRecord,
        reason: Optional[str] = None
    ) -> bool:
        """
        Add omitted record to job.

        Keyword arguments:
        record -- OAIPMHRecord-object to be added to the job
        reason -- reason for omitting record to be written into log
                  (default None)
        """

        # check whether record id already exists
        for _record in self._omitted_records:
            if _record.identifier == record.identifier:
                msg = f"Tried to omit existing record ({record.identifier})."
                self._log.log(
                    Context.ERROR,
                    body=msg
                )
                print(
                    f"Job {self._identifier}: " + msg,
                    file=sys.stderr
                )
                return False
        self._omitted_records.append(record)
        self._log.log(
            Context.INFO,
            body=f"Omit record {record.identifier}."
                + f" (Reason: {reason})" if reason is not None else ""
        )
        return True

    def omit_record(
        self,
        record: OAIPMHRecord,
        reason: Optional[str] = None
    ) -> bool:
        """
        Move record from records to omitted_records.

        Keyword arguments:
        record -- OAIPMHRecord-object to be moved
        reason -- reason for omitting record to be written into log
                  (default None)
        """

        # look for target record to be moved
        target_record = None
        for _record in self._records:
            if _record.identifier == record.identifier:
                target_record = record
                break
        if target_record is not None:
            # rebuild records-list
            self._records = [
                r for r in self._records
                    if r.identifier != target_record.identifier
            ]
            # append record to omitted-records-list
            self._omitted_records.append(target_record)
            self._log.log(
                Context.INFO,
                body=f"Omit record {target_record.identifier}."
                    + f" (Reason: {reason})" if reason is not None else ""
            )
            return True

        msg = f"Tried to omit untracked record ({record.identifier})."
        self._log.log(
            Context.ERROR,
            body=msg
        )
        print(
            f"Job {self._identifier}: " \
                + msg,
            file=sys.stderr
        )
        return False

    @property
    def log(self) -> Logger:
        """Job log property."""
        return self._log

    @staticmethod
    def generate_identifier(seed: str) -> str:
        """Generate (unique) identifier for Job-object."""
        from hashlib import sha256

        string = datetime.utcnow().strftime(seed + "%Y-%m-%d %H:%M:%S.%f")
        return sha256(
            string.encode(encoding="utf-8")
        ).hexdigest()

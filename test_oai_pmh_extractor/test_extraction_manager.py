"""Test module for the class ExtractionManager."""

import os
import sys
import io
import time
import threading
import shutil
from urllib import request
from unittest import mock

from pathlib import Path
import pytest
import requests

from oai_pmh_extractor \
    import ExtractionManager, RepositoryInterface, OAIPMHRecord

@pytest.fixture(name="fake_collector")
def fake_collector():
    return mock.Mock()

@pytest.fixture(name="fake_interface")
def fake_interface():
    return mock.Mock()

@pytest.fixture(name="simple_manager")
def simple_manager(fake_interface, fake_collector):
    return ExtractionManager(fake_interface, fake_collector)

def test_harvest_latency(simple_manager):
    """
    Test for harvest-method in ExtractionManager.

    Incorporate fake latency via time.sleep.
    """

    response_identifiers = ["id0", "id1"]
    response_token = None
    response_identifiers_delay = 0.3
    response_records_delay = 0.1

    # fake requests taking time to execute via side-effect
    def faked_list_identifiers(**kwargs):
        time.sleep(response_identifiers_delay)
        return (response_identifiers, response_token)
    def faked_get_record(metadata_prefix, identifier):
        time.sleep(response_records_delay)
        return OAIPMHRecord(identifier)

    with mock.patch.object(
        simple_manager._repository_interface,
        "list_identifiers",
        side_effect=faked_list_identifiers
    ), mock.patch.object(
        simple_manager._repository_interface,
        "get_record",
        side_effect=faked_get_record
    ):
        # for better io
        print("")

        # start harvest-job
        jobid = simple_manager.harvest(
            "oai_dc",
            _progress_callback=lambda job: print(len(job.records), end="\r")
        )

        # wait for job to terminate
        job = simple_manager.get_job(jobid)
        max_duration = 2 * (response_identifiers_delay \
            + response_records_delay * len(response_identifiers))
        i = 0
        while not job.complete:
            time.sleep(0.01)
            print(".", end="", flush=True)
            i += 1
            if i > max_duration/0.01:
                break

        # make assertions
        assert len(job.records) == len(response_identifiers)
        for record in job.records:
            assert isinstance(record, OAIPMHRecord)
            assert record.identifier in response_identifiers

        simple_manager._repository_interface.list_identifiers.assert_called_once()
        assert simple_manager._repository_interface.get_record.call_count == \
            len(response_identifiers)

        # for better io
        print("")
        # print(simple_manager.log)

def test_harvest_identifiers(simple_manager):
    """
    Test for harvest-method in ExtractionManager.

    Use _identifiers-argument to select records.
    """

    requested_identifiers = ["id0", "id1"]

    # fake requests taking time to execute via side-effect
    def faked_get_record(metadata_prefix, identifier):
        return OAIPMHRecord(identifier)

    with mock.patch.object(
        simple_manager._repository_interface,
        "list_identifiers"
    ), mock.patch.object(
        simple_manager._repository_interface,
        "get_record",
        side_effect=faked_get_record
    ):
        # start harvest-job
        jobid = simple_manager.harvest(
            "oai_dc",
            _identifiers=requested_identifiers
        )

        # wait for job to terminate
        job = simple_manager.get_job(jobid)
        max_duration = 0.5
        i = 0
        while not job.complete:
            time.sleep(0.01)
            i += 1
            if i > max_duration/0.01:
                break

        # make assertions
        assert len(job.records) == len(requested_identifiers)
        for record in job.records:
            assert isinstance(record, OAIPMHRecord)
            assert record.identifier in requested_identifiers

        assert simple_manager._repository_interface.list_identifiers.call_count == 0
        assert simple_manager._repository_interface.get_record.call_count == \
            len(requested_identifiers)
        # print(simple_manager.log)


def test_harvest_identifiers_with_error(simple_manager):
    """
    Test for harvest-method in ExtractionManager if GetRecord fails.

    Use _identifiers-argument to select records.
    """

    requested_identifiers = ["id0", "id1"]
    failing_identifier = requested_identifiers[0]

    # fake requests taking time to execute via side-effect
    def faked_get_record(metadata_prefix, identifier):
        if identifier == failing_identifier:
            return None
        return OAIPMHRecord(identifier)

    with mock.patch.object(
        simple_manager._repository_interface,
        "get_record",
        side_effect=faked_get_record
    ):
        # start harvest-job
        jobid = simple_manager.harvest(
            "oai_dc",
            _identifiers=requested_identifiers,
            _verbose_file=sys.stdout
        )

        # wait for job to terminate
        job = simple_manager.get_job(jobid)
        max_duration = 0.5
        i = 0
        while not job.complete:
            time.sleep(0.01)
            i += 1
            if i > max_duration/0.01:
                break

        # make assertions
        assert len(job.records) == len(requested_identifiers)
        for record in job.records:
            assert isinstance(record, OAIPMHRecord)
            assert record.identifier in requested_identifiers
            if record.identifier == failing_identifier:
                assert not record.complete
            else:
                assert record.complete

def test_harvest_abort(simple_manager):
    """
    Test for aborting harvest-method in ExtractionManager.

    Incorporate fake latency via time.sleep.
    """

    response_identifiers = ["id0", "id1"]
    response_identifiers_delay = 0.3
    response_token = None
    response_records_delay = 0.1

    # fake requests taking time to execute via side-effect
    def faked_list_identifiers(**kwargs):
        time.sleep(response_identifiers_delay)
        return (response_identifiers, response_token)
    def faked_get_record(metadata_prefix, identifier):
        time.sleep(response_records_delay)
        return OAIPMHRecord(identifier)

    with mock.patch.object(
        simple_manager._repository_interface,
        "list_identifiers",
        side_effect=faked_list_identifiers
    ), mock.patch.object(
        simple_manager._repository_interface,
        "get_record",
        side_effect=faked_get_record
    ):
        # start harvest-job
        jobid = simple_manager.harvest(
            "oai_dc"
        )

        time.sleep(0.25 * response_identifiers_delay)

        # check if thread has started
        assert len(threading.enumerate()) == 2

        # get reference to thread and abort
        thread = simple_manager._running_threads[jobid][0]
        simple_manager.abort_job(jobid)

        # wait for job to terminate
        max_duration = 2 * (response_identifiers_delay \
            + response_records_delay * len(response_identifiers))
        i = 0
        while thread.is_alive():
            time.sleep(0.01)
            i += 1
            if i > max_duration/0.01:
                break

        # thread is not running any more
        assert len(threading.enumerate()) == 1
        # not listed in active threads
        assert len(simple_manager._running_threads) == 0
        # job has not been completed
        assert not simple_manager.get_job(jobid).complete

        # print(f"==== {jobid} ====")
        # print(simple_manager.get_job(jobid).log)
        # print(simple_manager.log)

def test_harvest_simultaneous_jobs(simple_manager):
    """
    Test for harvest-method in ExtractionManager.

    Incorporate fake latency via time.sleep.
    """

    a_response_identifiers = ["id0", "id1", "id2", "id3"]
    a_response_records_delay = 0.1
    b_response_identifiers = ["id4", "id5"]
    b_response_records_delay = 0.02

    # fake requests taking time to execute via side-effect
    def faked_get_record(metadata_prefix, identifier):
        if identifier in a_response_identifiers:
            time.sleep(a_response_records_delay)
        else:
            time.sleep(b_response_records_delay)
        return OAIPMHRecord(identifier)

    with mock.patch.object(
        simple_manager._repository_interface,
        "list_identifiers"
    ), mock.patch.object(
        simple_manager._repository_interface,
        "get_record",
        side_effect=faked_get_record
    ):
        # start first harvest-job
        a_jobid = simple_manager.harvest(
            "oai_dc",
            _identifiers=a_response_identifiers
        )

        time.sleep(a_response_records_delay)

        # the first job should still be running
        assert len(threading.enumerate()) == 2

        # start second harvest-job
        b_jobid = simple_manager.harvest(
            "oai_dc",
            _identifiers=b_response_identifiers
        )

        assert len(threading.enumerate()) == 3

        # wait until jobs are finished
        a_job = simple_manager.get_job(a_jobid)
        b_job = simple_manager.get_job(b_jobid)
        max_duration = \
            a_response_records_delay * len(a_response_identifiers) * 2 \
            + b_response_records_delay * len(b_response_identifiers) * 2
        i = 0
        while not (a_job.complete and b_job.complete):
            time.sleep(0.01)
            i += 1
            if i > max_duration/0.01:
                break

        # assert results
        for record in a_job.records:
            assert record.identifier in a_response_identifiers
        for record in a_response_identifiers:
            assert record in [r.identifier for r in a_job.records]

        for record in b_job.records:
            assert record.identifier in b_response_identifiers
        for record in b_response_identifiers:
            assert record in [r.identifier for r in b_job.records]

        # print(a_job.log)
        # print(b_job.log)
        # print(simple_manager.log)

@pytest.mark.parametrize(
    ("io_file", "check_log"),
    [
        (os.devnull, False),
        (Path("tmp/log.txt"), True)
    ],
    ids=["without_io", "with_io"]
)
def test_extract(simple_manager, io_file, check_log):
    """
    Test extract-method in ExtractionManager.

    Fake PayloadCollector behavior but still write to local file-system.
    """

    test_url = "https://www.uni-muenster.de/imperia/md/images/allgemein/farbunabhaengig/unims.svg"
    test_file_name = "test.txt"

    response_identifiers = ["id0", "id1"]
    response_token = None

    # fake requests taking time to execute via side-effect
    def faked_list_identifiers(**kwargs):
        return (response_identifiers, response_token)
    def faked_get_record(metadata_prefix, identifier):
        return OAIPMHRecord(identifier)
    def faked_download_record_payload(record, **kwargs):
        record.register_files_by_url([test_url])

        # raise AssertionError if faked implementation unsuited
        if "skip_download" not in kwargs or \
                ("skip_download" in kwargs and not kwargs["skip_download"]):
            raise AssertionError("Test unsuited for implementation.")
    def faked_download_file(path, url):
        (path / test_file_name).write_text(url)
        return path / test_file_name

    with mock.patch.object(
        simple_manager._repository_interface,
        "list_identifiers",
        side_effect=faked_list_identifiers
    ), mock.patch.object(
        simple_manager._repository_interface,
        "get_record",
        side_effect=faked_get_record
    ), mock.patch.object(
        simple_manager._repository_interface.log,
        "__str__",
        side_effect=lambda: ""
    ), mock.patch.object(
        simple_manager._payload_collector,
        "download_record_payload",
        side_effect=faked_download_record_payload
    ), mock.patch.object(
        simple_manager._payload_collector,
        "download_file",
        side_effect=faked_download_file
    ):
        path = Path("tmp")
        path.mkdir(exist_ok=True)

        with open(io_file, "w", encoding="utf-8") as _io_file:
            # start first harvest-job
            jobid = simple_manager.extract(
                path=path,
                metadata_prefix="oai_dc",
                _verbose_file=_io_file
            )
            job = simple_manager.get_job(jobid)
            max_duration = 0.5
            i = 0
            while not job.complete:
                time.sleep(0.01)
                i += 1
                if i > max_duration/0.01:
                    break

            # check briefly for completed job content
            assert len(job.records) == len(response_identifiers)
            for record in job.records:
                assert record.identifier in response_identifiers
                assert record.path is not None
                assert record.path.is_dir()
                assert (record.path / test_file_name).is_file()
                payload_content = \
                    (record.path / test_file_name).read_text(encoding="utf-8")
                assert payload_content == test_url

            # check for use of PayloadCollector
            assert simple_manager._payload_collector.download_record_payload.call_count == 2

        if check_log:
            log = io_file.read_text(encoding="utf-8")
            print("")
            print(log)
            log_lines = log.split("\n")
            assert len(log_lines) == 13

        # print(job.log)
        # print(simple_manager.log)

        # clean up temporary files
        shutil.rmtree(path)

def test_extract_urlerror(simple_manager):
    """
    Test handling of URLerrors in extract-method of ExtractionManager.
    """

    test_url = "http://test"
    test_file_name = "test.txt"

    response_identifiers = ["id0", "id1"]
    error_msg = "URL error message"

    # fake requests taking time to execute via side-effect
    def faked_get_record(metadata_prefix, identifier):
        return OAIPMHRecord(identifier)
    def faked_download_record_payload(record, **kwargs):
        record.register_files_by_url([test_url + record.identifier])

        # raise AssertionError if faked implementation unsuited
        if "skip_download" not in kwargs or \
                ("skip_download" in kwargs and not kwargs["skip_download"]):
            raise AssertionError("Test unsuited for implementation.")
    def faked_download_file(path, url):
        if url == test_url + response_identifiers[0]:
            (path / test_file_name).write_bytes(b"test")
            return path / test_file_name
        raise request.URLError(error_msg)

    with mock.patch.object(
        simple_manager._repository_interface,
        "get_record",
        side_effect=faked_get_record
    ), mock.patch.object(
        simple_manager._repository_interface.log,
        "__str__",
        side_effect=lambda: ""
    ), mock.patch.object(
        simple_manager._payload_collector,
        "download_record_payload",
        side_effect=faked_download_record_payload
    ), mock.patch.object(
        simple_manager._payload_collector,
        "download_file",
        side_effect=faked_download_file
    ), io.StringIO() as log:
        path = Path("tmp")
        path.mkdir(exist_ok=True)

        # start first harvest-job
        jobid = simple_manager.extract(
            path=path,
            metadata_prefix="oai_dc",
            _identifiers=response_identifiers,
            _verbose_file=log
        )
        job = simple_manager.get_job(jobid)
        max_duration = 0.5
        i = 0
        while not job.complete:
            time.sleep(0.01)
            i += 1
            if i > max_duration/0.01:
                break

        #print("")
        #print(log.getvalue())

        # check briefly for completed job content
        assert len(job.records) == len(response_identifiers)
        for record in job.records:
            assert record.identifier in response_identifiers
            if record.identifier == response_identifiers[0]:
                for file in record.files:
                    assert file["complete"]
            else:
                for file in record.files:
                    assert not file["complete"]

        # clean up temporary files
        shutil.rmtree(path)


def test_harvest_listidentifiers_http_error(simple_manager, generate_FakeRequestsResponse):
    """
    Test for (http)-error-handling during ListIdentifiers in harvest-
    method of ExtractionManager.
    """

    expected_response = b""
    expected_error_code = 500
    expected_error_msg = "Internal Server Error"

    some_repository_interface = RepositoryInterface("fake_url")
    some_extraction_manager = ExtractionManager(some_repository_interface, None)

    with mock.patch.object(
                requests,
                "get",
                side_effect=lambda url, *args, **kwargs: generate_FakeRequestsResponse(
                    expected_response,
                    (expected_error_code, expected_error_msg)
                )
            ):
        with io.StringIO() as log:
            jobid = some_extraction_manager.harvest("oai_dc", _verbose_file=log)
            try: # the thread can already be done
                thread = simple_manager._running_threads[jobid][0]
                # wait for job to terminate
                max_duration = 0.1
                i = 0
                while thread.is_alive():
                    time.sleep(0.01)
                    i += 1
                    if i > max_duration/0.01:
                        break
            except KeyError:
                pass

            assert str(expected_error_code) in log.getvalue()
            assert expected_error_msg in log.getvalue()

def test_harvest_getrecord_http_error(simple_manager, generate_FakeRequestsResponse):
    """
    Test for (http)-error-handling during GetRecord in harvest-
    method of ExtractionManager.
    """

    expected_response = b""
    expected_error_code = 500
    expected_error_msg = "Internal Server Error"

    some_repository_interface = RepositoryInterface("fake_url")
    some_extraction_manager = ExtractionManager(some_repository_interface, None)

    with mock.patch.object(
                requests,
                "get",
                side_effect=lambda url, *args, **kwargs: generate_FakeRequestsResponse(
                    expected_response,
                    (expected_error_code, expected_error_msg)
                )
            ):
        with io.StringIO() as log:
            jobid = some_extraction_manager.harvest(
                "oai_dc",
                _identifiers=["id0"],
                _verbose_file=log
            )
            try: # the thread can already be done
                thread = simple_manager._running_threads[jobid][0]
                # wait for job to terminate
                max_duration = 0.1
                i = 0
                while thread.is_alive():
                    time.sleep(0.01)
                    i += 1
                    if i > max_duration/0.01:
                        break
            except KeyError:
                pass

            assert str(expected_error_code) in log.getvalue()
            assert expected_error_msg in log.getvalue()

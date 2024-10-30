"""Test module for the class Job."""

import pytest

from oai_pmh_extractor import OAIPMHRecord
from oai_pmh_extractor.job import Job

EMPTY_IDENTIFIER = Job.generate_identifier(seed="0")
SIMPLE_IDENTIFIER = Job.generate_identifier(seed="1")
SIMPLE_DESCRIPTION = "description"

@pytest.fixture(name="empty_job")
def empty_job():
    return Job(
        EMPTY_IDENTIFIER
    )

@pytest.fixture(name="simple_job")
def simple_job():
    return Job(
        SIMPLE_IDENTIFIER,
        description=SIMPLE_DESCRIPTION
    )

def test_generate_identifier():
    """Test for the class' Job-staticmethod generate_identifier."""

    id0 = Job.generate_identifier(seed="0")
    id1 = Job.generate_identifier(seed="1")

    assert isinstance(id0, str)
    assert id0 != id1
    assert len(id0) > 30

def test_instantiation_empty(empty_job):
    """Test default settings during instantiation."""

    assert empty_job.identifier == EMPTY_IDENTIFIER
    assert not empty_job.complete
    assert not empty_job.records
    assert empty_job.description == ""
    assert not empty_job.running
    assert isinstance(empty_job.creation_datetime, str)
    assert isinstance(empty_job.start_datetime, str)
    assert empty_job.start_datetime == "not started"
    assert isinstance(empty_job.complete_datetime, str)
    assert empty_job.complete_datetime == "not completed"
    assert not empty_job.omitted_records

def test_instantiation(simple_job):
    """Test pass through of data during instantiation."""

    assert simple_job.identifier == SIMPLE_IDENTIFIER
    assert simple_job.description == SIMPLE_DESCRIPTION

def test_getset_description(simple_job):
    """Test getter/setter method of description-property."""
    description = "another description"
    simple_job.description = description

    assert simple_job.description == description

def test_start(simple_job):
    """Test start-method."""

    simple_job.start()
    assert simple_job.running
    assert simple_job.start_datetime != "not started"

def test_pause_resume(simple_job):
    """Test pause-method."""

    simple_job.start()
    assert simple_job.running

    simple_job.pause()
    assert simple_job._paused
    assert not simple_job.running
    assert not simple_job.complete

    simple_job.resume()
    assert not simple_job._paused
    assert simple_job.running
    assert not simple_job.complete

    simple_job.pause()
    assert simple_job._paused
    assert not simple_job.running
    assert not simple_job.complete

def test_end(simple_job):
    """Test end-method."""

    simple_job.end()
    assert simple_job.complete
    assert simple_job.complete_datetime != "not completed"

def test_add_record(simple_job):
    """Test add_record-method of Job-object."""

    assert len(simple_job.records) == 0
    record = OAIPMHRecord("id0")
    simple_job.add_record(record)
    assert len(simple_job.records) == 1
    assert simple_job.records[0] == record

def test_add_omitted_record(simple_job):
    """Test add_omitted_record-method of Job-object."""

    assert len(simple_job.omitted_records) == 0
    record = OAIPMHRecord("id0")
    simple_job.add_omitted_record(record)
    assert len(simple_job.omitted_records) == 1
    assert simple_job.omitted_records[0] == record

def test_add_record_duplicates(simple_job):
    """Test add_record-method of Job-object."""

    # improve io
    print("")

    record = OAIPMHRecord("id0")
    record2 = OAIPMHRecord("id0")
    simple_job.add_record(record)
    assert len(simple_job.records) == 1
    simple_job.add_record(record2)
    assert len(simple_job.records) == 1

    simple_job.add_omitted_record(record)
    assert len(simple_job.omitted_records) == 1
    simple_job.add_omitted_record(record2)
    assert len(simple_job.omitted_records) == 1

def test_omit_record(simple_job):
    """Test omit_record-method of Job-object."""

    # setup non-trivial example
    record0 = OAIPMHRecord("id0")
    record1 = OAIPMHRecord("id1")
    record2 = OAIPMHRecord("id2")
    simple_job.add_record(record0)
    simple_job.add_record(record2)
    simple_job.add_omitted_record(record1)

    # move record
    simple_job.omit_record(record2)

    assert len(simple_job.records) == 1
    assert len(simple_job.omitted_records) == 2


def test_omit_record_iteration(simple_job):
    """Test omit_record-method of Job-object during iteration."""

    N = 10

    for i in range(N):
        simple_job.add_record(OAIPMHRecord("id"+str(i)))

    omit = True
    for record in simple_job.records:
        omit = not omit
        if omit:
            simple_job.omit_record(record)

    for record in simple_job.records:
        assert record not in simple_job.omitted_records
    for record in simple_job.omitted_records:
        assert record not in simple_job.records

"""Test module for the class OAIPMHRecord."""

from pathlib import Path

import pytest

from oai_pmh_extractor import OAIPMHRecord

# define test fixtures
EMPTY_IDENTIFIER = "id0"
SIMPLE_IDENTIFIER = "id1"
SIMPLE_STATUS = "test"
SIMPLE_METADATA_PREFIX = "oai_prefix"
SIMPLE_METADATA_RAW = "<key1>value1</key1><key2>value2</key2>"
SIMPLE_FILES = [
    {
        "identifier": "https://file1",
        "url": "https://file1",
        "path": None,
        "complete": False
    },
    {
        "identifier": "https://file2",
        "url": "https://file2",
        "path": None,
        "complete": False
    }
]
SIMPLE_FILE_URLS = [
    x["url"] for x in SIMPLE_FILES
]

@pytest.fixture(name="empty_record")
def empty_record():
    return OAIPMHRecord(
        EMPTY_IDENTIFIER
    )

@pytest.fixture(name="simple_record")
def simple_record():
    return OAIPMHRecord(
        SIMPLE_IDENTIFIER,
        status=SIMPLE_STATUS,
        metadata_prefix=SIMPLE_METADATA_PREFIX,
        metadata_raw=SIMPLE_METADATA_RAW,
        file_urls=SIMPLE_FILE_URLS
    )


def test_instantiation(simple_record):
    """Test pass through of data during instantiation."""

    assert simple_record.identifier == SIMPLE_IDENTIFIER
    assert simple_record.status == SIMPLE_STATUS
    assert simple_record.metadata_prefix == SIMPLE_METADATA_PREFIX
    assert simple_record.metadata_raw == SIMPLE_METADATA_RAW
    assert len(simple_record.files) == len(SIMPLE_FILES)
    for file in simple_record.files:
        assert len([
            x for x in simple_record.files
              if x["identifier"] == file["identifier"]]
        ) == 1

def test_instantiation_empty(empty_record):
    """Test pass through of data during instantiation."""

    assert empty_record.identifier == EMPTY_IDENTIFIER
    assert empty_record.status == ""
    assert empty_record.files == []

def test_set_identifier(simple_record):
    """Test setter function of identifier-property."""
    new_id = "id2"
    simple_record.identifier = new_id

    assert simple_record.identifier == new_id

def test_set_path(simple_record):
    """Test setter function of path-property."""

    assert simple_record.path is None
    new_path = Path("test")

    simple_record.path = new_path

    assert simple_record.path == new_path

def test_set_status(simple_record):
    """Test setter function of status-property."""
    new_status = "deleted"
    simple_record.status = new_status

    assert simple_record.status == new_status

def test_set_metadata_prefix(simple_record):
    """Test setter function of metadata_prefix-property."""
    new_prefix = "oai_prefix2"
    simple_record.metadata_prefix = new_prefix

    assert simple_record.metadata_prefix == new_prefix

def test_set_metadata_raw(simple_record):
    """Test setter function of metadata_raw-property."""
    new_raw = "<key3>value3</key3>"
    simple_record.metadata_raw = new_raw

    assert simple_record.metadata_raw == new_raw

def test_set_files(simple_record):
    """Test setter function of files-property."""
    new_files = [
        {
            "identifier": "https://file3",
            "url": "https://file3",
            "path": Path("relative/path/to/file/file3"),
            "complete": False
        }
    ]
    simple_record.files = new_files

    assert simple_record.files == new_files

def test_add_file(simple_record):
    """Test add_file of OAIPMHRecord-class."""
    new_file = {
        "identifier": "https://file3",
        "url": "https://file3",
        "path": Path("relative/path/to/file/file3"),
        "complete": False
    }

    assert new_file not in simple_record.files

    new_list = simple_record.add_file(new_file)

    assert new_list == simple_record.files
    assert new_file in simple_record.files

def test_remove_file(simple_record):
    """Test remove_file of OAIPMHRecord-class."""
    removed_file = SIMPLE_FILES[0]

    assert removed_file in simple_record.files

    new_list = simple_record.remove_file(removed_file)

    assert new_list == simple_record.files
    assert removed_file not in simple_record.files

def test_get_set_complete(simple_record):
    """Test getter and setter function of complete-property."""
    assert not simple_record.complete
    simple_record.complete = True
    assert simple_record.complete

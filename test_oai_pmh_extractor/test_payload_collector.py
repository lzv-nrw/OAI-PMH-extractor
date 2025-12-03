"""Test module for the class PayloadCollector."""

from pathlib import Path, PosixPath, WindowsPath
from time import sleep
import shutil
from urllib import request
from unittest import mock
from uuid import uuid4

import pytest
from flask import Response
from dcm_common import LoggingContext as Context

from oai_pmh_extractor import (
    PayloadCollector, TransferUrlFilters, OAIPMHRecord
)


@pytest.mark.parametrize(
    ("this_filter", "expected_result"),
    [
        (TransferUrlFilters.filter_by_regex("([a-z]+)"), ["abc", "d", "ef"]),
        (TransferUrlFilters.filter_by_regex("([0-9]+)"), ["123", "456"]),
        (TransferUrlFilters.filter_by_regex(".*"), ["123abc456d ef"]),
    ],
    ids=[
        "letters",
        "digits",
        "match anything"
    ]
)
def test_filter_by_regex(this_filter, expected_result):
    """Test factory `filter_by_regex` of `TransferUrlFilters`."""

    source_metadata = "123abc456d ef"

    assert expected_result == this_filter(source_metadata)


@pytest.mark.parametrize(
    ("source_metadata", "expected_result"),
    [
        ("<root><a>123abc123</a><a>123def123</a></root>", ["abc", "def"]),
        ("<root><a>123abc123def123</a></root>", ["abc", "def"]),
        ("<root><a>123abc123def123</a><b>123abc123</b></root>", ["abc", "def"]),
        ("<root><a>123abc123</a><a/><a>123def123</a></root>", ["abc", "def"]),
        ("<root><a>123</a></root>", []),
        ("<root><a/></root>", []),
        ("<root><b>123</b></root>", []),
    ],
    ids=[
        "multiple tags with matches",
        "tag with multiple matches",
        "matching contents at different path",
        "multiple tags with matches and an empty tag",
        "no match",
        "single empty tag",
        "xml path non-existent",
    ]
)
def test_filter_by_regex_in_xml_path(source_metadata, expected_result):
    """Test factory `filter_by_regex_in_xml_path` of `TransferUrlFilters`."""
    this_filter = TransferUrlFilters.filter_by_regex_in_xml_path(
        "([a-z]+)", ["root", "a"]
    )

    assert expected_result == this_filter(source_metadata)


def test_filter_by_regex_in_xml_path_match_anything():
    """
    Test factory `filter_by_regex_in_xml_path` of `TransferUrlFilters`
    for a regex that matches anything.

    Assert empty matches are removed.
    """
    this_filter = TransferUrlFilters.filter_by_regex_in_xml_path(
        ".*", ["root", "a"]
    )

    assert ["asd"] == this_filter("<root><a>asd</a></root>")


@pytest.mark.parametrize(
    ("source_metadata", "input_regex", "input_xpath", "expected_result"),
    [
        (
            "<root><a>123abc123</a><a>123def123</a></root>",
            "([a-z]+)",
            "./a",
            ["abc", "def"],
        ),
        (
            "<root><a>123abc123def123</a></root>",
            "([a-z]+)",
            "./a",
            ["abc", "def"],
        ),
        (
            "<root><a>123abc123def123</a><b>123abc123</b></root>",
            "([a-z]+)",
            "./a",
            ["abc", "def"],
        ),
        (
            "<root><a>123abc123</a><a/><a>123def123</a></root>",
            "([a-z]+)",
            "./a",
            ["abc", "def"],
        ),
        ("<root><a>123</a></root>", "([a-z]+)", "./a", []),
        ("<root><a/></root>", "([a-z]+)", "./a", []),
        ("<root><a>123abc123</a><a/></root>", "([a-z]+)", "./a", ["abc"]),
        ("<root><b>123</b></root>", "([a-z]+)", "./a", []),
        (
            "<root><a>123abc123</a><a>123def123</a></root>",
            "([a-z]+)",
            "./a[1]",
            ["abc"],
        ),
        (
            "<root><a>123abc123</a><a>123def123</a></root>",
            ".*",
            "./a",
            ["123abc123", "123def123"],
        ),
    ],
    ids=[
        "multiple tags with matches",
        "tag with multiple matches",
        "matching contents at different path",
        "multiple tags with matches and an empty tag",
        "no match",
        "single empty tag",
        "match and empty tag",
        "xpath non-existent",
        "multiple tags with matches - select first",
        "multiple tags with matches - match anything",
    ],
)
def test_filter_by_regex_with_xpath_query(
    source_metadata, input_regex, input_xpath, expected_result
):
    """Test factory `filter_by_regex_with_xpath_query` of `TransferUrlFilters`."""
    this_filter = TransferUrlFilters.filter_by_regex_with_xpath_query(
        input_regex, input_xpath
    )

    assert expected_result == this_filter(source_metadata)


@pytest.mark.parametrize(
    ("source_metadata", "input_regex", "input_xpath", "expected_result"),
    [
        (
            """<?xml version="1.0" encoding="UTF-8"?>
<root xmlns="uri_xmlns/">
  <namespace_a:dc xmlns:namespace_a="uri_xmlns/namespace_a/" xmlns:dc="uri_dc">
    <dc:identifier>123abc123</dc:identifier>
    <dc:identifier>123123</dc:identifier>
  </namespace_a:dc>
</root>""",
            "([a-z]+)",
            "./namespace_a:dc/dc:identifier",
            ["abc"],
        ),
        (
            """<?xml version="1.0" encoding="UTF-8"?>
<root xmlns="uri_xmlns/">
  <namespace_a:dc xmlns:namespace_a="uri_xmlns/namespace_a/" xmlns:dc="uri_dc">
    <dc:identifier>123abc123</dc:identifier>
    <dc:identifier>123def123</dc:identifier>
  </namespace_a:dc>
</root>""",
            "([a-z]+)",
            "./namespace_a:dc/dc:identifier",
            ["abc", "def"],
        ),
        (
            """<?xml version="1.0" encoding="UTF-8"?>
<root xmlns="uri_xmlns/">
  <namespace_a:dc xmlns:namespace_a="uri_xmlns/namespace_a/" xmlns:dc="uri_dc">
    <dc:identifier>123123</dc:identifier>
  </namespace_a:dc>
</root>""",
            "([a-z]+)",
            "./namespace_a:dc/dc:identifier",
            [],
        ),
        (
            """<?xml version="1.0" encoding="UTF-8"?>
<root xmlns="uri_xmlns/">
  <namespace_a:dc xmlns:namespace_a="uri_xmlns/namespace_a/" xmlns:dc="uri_dc">
    <dc:identifier>123abc123</dc:identifier>
    <dc:identifier/>
  </namespace_a:dc>
</root>""",
            "([a-z]+)",
            "./namespace_a:dc/dc:identifier",
            ["abc"],
        ),
        (
            """<?xml version="1.0" encoding="UTF-8"?>
<root xmlns="uri_xmlns/">
  <namespace_a:dc xmlns:namespace_a="uri_xmlns/namespace_a/" xmlns:dc="uri_dc">
    <dc:identifier>123abc123</dc:identifier>
  </namespace_a:dc>
  <namespace_b:dc xmlns:namespace_b="uri_xmlns/namespace_b/" xmlns:dc="uri_dc">
    <dc:identifier>123def123</dc:identifier>
  </namespace_b:dc>
</root>""",
            "([a-z]+)",
            "./namespace_b:dc/dc:identifier",
            ["def"],
        ),
    ],
    ids=[
        "single match",
        "multiple matches",
        "no match",
        "with empty tag",
        "several fields at different locations",
    ],
)
def test_filter_by_regex_with_xpath_query_realistic_metadata(
    source_metadata, input_regex, input_xpath, expected_result
):
    """
    Test factory `filter_by_regex_with_xpath_query` of `TransferUrlFilters`.
    """
    this_filter = TransferUrlFilters.filter_by_regex_with_xpath_query(
        input_regex, input_xpath
    )

    assert expected_result == this_filter(source_metadata)


def test_filter_by_regex_with_xpath_query_non_existent_namespace():
    """
    Test factory `filter_by_regex_with_xpath_query` of `TransferUrlFilters`.
    """
    this_filter = TransferUrlFilters.filter_by_regex_with_xpath_query(
        "", "./namespace_b:dc/dc:identifier"
    )

    with pytest.raises(SyntaxError) as exc_info:
        this_filter(
            """<?xml version="1.0" encoding="UTF-8"?>
<root xmlns="uri_xmlns/">
  <namespace_a:dc xmlns:namespace_a="uri_xmlns/namespace_a/" xmlns:dc="uri_dc">
    <dc:identifier>123abc123</dc:identifier>
  </namespace_a:dc>
</root>"""
        )
    assert "prefix 'namespace_b' not found in prefix map" in str(
        exc_info.value
    )


def test_download_record_payload_for_syntaxerror_in_transfer_url_filter():
    """
    Test method `known download_record_payload` of PayloadCollector class
    for a known SyntaxError from `transfer_url_filter`.
    """

    msg = "prefix 'namespace_b' not found in prefix map"
    def fake_transfer_url_filter(*args):
        raise SyntaxError(msg)

    some_payload_collector = PayloadCollector(transfer_url_filters=[fake_transfer_url_filter])
    with mock.patch.object(some_payload_collector, "download_file"):
        # test single url
        record = OAIPMHRecord("")
        some_payload_collector.download_record_payload(
            record=record,
            path=TEST_DIRECTORY,
            skip_download=True
        )
    assert msg in some_payload_collector.log[Context.ERROR][0].body


def test_download_record_payload_for_exception_in_transfer_url_filter():
    """
    Test method `download_record_payload` of PayloadCollector class
    for an unknown exception from `transfer_url_filter`.
    """

    msg = "error message"
    def fake_transfer_url_filter(*args):
        raise Exception(msg)

    some_payload_collector = PayloadCollector(transfer_url_filters=[fake_transfer_url_filter])
    with mock.patch.object(some_payload_collector, "download_file"):
        # test single url
        record = OAIPMHRecord("")
        with pytest.raises(Exception) as exc_info:
            some_payload_collector.download_record_payload(
                record=record,
                path=TEST_DIRECTORY,
                skip_download=True
            )
    assert msg == str(exc_info.value)


SIMPLE_TRANSFER_URLS = [
    "https://www.uni-muenster.de/imperia/md/images/allgemein/farbunabhaengig/unims.svg",
    "https://www.w3.org/WAI/ER/tests/xhtml/testfiles/resources/pdf/dummy.pdf"
]
SIMPLE_IDENTIFIER = "id0"
SIMPLE_METADATA = {
    "key1": "value1",
    "transfer-urls": SIMPLE_TRANSFER_URLS
}
TEST_DIRECTORY = Path("test_oai_pmh_extractor/tmp")

@pytest.fixture(name="simple_payload_collector")
def simple_payload_collector():
    # fake metadata mapper object
    def transfer_url_filter(source_metadata):
        return SIMPLE_TRANSFER_URLS

    # build PayloadCollector with faked mapper
    return PayloadCollector(
        transfer_url_filter
    )


def test_payload_collector_constructor():
    """
    Test constructor of `PayloadCollector` class for the mutually exclusive
    kwargs 'transfer_url_filters' and 'transfer_url_filter'.
    """
    with pytest.raises(TypeError) as exc_info:
        PayloadCollector(
            transfer_url_filters=[lambda x: []],
            transfer_url_filter=lambda x: []
        )
    assert str(exc_info.value) == (
        "Cannot instantiate 'PayloadCollector' with given args. "
        + "Exactly one of the kwargs 'transfer_url_filters' and "
        + "'transfer_url_filter' has to be specified."
    )

    with pytest.raises(TypeError) as exc_info:
        PayloadCollector()
    assert str(exc_info.value) == (
        "Cannot instantiate 'PayloadCollector' with given args. "
        + "Exactly one of the kwargs 'transfer_url_filters' and "
        + "'transfer_url_filter' has to be specified."
    )

    assert isinstance(
        PayloadCollector(transfer_url_filters=[lambda x: []]), PayloadCollector
    )
    assert isinstance(
        PayloadCollector(transfer_url_filter=lambda x: []), PayloadCollector
    )


def test_download_file_filename_from_header(
    simple_payload_collector
):
    """
    Test the extraction of filenames from header in method download_file
    of PayloadCollector class.

    To this end, patch the urllib.request-library's `urlopen` method to
    return stub.
    """

    # pre-test cleanup
    if TEST_DIRECTORY.is_dir():
        shutil.rmtree(TEST_DIRECTORY)

    # fake response
    test_data = b"test"
    test_filename = "filename.dat"
    class MockedResponse():
        def __init__(self, url, *args, **kwargs):
            self.url = url
        def __enter__(self):
            return self
        def __exit__(self, type, value, traceback):
            pass
        def read(self):
            return test_data
        def info(self):
            class MockedInfo():
                def get_filename(self):
                    return test_filename
            return MockedInfo()

    # fake urllib and filesystem
    # for the latter the individual types of paths are mocked
    # this is needed since pathlib.Path("..") does not necessarily return
    # Path but likely PosixPath|WindowsPath depending on OS
    with mock.patch.object(
                request,
                "urlopen",
                side_effect=lambda url, *args, **kwargs: MockedResponse(url)
            ), \
            mock.patch.object(Path, "write_bytes"), \
            mock.patch.object(PosixPath, "write_bytes"), \
            mock.patch.object(WindowsPath, "write_bytes"):

        # download file
        test_url = SIMPLE_TRANSFER_URLS[0]
        used_filename = \
            simple_payload_collector.download_file(TEST_DIRECTORY, test_url)

        assert used_filename.name == test_filename


def test_download_file_filename_from_url(
    simple_payload_collector
):
    """
    Test the extraction of filenames from url in method download_file
    of PayloadCollector class.

    To this end, patch the urllib.request-library's `urlopen` method to
    return stub.
    """

    # pre-test cleanup
    if TEST_DIRECTORY.is_dir():
        shutil.rmtree(TEST_DIRECTORY)

    # fake response
    test_data = b"test"
    test_filename = "filename.dat"
    class MockedResponse():
        def __init__(self, url, *args, **kwargs):
            self.url = url
        def __enter__(self):
            return self
        def __exit__(self, type, value, traceback):
            pass
        def read(self):
            return test_data
        def info(self):
            class MockedInfo():
                def get_filename(self):
                    return test_filename
            return MockedInfo()

    # fake urllib and filesystem
    # for the latter the individual types of paths are mocked
    # this is needed since pathlib.Path("..") does not necessarily return
    # Path but likely PosixPath|WindowsPath depending on OS
    with mock.patch.object(
                request,
                "urlopen",
                side_effect=lambda url, *args, **kwargs: MockedResponse(url)
            ), \
            mock.patch.object(Path, "write_bytes"), \
            mock.patch.object(PosixPath, "write_bytes"), \
            mock.patch.object(WindowsPath, "write_bytes"):

        # download file
        test_url = "http://domain.com/" + test_filename
        used_filename = \
            simple_payload_collector.download_file(TEST_DIRECTORY, test_url)

        assert used_filename.name in test_url


def test_download_file_filename_failing(
    simple_payload_collector
):
    """
    Test the exception behavior of filename extraction in method
    download_file of PayloadCollector class.

    To this end, patch the urllib.request-library's `urlopen` method to
    return stub and make pathlib.Path.is_file() always return True.
    """

    # pre-test cleanup
    if TEST_DIRECTORY.is_dir():
        shutil.rmtree(TEST_DIRECTORY)

    # fake response
    test_data = b"test"
    test_filename = "filename.dat"
    class MockedResponse():
        def __init__(self, url, *args, **kwargs):
            self.url = url
        def __enter__(self):
            return self
        def __exit__(self, type, value, traceback):
            pass
        def read(self):
            return test_data
        def info(self):
            class MockedInfo():
                def get_filename(self):
                    return test_filename
            return MockedInfo()

    # fake urllib and filesystem
    # for the latter the individual types of paths are mocked
    # this is needed since pathlib.Path("..") does not necessarily return
    # Path but likely PosixPath|WindowsPath depending on OS
    with mock.patch.object(
                request,
                "urlopen",
                side_effect=lambda url, *args, **kwargs: MockedResponse(url)
            ), \
            mock.patch.object(Path, "is_file", side_effect=lambda: True), \
            mock.patch.object(PosixPath, "is_file", side_effect=lambda: True), \
            mock.patch.object(WindowsPath, "is_file", side_effect=lambda: True):

        # download file
        test_url = SIMPLE_TRANSFER_URLS[0]
        with pytest.raises(FileExistsError) as exc_info:
            simple_payload_collector.download_file(TEST_DIRECTORY, test_url)

    assert test_url in simple_payload_collector.log[Context.ERROR][0].body


def test_download_file_filename_override(
    simple_payload_collector
):
    """
    Test the filename-override in method download_file of PayloadCollector
    class.

    To this end, patch the urllib.request-library's `urlopen` method to
    return stub.
    """

    # pre-test cleanup
    if TEST_DIRECTORY.is_dir():
        shutil.rmtree(TEST_DIRECTORY)

    # fake response
    test_data = b"test"
    test_filename = "filename.dat"
    actual_filename = "filename2.dat"
    class MockedResponse():
        def __init__(self, url, *args, **kwargs):
            self.url = url
        def __enter__(self):
            return self
        def __exit__(self, type, value, traceback):
            pass
        def read(self):
            return test_data
        def info(self):
            class MockedInfo():
                def get_filename(self):
                    return test_filename
            return MockedInfo()

    # fake urllib and filesystem
    # for the latter the individual types of paths are mocked
    # this is needed since pathlib.Path("..") does not necessarily return
    # Path but likely PosixPath|WindowsPath depending on OS
    with mock.patch.object(
                request,
                "urlopen",
                side_effect=lambda url, *args, **kwargs: MockedResponse(url)
            ), \
            mock.patch.object(Path, "write_bytes"), \
            mock.patch.object(PosixPath, "write_bytes"), \
            mock.patch.object(WindowsPath, "write_bytes"):

        # download file
        test_url = SIMPLE_TRANSFER_URLS[0]
        used_filename = \
            simple_payload_collector.download_file(
                TEST_DIRECTORY, test_url, Path(actual_filename)
            )

        assert used_filename.name == actual_filename


def test_download_file_faked(simple_payload_collector):
    """
    Test method download_file of PayloadCollector class.

    To this end, patch the urllib.request-library's `urlopen` method to
    return stub. Evaluate number of calls and arguments afterwards.
    """

    # pre-test cleanup
    if TEST_DIRECTORY.is_dir():
        shutil.rmtree(TEST_DIRECTORY)

    # fake response
    test_data = b"test"
    test_filename = "filename"
    class MockedResponse():
        def __init__(self, url, *args, **kwargs):
            self.url = url
        def __enter__(self):
            return self
        def __exit__(self, type, value, traceback):
            pass
        def read(self):
            return test_data
        def info(self):
            class MockedInfo():
                def get_filename(self):
                    return test_filename
            return MockedInfo()
    # fake urllib and filesystem
    # for the latter the individual types of paths are mocked
    # this is needed since pathlib.Path("..") does not necessarily return
    # Path but likely PosixPath|WindowsPath depending on OS
    with mock.patch.object(
                request,
                "urlopen",
                side_effect=lambda url, *args, **kwargs: MockedResponse(url)
            ), \
            mock.patch.object(Path, "write_bytes"), \
            mock.patch.object(PosixPath, "write_bytes"), \
            mock.patch.object(WindowsPath, "write_bytes"):

        # download file
        test_url = SIMPLE_TRANSFER_URLS[0]
        simple_payload_collector.download_file(TEST_DIRECTORY, test_url)

        # assert single file has been written
        called_path_class = None
        total_call_count = 0
        for path_class in [Path, PosixPath, WindowsPath]:
            total_call_count += path_class.write_bytes.call_count
            if path_class.write_bytes.call_count > 0:
                called_path_class = path_class
        assert total_call_count == 1
        assert called_path_class is not None

        # test arguments
        written_content = called_path_class.write_bytes.call_args_list[0][0][0]
        assert written_content == MockedResponse(SIMPLE_TRANSFER_URLS[0]).read()


# switch these two lines to in-/exclude this test in suite
# def notest_download_file_encoding(simple_payload_collector):
def test_download_file_encoding(simple_payload_collector):
    """
    Actually test method download_file of PayloadCollector class.

    Two example files are downloaded and afterwards removed.
    """

    # pre-test cleanup
    if TEST_DIRECTORY.is_dir():
        shutil.rmtree(TEST_DIRECTORY)

    TEST_DIRECTORY.mkdir(parents=True, exist_ok=True)

    # download file
    for url in SIMPLE_TRANSFER_URLS:
        file = simple_payload_collector.download_file(TEST_DIRECTORY, url)
        assert file.is_file()

    # post-test cleanup
    if TEST_DIRECTORY.is_dir():
        shutil.rmtree(TEST_DIRECTORY)


def test_download_record_payload_success():
    """
    Test method download_record_payload of PayloadCollector class.

    To this end, patch the download_file-method to do nothing. Evaluate
    number of calls afterwards.
    """

    SINGLE_TRANSFER_URLS = [
        "a"
    ]
    MORE_TRANSFER_URLS = [
        "1",
        "2"
    ]

    some_payload_collector = PayloadCollector(lambda x: [])
    with mock.patch.object(some_payload_collector, "download_file"):
        # test single url
        record = OAIPMHRecord("", file_urls=SINGLE_TRANSFER_URLS)
        some_payload_collector.download_record_payload(
            record=record,
            path=TEST_DIRECTORY
        )
        assert some_payload_collector.download_file.call_count == \
            len(SINGLE_TRANSFER_URLS)
        assert record.files[0]["complete"]
        # reset
        some_payload_collector.download_file.reset_mock()
        # assert multiple urls
        record = OAIPMHRecord("", file_urls=MORE_TRANSFER_URLS)
        some_payload_collector.download_record_payload(
            record=record,
            path=TEST_DIRECTORY
        )
        for file in record.files:
            assert file["complete"]
        assert some_payload_collector.download_file.call_count == \
            len(MORE_TRANSFER_URLS)


def test_download_record_payload_duplicate_urls():
    """
    Test method download_record_payload of PayloadCollector class
    for duplicate urls.

    To this end, patch the download_file-method to do nothing. Evaluate
    number of calls afterwards.
    """

    TRANSFER_URLS_1 = [
        "1",
        "2",
        "1"
    ]
    TRANSFER_URLS_2 = [
        "3",
        "3",
        "2"
    ]

    some_payload_collector = PayloadCollector(
        transfer_url_filters=[
            lambda x: TRANSFER_URLS_1,
            lambda x: TRANSFER_URLS_2
        ]
    )
    with mock.patch.object(some_payload_collector, "download_file"):
        record = OAIPMHRecord("")
        some_payload_collector.download_record_payload(
            record=record,
            path=TEST_DIRECTORY
        )
        for file in record.files:
            assert file["complete"]
        assert some_payload_collector.download_file.call_count == 3


def test_download_record_payload_renew_urls():
    """
    Test method download_record_payload of PayloadCollector class
    with renew_urls.

    To this end, patch the download_file-method to do nothing. Evaluate
    number of calls afterwards.
    """

    SINGLE_TRANSFER_URLS = [
        "a"
    ]
    MORE_TRANSFER_URLS = [
        "1",
        "2"
    ]

    some_payload_collector = PayloadCollector(
        transfer_url_filters=[
            lambda x: SINGLE_TRANSFER_URLS,
            lambda x: MORE_TRANSFER_URLS,
        ]
    )
    with mock.patch.object(some_payload_collector, "download_file"):
        # test single url
        record = OAIPMHRecord("", file_urls=SINGLE_TRANSFER_URLS)
        some_payload_collector.download_record_payload(
            record=record,
            path=TEST_DIRECTORY,
            renew_urls=True
        )
        assert some_payload_collector.download_file.call_count == \
            len(SINGLE_TRANSFER_URLS) + len(MORE_TRANSFER_URLS)
        assert record.files[0]["complete"]
        # reset
        some_payload_collector.download_file.reset_mock()
        # assert multiple urls
        record = OAIPMHRecord("", file_urls=MORE_TRANSFER_URLS)
        some_payload_collector.download_record_payload(
            record=record,
            path=TEST_DIRECTORY
        )
        for file in record.files:
            assert file["complete"]
        assert some_payload_collector.download_file.call_count == \
            len(MORE_TRANSFER_URLS)


def test_download_record_payload_no_files():
    """
    Test method download_record_payload of PayloadCollector class
    for record with an empty list/None in record.files.

    To this end, patch the download_file-method to do nothing. Evaluate
    number of calls afterwards.
    """

    SINGLE_TRANSFER_URLS = [
        "a"
    ]
    MORE_TRANSFER_URLS = [
        "1",
        "2"
    ]

    some_payload_collector = PayloadCollector(
        transfer_url_filters=[
            lambda x: SINGLE_TRANSFER_URLS,
            lambda x: MORE_TRANSFER_URLS,
        ]
    )
    with mock.patch.object(some_payload_collector, "download_file"):
        # test single url
        record = OAIPMHRecord("", file_urls=[])
        some_payload_collector.download_record_payload(
            record=record,
            path=TEST_DIRECTORY
        )
        assert some_payload_collector.download_file.call_count == \
            len(SINGLE_TRANSFER_URLS) + len(MORE_TRANSFER_URLS)
        assert record.files[0]["complete"]
        # reset
        some_payload_collector.download_file.reset_mock()
        # assert multiple urls
        record = OAIPMHRecord("", file_urls=MORE_TRANSFER_URLS)
        some_payload_collector.download_record_payload(
            record=record,
            path=TEST_DIRECTORY
        )
        for file in record.files:
            assert file["complete"]
        assert some_payload_collector.download_file.call_count == \
            len(MORE_TRANSFER_URLS)


def test_download_files_partial_fail():
    """
    Test method downloads_files of PayloadCollector class.

    To this end, patch the download_file-method to do nothing. Evaluate
    number of calls afterwards.
    """

    MORE_TRANSFER_URLS = [
        "1",
        "2"
    ]
    error_msg = "."

    first_run = True
    # fake raising Error
    def download_side_effect_error(*args, **kwargs):
        nonlocal first_run
        if first_run:
            first_run = False
            raise KeyError(error_msg)

    some_payload_collector = PayloadCollector(lambda x: [])
    with mock.patch.object(
            some_payload_collector,
            "download_file",
            side_effect=download_side_effect_error
    ):
        # assert multiple urls
        record = OAIPMHRecord("", file_urls=MORE_TRANSFER_URLS)
        some_payload_collector.download_record_payload(
            record=record,
            path=TEST_DIRECTORY
        )
        complete = 0
        incomplete = 0
        for file in record.files:
            complete += 1 if file["complete"] else 0
            incomplete += 1 if not file["complete"] else 0

        assert complete == 1
        assert incomplete == len(MORE_TRANSFER_URLS) - 1
        assert some_payload_collector.download_file.call_count == \
            len(MORE_TRANSFER_URLS)
        assert error_msg in some_payload_collector.log[Context.ERROR][0].body


def test_download_file_file_url(simple_payload_collector):
    """
    Test method downloads_file of PayloadCollector class with a file://..
    url.
    """

    # pre-test cleanup
    if TEST_DIRECTORY.is_dir():
        shutil.rmtree(TEST_DIRECTORY)

    # prepare test
    test_data = b"test"
    test_filename = "test.dat"
    test_file = TEST_DIRECTORY / test_filename
    test_file_download = TEST_DIRECTORY / "subdir"

    TEST_DIRECTORY.mkdir(parents=True, exist_ok=True)
    test_file_download.mkdir(parents=True, exist_ok=True)
    test_file.write_bytes(test_data)

    # perform download
    simple_payload_collector.download_file(
        test_file_download,
        "file://" + str(test_file.absolute())
    )

    # validate result
    assert (test_file_download / test_filename).is_file()
    assert (test_file_download / test_filename).read_bytes() == test_data

    # post-test cleanup
    if TEST_DIRECTORY.is_dir():
        shutil.rmtree(TEST_DIRECTORY)


def test_download_file_url_error(simple_payload_collector):
    """
    Test method download_file of PayloadCollector class with url-error.
    """
    # pre-test cleanup
    if TEST_DIRECTORY.is_dir():
        shutil.rmtree(TEST_DIRECTORY)
    TEST_DIRECTORY.mkdir(parents=True)

    class FakeStdErr:
        def __init__(self):
            self.text = ""
        def write(self, line):
            self.text += line

    stderr = FakeStdErr()

    # perform download
    with mock.patch("oai_pmh_extractor.payload_collector.sys.stderr", stderr):
        with pytest.raises(request.URLError):
            simple_payload_collector.download_file(
                TEST_DIRECTORY / str(uuid4()),
                "file://" + str(uuid4())
            )

        # two failed attempts
        stderr_lines = stderr.text.splitlines()
        assert len(stderr_lines) == 2
        assert stderr_lines[0] == stderr_lines[1]

    # post-test cleanup
    if TEST_DIRECTORY.is_dir():
        shutil.rmtree(TEST_DIRECTORY)


@pytest.mark.parametrize(
    "latency",
    [0, 0.1],
    ids=["no-timeout", "timeout"]
)
def test_payload_collector_timeout(latency, run_service):
    """
    Test timeout-behavior of PayloadCollector by faking a minimal
    http-server.
    """
    # pre-test cleanup
    if TEST_DIRECTORY.is_dir():
        shutil.rmtree(TEST_DIRECTORY)
    TEST_DIRECTORY.mkdir(parents=True)

    expected_response = b"data"
    filename = Path("timeout-test.txt")

    # setup fake server
    def respond_with_latency():
        sleep(latency)
        return Response(expected_response, mimetype="text/html")

    run_service(routes=[("/", respond_with_latency, ["GET"])], port=8080)

    # perform test
    def transfer_url_filter(source_metadata):
        return ["some_file"]
    some_payload_collector = PayloadCollector(
        transfer_url_filter, timeout=0.05
    )
    if latency > 0:
        with pytest.raises(TimeoutError):
            some_payload_collector.download_file(
                TEST_DIRECTORY,
                url="http://localhost:8080",
                _filename=filename
            )
    else:
        some_payload_collector.download_file(
            TEST_DIRECTORY,
            url="http://localhost:8080",
            _filename=filename
        )
        assert (TEST_DIRECTORY / filename).read_bytes() == expected_response

    # post-test cleanup
    if TEST_DIRECTORY.is_dir():
        shutil.rmtree(TEST_DIRECTORY)

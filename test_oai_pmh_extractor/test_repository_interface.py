"""Test module for the class RepositoryInterface."""

from unittest import mock
from time import sleep

import requests
import pytest
import xmltodict
from dcm_common import LoggingContext as Context

from oai_pmh_extractor import RepositoryInterface, OAIPMHRecord


@pytest.fixture(name="simple_interface")
def simple_interface():
    return RepositoryInterface("https://www.lzv.nrw/oai")


def _build_request_options(options):
    """Helper for test__build_request_*-tests."""
    if len(options) > 0:
        options_substr = "?"
        stringified_options = []
        for option, value in options.items():
            stringified_options.append(option + "=" + value)
        options_substr += "&".join(stringified_options)
        return options_substr
    return ""
@pytest.mark.parametrize("request_options", [
    { # empty options
    },
    { # single option
        "option": "value"
    },
    { # multiple options
        "option1": "value1",
        "option2": "value2"
    }
], ids=["empty options", "single option", "multiple options"])
def test__build_request(simple_interface, request_options):
    """Test _build_request-method of RepositoryInterface."""

    request_url = simple_interface._build_request(**request_options)
    expected_url = simple_interface._base_url \
        + _build_request_options(request_options)

    assert expected_url == request_url

def test__build_request_missing_question_mark(simple_interface):
    """
    Test _build_request-method of RepositoryInterface for dynamically
    adding '?' to harvest-url if needed.
    """

    base = "a"
    options = {"option": "value"}

    # test without '?'
    simple_interface._base_url = base
    request_url = simple_interface._build_request(**options)
    expected_url = base + _build_request_options(options)
    assert expected_url == request_url

    # test with '?'
    simple_interface._base_url = base + "?"
    request_url = simple_interface._build_request(**options)
    expected_url = base + _build_request_options(options)
    assert expected_url == request_url

def test_identify(simple_interface, generate_FakeRequestsResponse):
    """
    Test identify-method of RepositoryInterface.

    This test relies on RepositoryInterface using the
    requests-library internally.
    """
    request_options = {
        "verb": "Identify"
    }

    # define requests.get-stub
    expected_response = b"<test_tag>value</test_tag>"
    expected_response_dict = {"test_tag": "value"}

    # use fake-setup for test
    with mock.patch.object(
                requests,
                "get",
                side_effect=lambda url, *args, **kwargs: generate_FakeRequestsResponse(expected_response)
            ), \
            mock.patch.object(simple_interface, "_build_request"):
        # execute in RepositoryInterface
        test_response = simple_interface.identify()

        # assert behavior
        assert isinstance(test_response, dict)

        assert expected_response_dict == test_response

        simple_interface._build_request.assert_called_once_with(**request_options)

def test_list_metadata_formats(simple_interface, generate_FakeRequestsResponse):
    """
    Test list_metadata_formats-method of RepositoryInterface.

    This test relies on RepositoryInterface using the
    requests-library internally.
    """
    request_options = {
        "verb": "ListMetadataFormats"
    }

    # define requests.get-stub
    fake_response = b"""<OAI-PMH>
<ListMetadataFormats>
<metadataFormat>
    <metadataPrefix>epicur</metadataPrefix>
    <schema>
        http://nbn-resolving.de/urn/resolver.pl?urn=urn:nbn:de:1111-2004033116
    </schema>
    <metadataNamespace>urn:nbn:de:1111-2004033116</metadataNamespace>
</metadataFormat>
<metadataFormat>
    <metadataPrefix>oai_dc</metadataPrefix>
    <schema>
    http://dublincore.org/schemas/xmls/simpledc20021212.xsd
    </schema>
    <metadataNamespace>http://www.openarchives.org/OAI/2.0/oai_dc/</metadataNamespace>
</metadataFormat>
</ListMetadataFormats>
</OAI-PMH>
"""
    expected_response_list = [
        {
            "metadataPrefix": "epicur",
            "schema": "http://nbn-resolving.de/urn/resolver.pl?urn=urn:nbn:de:1111-2004033116",
            "metadataNamespace": "urn:nbn:de:1111-2004033116"
        },
        {
            "metadataPrefix": "oai_dc",
            "schema": "http://dublincore.org/schemas/xmls/simpledc20021212.xsd",
            "metadataNamespace": "http://www.openarchives.org/OAI/2.0/oai_dc/"
        }
    ]

    # use fake-setup for test
    with mock.patch.object(
                requests,
                "get",
                side_effect=lambda url, *args, **kwargs: generate_FakeRequestsResponse(fake_response)
            ), \
            mock.patch.object(simple_interface, "_build_request"):
        # execute in RepositoryInterface
        test_response = simple_interface.list_metadata_formats()

        # assert behavior
        assert isinstance(test_response, list)

        assert sorted(test_response, key=lambda k: k['metadataPrefix']) ==\
            sorted(expected_response_list, key=lambda k: k['metadataPrefix'])

        simple_interface._build_request.assert_called_once_with(**request_options)

def test_list_metadata_prefixes(simple_interface, generate_FakeRequestsResponse):
    """
    Test list_metadata_prefixes-method of RepositoryInterface.

    This test relies on RepositoryInterface using the
    requests-library internally.
    """
    request_options = {
        "verb": "ListMetadataFormats"
    }

    # define requests.get-stub
    fake_response = b"""<OAI-PMH>
<ListMetadataFormats>
<metadataFormat>
    <metadataPrefix>epicur</metadataPrefix>
    <schema>
        http://nbn-resolving.de/urn/resolver.pl?urn=urn:nbn:de:1111-2004033116
    </schema>
    <metadataNamespace>urn:nbn:de:1111-2004033116</metadataNamespace>
</metadataFormat>
<metadataFormat>
    <metadataPrefix>oai_dc</metadataPrefix>
    <schema>
    http://dublincore.org/schemas/xmls/simpledc20021212.xsd
    </schema>
    <metadataNamespace>http://www.openarchives.org/OAI/2.0/oai_dc/</metadataNamespace>
</metadataFormat>
</ListMetadataFormats>
</OAI-PMH>
"""
    expected_response_list = ["epicur", "oai_dc"]

    # use fake-setup for test
    with mock.patch.object(
                requests,
                "get",
                side_effect=lambda url, *args, **kwargs: generate_FakeRequestsResponse(fake_response)
            ), \
            mock.patch.object(simple_interface, "_build_request"):
        # execute in RepositoryInterface
        test_response = simple_interface.list_metadata_prefixes()

        # assert behavior
        assert isinstance(test_response, list)

        assert sorted(test_response) == sorted(expected_response_list)

        simple_interface._build_request.assert_called_once_with(**request_options)

@pytest.mark.parametrize(
    ("fake_response", "expected_response_list", "expected_response_token"),
    [
        ( # no identifier
            b"""<OAI-PMH>
<error code="noRecordsMatch">No documents retrieved.</error>
</OAI-PMH>
""",
            [],
            None
        ),
        ( # single identifier
            b"""<OAI-PMH>
<ListIdentifiers>
    <header>
        <identifier>oai:id0</identifier>
    </header>
</ListIdentifiers>
</OAI-PMH>
""",
            ["oai:id0"],
            None
        ),
        ( # multiple identifiers with resumption token
            b"""<OAI-PMH>
<ListIdentifiers>
    <header>
        <identifier>oai:id0</identifier>
    </header>
    <header>
        <identifier>oai:id1</identifier>
    </header>
    <resumptionToken>x</resumptionToken>
</ListIdentifiers>
</OAI-PMH>
""",
            ["oai:id0", "oai:id1"],
            "x"
        ),
        ( # resumption token with attributes
            b"""<OAI-PMH>
<ListIdentifiers>
    <header>
        <identifier>oai:id0</identifier>
    </header>
    <resumptionToken attribute="y">x</resumptionToken>
</ListIdentifiers>
</OAI-PMH>
""",
            ["oai:id0"],
            "x"
        ),
        ( # resumption token with attributes
            b"""<OAI-PMH>
<ListIdentifiers>
    <header>
        <identifier>oai:id0</identifier>
    </header>
    <resumptionToken attribute="y"></resumptionToken>
</ListIdentifiers>
</OAI-PMH>
""",
            ["oai:id0"],
            None
        ),
    ],
    ids=[
        "no_ids", "single_id", "two_ids_with_resumption_token",
        "resumption_token_with_attributes",
        "resumption_token_with_attributes_empty",
    ]
)
def test_list_identifiers_parameterized(
    simple_interface,
    generate_FakeRequestsResponse,
    fake_response,
    expected_response_list,
    expected_response_token
):
    """
    Test list_identifiers-method of RepositoryInterface.

    This test relies on RepositoryInterface using the
    requests-library internally.
    """
    request_options = {
        "verb": "ListIdentifiers",
        "metadataPrefix": "oai_dc"
    }

    # use fake-setup for test
    with mock.patch.object(
                requests,
                "get",
                side_effect=lambda url, *args, **kwargs: generate_FakeRequestsResponse(fake_response)
            ), \
            mock.patch.object(simple_interface, "_build_request"):
        # execute in RepositoryInterface
        test_response, test_response_token = \
            simple_interface.list_identifiers(
                _metadata_prefix = request_options["metadataPrefix"]
            )

        # assert behavior
        assert isinstance(test_response, list)

        assert sorted(test_response) == sorted(expected_response_list)

        assert test_response_token == expected_response_token

        simple_interface._build_request.assert_called_once_with(**request_options)


def test_list_identifiers_exhaustive(simple_interface, generate_FakeRequestsResponse):
    """
    Test method `list_identifiers_exhaustive` of `RepositoryInterface`.
    """

    counter = [0]  # non-primitive type to enable use in fake function
    def fake_list_identifiers(_resumption_token, *args, **kwargs):
        if counter[0] < 3:
            counter[0] = counter[0] + 1
            return [str(counter[0])], "x"
        return [], None

    with mock.patch.object(
        simple_interface, "list_identifiers", side_effect=fake_list_identifiers
    ):
        identifiers = simple_interface.list_identifiers_exhaustive(
            metadata_prefix="y"
        )
        assert identifiers == ["1", "2", "3"]
        assert simple_interface.list_identifiers.call_count == 4


def test_list_identifiers_resumption_token(simple_interface, generate_FakeRequestsResponse):
    """
    Test list_identifiers-method of RepositoryInterface.

    This test relies on RepositoryInterface using the
    requests-library internally.
    """
    request_options = {
        "verb": "ListIdentifiers",
        "metadataPrefix": "oai_dc",
        "resumptionToken": "x"
    }

    # define requests.get-stub
    fake_response = b"""<OAI-PMH>
<ListIdentifiers>
    <header>
        <identifier>oai:id2</identifier>
    </header>
</ListIdentifiers>
</OAI-PMH>
"""
    expected_response_list = ["oai:id2"]
    expected_response_token = None

    # use fake-setup for test
    with mock.patch.object(
                requests,
                "get",
                side_effect=lambda url, *args, **kwargs: generate_FakeRequestsResponse(fake_response)
            ), \
            mock.patch.object(simple_interface, "_build_request"):
        # execute in RepositoryInterface
        test_response, test_response_token = \
            simple_interface.list_identifiers(
                _metadata_prefix = request_options["metadataPrefix"],
                _resumption_token = request_options["resumptionToken"]
            )

        # assert behavior
        assert isinstance(test_response, list)

        assert sorted(test_response) == sorted(expected_response_list)

        assert test_response_token == expected_response_token

        simple_interface._build_request.assert_called_once_with(
            verb="ListIdentifiers",
            resumptionToken="x"
        )


def test_list_identifiers_empty_kwargs(simple_interface, generate_FakeRequestsResponse):
    """Test error behavior of list_identifiers-method of RepositoryInterface."""

    # use fake-setup for test
    with pytest.raises(ValueError) as exc_info:
        # execute in RepositoryInterface
        test_response, test_response_token = \
            simple_interface.list_identifiers()
    assert "Missing _metadata_prefix".lower() in str(exc_info.value).lower()

@pytest.mark.parametrize(
    ("fake_response", "expected_response_list", "expected_resumption_token"),
    [
        (b"""<OAI-PMH>
<ListSets>
<set>
    <setSpec>set1</setSpec>
    <setName>Set 1</setName>
</set>
<set>
    <setSpec>set2</setSpec>
    <setName>Set 2</setName>
</set>
<set>
    <setSpec>set1:seta</setSpec>
    <setName>Subset A in Set 1</setName>
</set>
</ListSets>
</OAI-PMH>
""",
        [
            {
                "setSpec": "set1",
                "setName": "Set 1"
            },
            {
                "setSpec": "set2",
                "setName": "Set 2"
            },
            {
                "setSpec": "set1:seta",
                "setName": "Subset A in Set 1"
            }
        ],
        None
    ), (b"""<OAI-PMH>
<ListSets>
    <resumptionToken>x</resumptionToken>
</ListSets>
</OAI-PMH>
""", [], "x"), (b"""<OAI-PMH>
<ListSets>
    <resumptionToken attribute="y">x</resumptionToken>
</ListSets>
</OAI-PMH>
""", [], "x")
    ],
    ids=[
        "simple_list_sets_wo_resumptionToken",
        "list_sets_w_resumptionToken",
        "list_sets_w_resumptionToken_w_attribute"
    ]
)
def test_list_sets(
    simple_interface,
    generate_FakeRequestsResponse,
    fake_response,
    expected_response_list,
    expected_resumption_token
):
    """
    Test list_sets-method of RepositoryInterface.

    This test relies on RepositoryInterface using the
    requests-library internally.
    """
    request_options = {
        "verb": "ListSets"
    }

    # use fake-setup for test
    with mock.patch.object(
                requests,
                "get",
                side_effect=lambda url, *args, **kwargs: generate_FakeRequestsResponse(fake_response)
            ), \
            mock.patch.object(simple_interface, "_build_request"):
        # execute in RepositoryInterface
        test_response = simple_interface.list_sets()

        simple_interface._build_request.assert_called_once_with(**request_options)

        # assert behavior
        assert isinstance(test_response[0], list)

        assert sorted(test_response[0], key=lambda k: k["setSpec"]) ==\
            sorted(expected_response_list, key=lambda k: k["setSpec"])

        assert test_response[1] == expected_resumption_token

@pytest.mark.parametrize(
    ("resumption_token"),
    [
        (None),
        ("x")
    ],
    ids=["without_token", "with_token"]
)
def test_list_sets_getting_resumption_token(
    simple_interface,
    resumption_token
):
    """
    Test resumption_token-input for list_sets-method of RepositoryInterface.
    """

    request_options = {
        "verb": "ListSets"
    }
    if resumption_token is not None:
        request_options["resumptionToken"] = resumption_token

    # use fake-setup for test
    with mock.patch.object(
        simple_interface,
        "_execute_http_request",
        side_effect=lambda request_url: b"<OAI-PMH></OAI-PMH>"
    ), mock.patch.object(simple_interface, "_build_request"):
        # execute in RepositoryInterface
        simple_interface.list_sets(_resumption_token=resumption_token)

        simple_interface._build_request.assert_called_once_with(**request_options)


@pytest.mark.parametrize(
    ("fake_response", "expected_status"),
    [
        (b"""<OAI-PMH>
<GetRecord>
    <record>
        <header>
            <identifier>oai:id0</identifier>
        </header>
        <metadata>
            <oai_dc:dc>
                <dc:title>
                    Title
                </dc:title>
            </oai_dc:dc>
        </metadata>
    </record>
</GetRecord>
</OAI-PMH>
""", ""),
        (b"""<OAI-PMH>
<GetRecord>
    <record>
        <header status="deleted">
            <identifier>oai:id0</identifier>
        </header>
    </record>
</GetRecord>
</OAI-PMH>
""", "deleted")
    ],
    ids=["basic_record", "deleted_record"]
)
def test_get_record(
    simple_interface,
    generate_FakeRequestsResponse,
    fake_response,
    expected_status
):
    """
    Test get_record-method of RepositoryInterface.

    This test relies on RepositoryInterface using the
    requests-library internally.
    """
    request_options = {
        "verb": "GetRecord",
        "identifier": "oai:id0",
        "metadataPrefix": "oai_dc"
    }

    expected_response = fake_response.decode("utf-8")

    # use fake-setup for test
    with mock.patch.object(
                requests,
                "get",
                side_effect=lambda url, *args, **kwargs: generate_FakeRequestsResponse(fake_response)
            ), \
            mock.patch.object(simple_interface, "_build_request"):
        # execute in RepositoryInterface
        test_response = simple_interface.get_record(
            metadata_prefix=request_options["metadataPrefix"],
            identifier=request_options["identifier"],
        )

        # assert behavior
        assert isinstance(test_response, OAIPMHRecord)

        assert test_response.metadata_raw == expected_response
        assert test_response.status == expected_status

        simple_interface._build_request.assert_called_once_with(**request_options)

def test_list_records(simple_interface):
    """Test list_records-method of RepositoryInterface."""

    fake_ids = ["id0", "id1"]
    resumption_token = None

    # use fake-setup for test
    with mock.patch.object(
                simple_interface,
                "list_identifiers",
                side_effect=lambda **kwargs: (fake_ids, resumption_token)
            ), \
        mock.patch.object(
                simple_interface,
                "get_record",
                side_effect=lambda metadata_prefix, identifier: OAIPMHRecord(identifier)
            ):
        # execute in RepositoryInterface
        test_response, test_resumption_token = \
            simple_interface.list_records("oai_dc")

        # assert behavior
        assert isinstance(test_response, list)
        assert len(test_response) == len(fake_ids)
        for record, fake_id in zip(test_response, fake_ids):
            assert record.identifier == fake_id
        assert test_resumption_token == resumption_token

        assert simple_interface.list_identifiers.call_count == 1
        assert simple_interface.get_record.call_count == len(fake_ids)

def test__check_for_oaipmh_errors(simple_interface):
    """Test internal-method _check_for_oaipmh_errors of RepositoryInterface."""

    fake_code = "badVerb"
    fake_message = "Illegal OAI verb"
    fake_data = f"""<?xml version="1.0" encoding="UTF-8"?>
<OAI-PMH>
  <error code="{fake_code}">{fake_message}</error>
</OAI-PMH>"""
    fake_data_dict = xmltodict.parse(fake_data)

    assert len(simple_interface.log.report) == 0

    error = simple_interface._check_for_oaipmh_errors(fake_data_dict)

    assert error

    assert Context.ERROR in simple_interface.log
    assert len(simple_interface.log[Context.ERROR]) == 1
    assert fake_code in simple_interface.log[Context.ERROR][0].body
    assert fake_message in simple_interface.log[Context.ERROR][0].body

@pytest.mark.parametrize(
    ("method", "kwargs", "expected_return"),
    [
        (
            "list_metadata_formats",
            {},
            []
        ),
        (
            "list_metadata_prefixes",
            {},
            []
        ),
        (
            "list_identifiers",
            {"_metadata_prefix": ""},
            ([], None)
        ),
        (
            "list_sets",
            {},
            ([], None)
        ),
        (
            "get_record",
            {"metadata_prefix": "", "identifier": ""},
            None
        ),
        (
            "list_records",
            {"metadata_prefix": ""},
            ([], None)
        ),
    ],
    ids=[
        "list_metadata_formats", "list_metadata_prefixes", "list_identifiers",
        "list_sets", "get_record", "list_records"
    ]
)
def test_methods_with_error(
    simple_interface,
    method, kwargs,
    expected_return
):
    """Test error-behavior of RepositoryInterface-methods."""

    fake_error_code = "errorCode"
    fake_error_message = "Short Description"
    fake_response = f"""<?xml version="1.0" encoding="UTF-8"?>
<OAI-PMH>
  <error code="{fake_error_code}">{fake_error_message}</error>
</OAI-PMH>"""

    # fake _execute_http_request
    with mock.patch.object(
                simple_interface,
                "_execute_http_request",
                side_effect=lambda request_url: fake_response
            ):
        # check state of log before request
        assert len(simple_interface.log.report) == 0

        # make request
        result = getattr(simple_interface, method)(**kwargs)


        # check results
        assert result == expected_return

        assert Context.ERROR in simple_interface.log
        assert len(simple_interface.log[Context.ERROR]) == 1
        assert fake_error_code in simple_interface.log[Context.ERROR][0].body
        assert fake_error_message in simple_interface.log[Context.ERROR][0].body

def test__execute_http_request_error(generate_FakeRequestsResponse):
    """
    Test error-behavior of internal method _execute_http_request.

    This test relies on RepositoryInterface using the
    requests-library internally.
    """

    # define requests.get-stub
    expected_response = b""
    expected_error_code = 500
    expected_error_msg = "Internal Server Error"
    some_repository_interface = RepositoryInterface("")

    # use fake-setup for test
    with mock.patch.object(
                requests,
                "get",
                side_effect=lambda url, *args, **kwargs: generate_FakeRequestsResponse(
                    expected_response,
                    (expected_error_code, expected_error_msg)
                )
            ):
        # execute in RepositoryInterface
        with pytest.raises(requests.HTTPError) as exc_info:
            some_repository_interface._execute_http_request("")
        assert str(expected_error_code) in str(exc_info)
        assert expected_error_msg in str(exc_info)


@pytest.mark.parametrize(
    "latency",
    [0, 0.1],
    ids=["no-timeout", "timeout"]
)
def test_interface_timeout(latency, request):
    """
    Test timeout-behavior of RepositoryInterface by faking a minimal
    http-server.
    """
    from http.server import HTTPServer, BaseHTTPRequestHandler
    from multiprocessing import Process

    expected_response = b"<test_tag>value</test_tag>"
    expected_response_dict = {"test_tag": "value"}
    # setup fake server
    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            sleep(latency)
            self.send_response(200)
            self.send_header("Content-type", "text/html")
            self.end_headers()
            self.wfile.write(expected_response)
    http = HTTPServer(("localhost", 8080), Handler)
    # run fake server
    p = Process(
        target=http.serve_forever,
        daemon=True
    )
    def __():  # kill server after this test
        if p.is_alive():
            p.kill()
            p.join()
    request.addfinalizer(__)
    p.start()

    # perform test
    some_repository_interface = RepositoryInterface(
        "http://localhost:8080", timeout=0.05
    )
    if latency > 0:
        with pytest.raises(requests.exceptions.ReadTimeout):
            some_repository_interface.identify()
    else:
        assert some_repository_interface.identify() == expected_response_dict

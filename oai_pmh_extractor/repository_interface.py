"""
The RepositoryInterface handles all communication with a repository server.

This repository's configuration (base url for harvesting) is passed
over on instantiation. Objects of this class are used by the
ExtractionManager class defined in `extraction_manager.py`.
"""

from typing import Optional

import requests
import xmltodict
from dcm_common import LoggingContext as Context, Logger
from dcm_common.util import NestedDict, value_from_dict_path

from oai_pmh_extractor.oaipmh_record import OAIPMHRecord


class RepositoryInterface():
    """
    Interface for the communication between ExtractionManager and
    OAI-PMH-repository server.

    Keyword arguments:
    base_url -- base url of repository for metadata harvesting
    timeout -- timeout duration for remote repository in seconds; None
               indicates not timing out
               (default 10)
    """

    # define available arguments for selective harvest
    OAI_PMH_ARGUMENTS = {
        "metadata_prefix": "metadataPrefix",
        "from": "from",
        "until": "until",
        "set": "set",
        "resumption_token": "resumptionToken"
    }

    def __init__(
        self,
        base_url: str,
        timeout: Optional[float] = 10
    ) -> None:
        self._base_url = base_url
        self._timeout = timeout

        self.preserve_log = False
        self.log = Logger(default_origin="OAI Repository Interface")

    def _build_request(self, **kwargs: str) -> str:
        """
        Build server request as string.

        Returns url consisting of the base server url followed by `?`
        and a list of the provided keyword arguments in the format
        `key=value` separated by ampersands.
        """

        if len(kwargs) == 0:
            return self._base_url

        # build url base (add character '?' if missing)
        request_url = self._base_url
        if self._base_url[-1] != "?":
            request_url += "?"

        stringified_options = []
        for option, value in kwargs.items():
            stringified_options.append(option + "=" + value)
        return request_url + "&".join(stringified_options)

    def _execute_http_request(self, request_url: str) -> str:
        """
        Execute http-request for request_url.

        Return response as utf-8-encoded string.
        """

        response = requests.get(request_url, timeout=self._timeout)
        response.raise_for_status()
        return response.content.decode("utf-8")

    def _check_for_oaipmh_errors(self, response: NestedDict) -> bool:
        if value_from_dict_path(response, ["OAI-PMH", "error"]) is not None:
            self.log.log(
                Context.ERROR,
                body=response["OAI-PMH"]["error"]["@code"] + ": " +
                    response["OAI-PMH"]["error"]["#text"]
            )
            return True
        return False

    def identify(self) -> NestedDict:
        """
        Issue repository server with `Identify`-request.

        Returns dictionary build from answer.
        """

        # no error expected here (according to OAI-PMH specification)

        response = self._execute_http_request(
            self._build_request(verb="Identify")
        )
        return xmltodict.parse(response)

    def list_metadata_formats(self) -> list[dict[str, str]]:
        """
        Issue repository server with `ListMetadataFormats`-request.

        Returns a list of metadata formats as dictionaries.
        """

        # clear log
        if not self.preserve_log:
            self.log = Logger(default_origin="OAI Repository Interface")

        # make request
        response = self._execute_http_request(
            self._build_request(verb="ListMetadataFormats")
        )

        # process response
        response_dict = xmltodict.parse(response)

        # check and handle possible errors
        if self._check_for_oaipmh_errors(response_dict):
            return []

        # continue to process response
        list_of_formats = \
            value_from_dict_path(
                response_dict,
                ["OAI-PMH", "ListMetadataFormats", "metadataFormat"]
            )

        # secure response type
        if list_of_formats is None:
            list_of_formats = []
        if not isinstance(list_of_formats, list):
            list_of_formats = [list_of_formats]
        return list_of_formats

    def list_metadata_prefixes(self) -> list[str]:
        """
        Issue repository server with `ListMetadataFormats`-request.

        Returns a list of metadata prefixes as strings.
        """

        # make request
        list_of_formats = self.list_metadata_formats()

        # process response
        list_of_prefixes = []
        for metadata_format in list_of_formats:
            list_of_prefixes.append(metadata_format["metadataPrefix"])
        return list_of_prefixes

    def list_identifiers_exhaustive_multiple_sets(
        self,
        metadata_prefix: str,
        _from: Optional[str] = None,
        _until: Optional[str] = None,
        _set_spec: Optional[list[str]] = None,
        _max_resumption_tokens: Optional[int] = None,
    ) -> list[str]:
        """
        Calls `list_identifiers_exhaustive` repeatedly to retrieve identifiers
        from multiple sets (logical OR).

        Returns a list of unique identifiers as strings.

        Keyword arguments:
        metadata_prefix -- required argument for oai-pmh; only identifiers
                           are listed that can satisfy the given format
        _from -- lower datestamp in daterange for selective harvest
                 (default None)
        _until -- upper datestamp in daterange for selective harvest
                  (default None)
        _set_spec -- list of set memberships; each element as
                     colon-separated list of path in set hierarchy
                     (default None)
        _max_resumption_tokens -- maximum number of processed resumption tokens;
                                  only considered when positive
                                  (default None leads to no restriction)
        """

        identifiers = []
        if _set_spec is not None:
            for _set in _set_spec:
                identifiers_ = self.list_identifiers_exhaustive(
                    metadata_prefix=metadata_prefix,
                    _from=_from,
                    _until=_until,
                    _set_spec=_set,
                    _max_resumption_tokens=_max_resumption_tokens,
                )
                identifiers.extend(identifiers_)

            return list(set(identifiers))
        return self.list_identifiers_exhaustive(
            metadata_prefix=metadata_prefix,
            _from=_from,
            _until=_until,
            _set_spec=_set_spec,
            _max_resumption_tokens=_max_resumption_tokens,
        )

    def list_identifiers_exhaustive(
        self,
        metadata_prefix: str,
        _from: Optional[str] = None,
        _until: Optional[str] = None,
        _set_spec: Optional[str] = None,
        _max_resumption_tokens: Optional[int] = None,
    ) -> list[str]:
        """
        Issue repository server with repeated `ListIdentifiers`-requests
        until no resumption token is returned.

        Returns a list of identifiers as strings.

        Keyword arguments:
        metadata_prefix -- required argument for oai-pmh; only identifiers
                           are listed that can satisfy the given format
        _from -- lower datestamp in daterange for selective harvest
                 (default None)
        _until -- upper datestamp in daterange for selective harvest
                  (default None)
        _set_spec -- colon-separated list of path in set hierarchy
                     (default None)
        _max_resumption_tokens -- maximum number of processed resumption tokens;
                                  only considered when positive
                                  (default None leads to no restriction)
        """

        identifiers = []
        token = None
        tokens_count = 0
        while True:  # iterate until no resumption token is given
            identifiers_, token = self.list_identifiers(
                _metadata_prefix=metadata_prefix,
                _from=_from,
                _until=_until,
                _set_spec=_set_spec,
                _resumption_token=token
            )
            identifiers.extend(identifiers_)
            if token is None:
                break
            tokens_count += 1
            if (
                _max_resumption_tokens is not None
                and tokens_count > _max_resumption_tokens > 0
            ):
                raise OverflowError(
                    "Maximum number of resumption tokens exceeded "
                    + f"({_max_resumption_tokens})."
                )

        return identifiers

    def list_identifiers(
        self,
        _metadata_prefix: Optional[str] = None,
        _from: Optional[str] = None,
        _until: Optional[str] = None,
        _set_spec: Optional[str] = None,
        _resumption_token: Optional[str] = None
    ) -> tuple[list[str], Optional[str]]:
        """
        Issue repository server with `ListIdentifiers`-request.

        Returns a list of identifiers as string and resumption token
        as tuple.

        Keyword arguments:
        _metadata_prefix -- required argument for oai-pmh; only identifiers
                            are listed that can satisfy the given format
                            (default None)
        _from -- lower datestamp in daterange for selective harvest
                 (default None)
        _until -- upper datestamp in daterange for selective harvest
                  (default None)
        _set_spec -- colon-separated list of path in set hierarchy
                     (default None)
        _resumption_token -- resumption token for follow up of previous
                             request
                             (default None)
        """

        # clear log
        if not self.preserve_log and _resumption_token is None:
            self.log = Logger(default_origin="OAI Repository Interface")

        # build options-dict
        options = {}
        # resumption token is an exclusive argument
        if _resumption_token is not None:
            # ignore all arguments but _resumption_token
            options[self.OAI_PMH_ARGUMENTS["resumption_token"]] = \
                _resumption_token
        else:
            if _metadata_prefix is None:
                raise ValueError(
                    "Missing _metadata_prefix for ListIdentifiers-request."
                )
            options[self.OAI_PMH_ARGUMENTS["metadata_prefix"]] = \
                _metadata_prefix
            for option in [
                (self.OAI_PMH_ARGUMENTS["from"], _from),
                (self.OAI_PMH_ARGUMENTS["until"], _until),
                (self.OAI_PMH_ARGUMENTS["set"], _set_spec)
            ]:
                if option[1] is not None:
                    options[option[0]] = option[1]

        # make request
        response = self._execute_http_request(
            self._build_request(verb="ListIdentifiers", **options)
        )

        # process result
        response_dict = \
            xmltodict.parse(response)

        # check and handle possible errors
        if self._check_for_oaipmh_errors(response_dict):
            return [], _resumption_token

        # make list of identifiers and get token
        list_of_identifiers = []
        resumption_token = None
        if "ListIdentifiers" in response_dict["OAI-PMH"]:
            if "header" in response_dict["OAI-PMH"]["ListIdentifiers"]:
                if isinstance(response_dict["OAI-PMH"]["ListIdentifiers"]["header"], list):
                    for header in response_dict["OAI-PMH"]["ListIdentifiers"]["header"]:
                        list_of_identifiers.append(header["identifier"])
                else:
                    list_of_identifiers.append(
                        response_dict["OAI-PMH"]["ListIdentifiers"]["header"]["identifier"]
                    )
            if "resumptionToken" in response_dict["OAI-PMH"]["ListIdentifiers"]:
                if isinstance(response_dict["OAI-PMH"]["ListIdentifiers"]["resumptionToken"], dict):
                    if "#text" in response_dict["OAI-PMH"]["ListIdentifiers"]["resumptionToken"]:
                        resumption_token = \
                            response_dict["OAI-PMH"]["ListIdentifiers"]["resumptionToken"]["#text"]
                else:
                    resumption_token = \
                        response_dict["OAI-PMH"]["ListIdentifiers"]["resumptionToken"]
        return list_of_identifiers, resumption_token

    def list_sets(self, _resumption_token: Optional[str] = None) \
            -> tuple[list[NestedDict], Optional[str]]:
        """
        Issue repository server with `ListSets`-request.

        Returns a tuple of
        * a list of sets as dictionaries which each take the form of
          {'setSpec': <colon-separated-list>, 'setName': <name-string>, ..}
        * and a resumption token (if available, otherwise None).

        Keyword arguments:
        _resumption_token -- resumption token for follow up of previous
                             request
                             (default None)
        """

        # clear log
        if not self.preserve_log and _resumption_token is None:
            self.log = Logger(default_origin="OAI Repository Interface")

        # build options
        options = {}
        if _resumption_token is not None:
            options[self.OAI_PMH_ARGUMENTS["resumption_token"]] = _resumption_token

        # make request
        response = self._execute_http_request(
            self._build_request(verb="ListSets", **options)
        )

        # process response
        response_dict = \
            xmltodict.parse(response)

        # check and handle possible errors
        if self._check_for_oaipmh_errors(response_dict):
            return [], _resumption_token

        # continue processing response
        list_of_sets = value_from_dict_path(response_dict, ["OAI-PMH", "ListSets", "set"])
        if list_of_sets is None:
            list_of_sets = []
        elif not isinstance(list_of_sets, list):
            list_of_sets = [list_of_sets]
        resumption_token = value_from_dict_path(
            response_dict, ["OAI-PMH", "ListSets", "resumptionToken", "#text"]
        ) or value_from_dict_path(
            response_dict, ["OAI-PMH", "ListSets", "resumptionToken"]
        )

        return list_of_sets, resumption_token

    def get_record(self, metadata_prefix: str, identifier: str) \
            -> Optional[OAIPMHRecord]:
        """
        Issue repository server with `GetRecord`-request.

        Returns OAIPMHRecord-object corresponding to record on success.
        Returns None if an OAI-PMH-error occurred.

        Keyword arguments:
        metadata_prefix -- specify metadata-format ("oai_dc", ..)
        identifier -- repository record identifier string
        """

        # clear log
        if not self.preserve_log:
            self.log = Logger(default_origin="OAI Repository Interface")

        # make request
        response = self._execute_http_request(
            self._build_request(
                verb="GetRecord",
                metadataPrefix=metadata_prefix,
                identifier=identifier
            )
        )

        # check and handle possible errors
        if self._check_for_oaipmh_errors(xmltodict.parse(response)):
            return None

        # process response
        response_dict = \
            xmltodict.parse(response)["OAI-PMH"]["GetRecord"]["record"]

        status = ""
        if "header" in response_dict \
                and "@status" in response_dict["header"]:
            status = response_dict["header"]["@status"]
            self.log.log(
                Context.WARNING,
                body=f"Record {identifier} has status {status}."
            )
        return OAIPMHRecord(
            identifier=identifier,
            status=status,
            metadata_prefix=metadata_prefix,
            metadata_raw=response,
        )

    # list_records is implemented using a combination of `ListIdentifiers`-
    # and `GetRecord`-requests. Note that the harvest is not a time-critical
    # operation. A use of the `ListRecords`-verb furthermore complicates the
    # separation of individual source-metadata records as those are returned
    # in a single string which then requires additional processing.
    def list_records(
        self,
        metadata_prefix: str,
        _from: Optional[str] = None,
        _until: Optional[str] = None,
        _set_spec: Optional[str] = None,
        _resumption_token: Optional[str] = None,
    ) -> tuple[list[OAIPMHRecord], Optional[str]]:
        """
        Issue repository server with `ListIdentifiers`-request followed
        by a series of `GetRecord`-requests.

        Returns list of OAIPMHRecord-objects corresponding to records
        and resumption token as tuple.

        If a single GetRecord returns an error, the process is aborted
        and an empty list along with the previous _resumption_token is
        returned.

        Keyword arguments:
        metadata_prefix -- required argument for oai-pmh; only identifiers
                           are processed that can satisfy the given format
        _from -- lower datestamp in daterange for selective harvest
                 (default None)
        _until -- upper datestamp in daterange for selective harvest
                  (default None)
        _set_spec -- colon-separated list of path in set hierarchy
                     (default None)
        _resumption_token -- resumption token for follow up of previous
                             request
                             (default None)
        """

        # clear log
        if not self.preserve_log:
            self.log = Logger(default_origin="OAI Repository Interface")

        # get list of identifiers and, if available, resumption token
        list_of_identifiers, resumption_token = self.list_identifiers(
            _metadata_prefix=metadata_prefix,
            _from=_from,
            _until=_until,
            _set_spec=_set_spec,
            _resumption_token=_resumption_token
        )

        # make list of OAIPMHRecords
        list_of_records = []
        for identifier in list_of_identifiers:
            record = self.get_record(
                metadata_prefix=metadata_prefix,
                identifier=identifier
            )
            # handle error (failing single record means the request
            # cannot be fulfilled)
            if record is None:
                return [], _resumption_token

            list_of_records.append(
                record
            )

        return list_of_records, resumption_token

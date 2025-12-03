"""
The PayloadCollector can be used to extract payload transfer urls from a
metadata dictionary and download to the local filesystem. Objects of
this class are used by the ExtractionManager class defined in
`extraction_manager.py`.

The extraction of transfer urls is based on a callable filter function
acting on the raw source metadata. The class TransferUrlFilters offers a
collection of filter factories:
* filter_by_regex
* filter_by_regex_in_xml_path
"""

from typing import Optional, Callable
import sys
from pathlib import Path
from urllib import request, parse
from time import sleep

from dcm_common.util import value_from_dict_path
from dcm_common import LoggingContext as Context, Logger

from oai_pmh_extractor.oaipmh_record import OAIPMHRecord


class TransferUrlFilters:
    """Collection of transfer url filter factories."""

    @staticmethod
    def filter_by_regex(regex: str) -> Callable[[Optional[str]], list[str]]:
        """
        Returns filter based on regex acting on entire source metadata
        xml.

        Keyword arguments:
        regex -- regex for filtering
        """

        from re import findall

        def _(source_metadata: Optional[str]) -> list[str]:
            if source_metadata is None:
                return []
            urls = []
            matches = findall(regex, source_metadata)
            if matches is not None:
                # reject empty matches
                urls.extend(list(filter(None, matches)))
            return urls

        return _

    @staticmethod
    def filter_by_regex_in_xml_path(
        regex: str, xml_path: list[str]
    ) -> Callable[[Optional[str]], list[str]]:
        """
        Returns filter based on regex acting on data at given path in
        source metadata xml.

        Keyword arguments:
        regex -- regex for filtering
        xml_path -- path in xml relative to root
        """

        from re import findall

        import xmltodict

        def _(source_metadata: Optional[str]) -> list[str]:
            if source_metadata is None:
                return []
            urls = []
            metadata_fields = value_from_dict_path(
                xmltodict.parse(source_metadata),
                xml_path
            )
            if metadata_fields is not None:
                if isinstance(metadata_fields, str):
                    metadata_fields = [metadata_fields]
                for x in metadata_fields:
                    if x is not None:
                        matches = findall(regex, x)
                        if matches is not None:
                            # reject empty matches
                            urls.extend(list(filter(None, matches)))
            return urls

        return _

    @staticmethod
    def filter_by_regex_with_xpath_query(
        regex: str,
        path: str,
    ) -> Callable[[Optional[str]], list[str]]:
        """
        Returns filter based on regex acting on data
        at given xpath (`path` kwarg) in source metadata xml.

        Keyword arguments:
        regex -- regex for filtering
        path -- xpath query
        """

        from re import findall
        import xml.etree.ElementTree as ET
        from io import BytesIO

        from lxml import etree

        def _(source_metadata: Optional[str]) -> list[str]:
            if source_metadata is None:
                return []
            urls = []

            # Get the namespaces
            context = etree.parse(BytesIO(str.encode(source_metadata)))
            nsmap = {}
            for ns in context.xpath("//namespace::*"):
                if ns[0] not in nsmap:
                    nsmap["" if ns[0] is None else ns[0]] = ns[1]

            # use xml library with xpath_query
            # (lxml supports no empty namespace prefix in XPath)
            metadata_elements = ET.ElementTree(
                ET.fromstring(source_metadata)
            ).findall(path, nsmap)

            for x in metadata_elements:
                if x.text is not None:
                    matches = findall(regex, x.text)
                    if matches is not None:
                        # reject empty matches
                        urls.extend(list(filter(None, matches)))
            return urls

        return _


class PayloadCollector():
    """
    The PayloadCollector can be used to extract payload transfer urls from
    a metadata dictionary and download to the local filesystem.

    Keyword arguments:
    transfer_url_filter -- callable taking source metadata
                           and returning list of transfer urls;
                           mutually exclusive with `transfer_url_filters`
    timeout -- timeout duration for remote repository in seconds; None
               indicates not timing out
               (default 10)
    transfer_url_filters -- list of callables taking source metadata
                            and returning list of transfer urls
    max_retries -- maximum number of retries for downloading a file
                   (default 1)
    retry_interval -- interval between retries in seconds
                      (default 1)
    retry_on_http_status -- http status codes for which retries should be made
                            (default None, uses: [429, 503])
    """

    def __init__(
        self,
        transfer_url_filter: Optional[
            Callable[[Optional[str]], list[str]]
        ] = None,
        timeout: Optional[float] = 10,
        transfer_url_filters: Optional[
            list[Callable[[Optional[str]], list[str]]]
        ] = None,
        max_retries: int = 1,
        retry_interval: float = 1.0,
        retry_on_http_status: Optional[list[int]] = None,
    ) -> None:
        exclusive_kwargs = [transfer_url_filters, transfer_url_filter]
        if all(exclusive_kwargs) or not any(exclusive_kwargs):
            raise TypeError(
                "Cannot instantiate 'PayloadCollector' with given args. "
                + "Exactly one of the kwargs 'transfer_url_filters' and "
                + "'transfer_url_filter' has to be specified."
            )
        if transfer_url_filters is not None:
            self._transfer_url_filters = transfer_url_filters
        if transfer_url_filter is not None:
            self._transfer_url_filters = [transfer_url_filter]
        self._timeout = timeout
        self.max_retries = max_retries
        self.retry_interval = retry_interval
        self.retry_on_http_status = (
            [429, 503]
            if retry_on_http_status is None
            else retry_on_http_status
        )
        self.log: Logger = Logger(default_origin="Payload Collector")

    def download_file(
        self,
        path: Path,
        url: str,
        _filename: Optional[Path] = None
    ) -> Path:
        """
        Download payload file to local file system.

        Keyword arguments:
        path -- output path to directory in local filesystem
        url -- file transfer url
        _filename -- optional filename override
                     (default None)
        """

        exc_info = None
        for retry in range(self.max_retries + 1):
            try:
                with request.urlopen(url, timeout=self._timeout) as response:
                    filename = _filename
                    # check for filename provided by header
                    if filename is None:
                        filename_from_info = response.info().get_filename()
                        if filename_from_info is not None:
                            filename = Path(filename_from_info)
                    # check for filename in url
                    if filename is None:
                        filename = Path(
                            Path(
                                parse.unquote(
                                    parse.urlparse(response.url).path
                                )
                            ).name
                        )

                    # mypy-hint
                    assert filename is not None

                    # make "sure" nothing is overwritten
                    for i in range(0, 10):
                        if i == 9:
                            msg = (
                                "Cannot find valid filename for requested "
                                + f"file with url '{response.url}'."
                            )
                            self.log.log(Context.ERROR, body=msg)
                            raise FileExistsError(msg)
                        if not (path / filename).is_file():
                            break
                        filename.with_name(
                            filename.stem + f"_{i}" + path.suffix
                        )

                    # write file
                    (path / filename).write_bytes(response.read())
                    return path / filename
            except request.HTTPError as _exc_info:
                msg = (
                    "PayloadCollector encountered an error while requesting "
                    + f"'{url}' (downloading to '{path}'): {_exc_info}"
                )
                self.log.log(Context.ERROR, body=msg)
                print(msg, file=sys.stderr)
                exc_info = _exc_info
                if exc_info.code not in self.retry_on_http_status:
                    break
                if retry < self.max_retries:
                    sleep(self.retry_interval)
            except request.URLError as _exc_info:
                msg = (
                    "PayloadCollector encountered an error while requesting "
                    + f"'{url}' (downloading to '{path}'): {_exc_info}"
                )
                self.log.log(Context.ERROR, body=msg)
                print(msg, file=sys.stderr)
                exc_info = _exc_info
                if retry < self.max_retries:
                    sleep(self.retry_interval)
        raise exc_info

    def download_record_payload(
        self,
        record: OAIPMHRecord,
        path: Optional[Path] = None,
        renew_urls: bool = False,
        skip_download: bool = False
    ) -> None:
        """
        Download the payload associated with an OAIPMHRecord.

        If record.files is empty, generate urls from metadata first and
        store in record-object.

        Keyword arguments:
        record -- OAIPMHRecord to be processed
        path -- output path to directory in local filesystem
                required if skip_download=False
                (default None)
        renew_urls -- renew the transfer urls even if files are already
                      listed in record
                      (default False)
        skip_download -- if false, skip downloading (can be used to only
                         generate list of urls for download)
                         (default False)
        """

        # renew file-index if either requested or there is an empty
        # list/None in record.files
        if renew_urls or not record.files:
            record.files = []
            urls = []
            for idx, transfer_url_filter in enumerate(
                self._transfer_url_filters
            ):
                try:
                    urls += transfer_url_filter(record.metadata_raw)
                except SyntaxError as exc_info:
                    if "not found in prefix map" in str(exc_info):
                        self.log.log(
                            Context.ERROR,
                            body=(
                                f"Failed to generate url with filter {idx}. "
                                + "XPath contains unknown namespace: "
                                + f"{exc_info}."
                            )
                        )
                    else:
                        raise exc_info

            # remove duplicate urls
            url_set = set(urls)

            if len(url_set) > 0:
                self.log.log(
                    Context.INFO,
                    body=(
                        f"Filter on '{record.identifier}'-metadata returned "
                        + "the following urls: "
                        + ", ".join(map(lambda s: f"'{s.strip()}'", url_set))
                    )
                )
            else:
                self.log.log(
                    Context.WARNING,
                    body=(
                        "Got no payload-urls from metadata of "
                        + f"'{record.identifier}'."
                    )
                )

            record.register_files_by_url(url_set)

        # exit if skipped download
        if skip_download:
            return None

        # path is required if download is not skipped
        if path is None:
            msg = "Missing expected filesystem path as argument in " \
                + "call to download_record_payload."
            self.log.log(
                Context.ERROR,
                body=msg
            )
            raise TypeError(msg)

        # iterate files
        for file in record.files:
            try:
                file["path"] = self.download_file(
                    path=path,
                    url=file["url"]
                )
                file["complete"] = True
            except (KeyError, FileNotFoundError) as exc_info:
                self.log.log(
                    Context.ERROR,
                    body=f"Download failed: {exc_info}."
                )

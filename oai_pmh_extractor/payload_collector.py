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
from pathlib import Path
from urllib import request, parse

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
                urls.extend(matches)
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
                            urls.extend(matches)
            return urls

        return _


class PayloadCollector():
    """
    The PayloadCollector can be used to extract payload transfer urls from
    a metadata dictionary and download to the local filesystem.

    Keyword arguments:
    transfer_url_filter -- callable taking source metadata and returning
                           list of transfer urls
    timeout -- timeout duration for remote repository in seconds; None
               indicates not timing out
               (default 10)
    """

    def __init__(
        self,
        transfer_url_filter: Callable[[Optional[str]], list[str]],
        timeout: Optional[float] = 10
    ) -> None:
        self._transfer_url_filter = transfer_url_filter
        self._timeout = timeout
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

        # make request (raises URLError if not successful)
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
                        parse.unquote(parse.urlparse(response.url).path)
                    ).name
                )

            # mypy-hint
            assert filename is not None

            # make "sure" nothing is overwritten
            for i in range(0, 10):
                if i == 9:
                    msg = "Cannot find valid filename for requested file with " \
                        + f"url '{response.url}'."
                    self.log.log(
                        Context.ERROR,
                        body=msg
                    )
                    raise FileExistsError(msg)
                if not (path / filename).is_file():
                    break
                filename.with_name(filename.stem + f"_{i}" + path.suffix)

            # write file
            (path / filename).write_bytes(response.read())
            return path / filename

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
            record.register_files_by_url(
                self._transfer_url_filter(record.metadata_raw)
            )

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

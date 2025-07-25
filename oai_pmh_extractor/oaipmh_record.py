"""
This module contains the definition of a record-class storing information
regarding a single OAIPMH-item (identifier, metadata, ..).
"""

from typing import TypedDict, Optional, Iterable
from hashlib import md5
from pathlib import Path


class File(TypedDict):
    """
    Files are represented by a collection properties: remote url, local
    path, and a complete/loaded property. Files are considered identical
    if file1["identifier"] == file2["identifier"].
    """
    identifier: str
    url: str
    path: Optional[Path]
    complete: bool


class OAIPMHRecord():
    """
    Record-class storing information regarding a single OAIPMH-item.

    Keyword arguments:
    identifier -- external identifier for record
    status -- status of record (e.g. "", "deleted", "modified")
              (default "")
    metadata_prefix -- oai-pmh metadataPrefix
                       (default None)
    metadata_raw -- metadata raw str
                    (default None)
    file_urls -- list of urls to payload files; this can be used to initialize
                 a list of File-objects with urls at instantiation
                 (default None)
    """

    def __init__(
        self,
        identifier: str,
        status: str = "",
        metadata_prefix: Optional[str] = None,
        metadata_raw: Optional[str] = None,
        file_urls: Optional[list[str]] = None
    ) -> None:
        self._identifier = identifier
        self._identifier_hash: str = md5(
                self._identifier.encode(encoding="utf-8")
            ).hexdigest()
        self._path: Optional[Path] = None
        self._status = status
        self._metadata_raw = metadata_raw
        self._metadata_prefix = metadata_prefix
        self._files: list[File] = []
        if file_urls is not None:
            self.register_files_by_url(file_urls)
        self._complete = False

    def register_files_by_url(self, file_urls: Iterable[str]) -> None:
        """
        Generate templates for File-dictionaries based on list of urls.

        Keyword arguments:
        file_urls -- list of urls
        """
        for url in file_urls:
            self._files.append(
                {
                    "identifier": url,
                    "url": url,
                    "path": None,
                    "complete": False
                }
            )

    @property
    def identifier(self) -> str:
        """OAIPMHRecord identifier property."""
        return self._identifier

    @identifier.setter
    def identifier(self, value: str) -> None:
        self._identifier = value

    @property
    def identifier_hash(self) -> str:
        """OAIPMHRecord identifier_hash property."""
        return self._identifier_hash

    @property
    def path(self) -> Optional[Path]:
        """OAIPMHRecord path property."""
        return self._path

    @path.setter
    def path(self, value: Path) -> None:
        self._path = value

    @property
    def status(self) -> str:
        """OAIPMHRecord status property."""
        return self._status

    @status.setter
    def status(self, value: str) -> None:
        self._status = value

    @property
    def metadata_prefix(self) -> Optional[str]:
        """OAIPMHRecord metadata_prefix property."""
        return self._metadata_prefix

    @metadata_prefix.setter
    def metadata_prefix(self, value: str) -> None:
        self._metadata_prefix = value

    @property
    def metadata_raw(self) -> Optional[str]:
        """OAIPMHRecord metadata_raw property."""
        return self._metadata_raw

    @metadata_raw.setter
    def metadata_raw(self, value: str) -> None:
        self._metadata_raw = value

    @property
    def files(self) -> list[File]:
        """OAIPMHRecord files property."""
        return self._files

    @files.setter
    def files(self, value: list[File]) -> None:
        self._files = value

    @property
    def complete(self) -> bool:
        """OAIPMHRecord complete property."""
        return self._complete

    @complete.setter
    def complete(self, value: bool) -> None:
        self._complete = value

    def add_file(self, file: File) -> list[File]:
        """
        Add file to list of files in record.

        Keyword arguments:
        file -- file represented by TypedDict-compliant dictionary to be
                added to list of associated files
        """

        self._files.append(file)
        return self._files

    def remove_file(self, file: File) -> list[File]:
        """
        Remove file from list of files in record.

        Keyword arguments:
        file -- file represented by TypedDict-compliant dictionary to be
                removed from list of associated files
        """

        self._files = [
            x for x in self._files
              if x["identifier"] != file["identifier"]
        ]
        return self._files

"""
pyexcel.plugins.sources.http
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Representation of http sources

:copyright: (c) 2014-2026 by C Wang
:license: New BSD License
"""

from urllib.parse import urlparse

from pyexcel import constants
from pyexcel.source import AbstractSource
from pyexcel._compact import request
from pyexcel.internal import PARSER

from . import params
from .resume import DownloadProgress, download_resumable

XLSX = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"

FILE_TYPE_MIME_TABLE = {
    "text/csv": "csv",
    "text/tab-separated-values": "tsv",
    "application/vnd.oasis.opendocument.spreadsheet": "ods",
    "application/vnd.ms-excel": "xls",
    XLSX: "xlsx",
    "application/vnd.ms-excel.sheet.macroenabled.12": "xlsm",
    "text/html": "html",
}


# pylint: disable=W0223
class HttpSource(AbstractSource):
    """
    Multiple sheet data source via http protocol
    """

    fields = [params.URL]
    targets = (constants.SHEET, constants.BOOK)
    actions = (constants.READ_ACTION,)
    attributes = [params.URL]
    key = params.URL

    def __init__(self, url=None, **keywords):
        self.__url = url
        self.__resumable = keywords.pop(params.RESUMABLE, False)
        self.__download_progress = keywords.pop(
            params.DOWNLOAD_PROGRESS, None
        )
        self.__download_report = None
        AbstractSource.__init__(self, **keywords)

    def get_data(self):
        if self.__resumable:
            content, info, file_url = self._download_resumable_content()
        else:
            connection = request.urlopen(self.__url)
            info = connection.info()
            file_url = self.__url
            content = connection.read()
        mime_type = info.get_content_type() if info is not None else None
        file_type = FILE_TYPE_MIME_TABLE.get(mime_type, None)
        if file_type is None:
            file_type = _get_file_type_from_url(file_url)
        parser_library = self._keywords.get("parser_library", None)
        aparser = PARSER.get_a_plugin(file_type, parser_library)
        sheets = aparser.parse_file_content(content, **self._keywords)
        return sheets

    def get_source_info(self):
        return self.__url, None

    def get_download_progress(self):
        """Return the :class:`DownloadProgress` used by the last call."""
        return self.__download_progress

    def get_download_report(self):
        """Return the :class:`DownloadReport` of a completed download."""
        return self.__download_report

    def _download_resumable_content(self):
        if self.__download_progress is None:
            self.__download_progress = DownloadProgress()
        content, info, file_url, report = download_resumable(
            self.__url, self.__download_progress
        )
        self.__download_report = report
        return content, info, file_url


def _get_file_type_from_url(url):
    path = urlparse(url).path
    extension = path.split(".")
    return extension[-1]

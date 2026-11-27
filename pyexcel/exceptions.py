"""
pyexcel.exceptions
~~~~~~~~~~~~~~~~~~~

Exceptions appeared in pyexcel

:copyright: (c) 2014-2026 by C Wang
:license: New BSD License
"""


class UnknownParameters(Exception):
    """Unknown parameter(s) were given to the signature functions"""

    pass


class FileTypeNotSupported(Exception):
    """A file type is not supported"""

    pass


class UpgradePlugin(Exception):
    """Please upgrade your plugin"""

    pass


class DownloadError(Exception):
    """Base class for resumable download failures.

    The :class:`pyexcel.plugins.sources.resume.DownloadReport` describing
    what happened is available as :attr:`report`.
    """

    def __init__(self, report):
        self.report = report
        super().__init__(report.message or self.__class__.__name__)


class DownloadInterrupted(DownloadError):
    """The connection ended before the announced response body arrived."""

    pass


class DownloadResponseMismatch(DownloadError):
    """The response headers do not match the resumed file version."""

    pass


class IncompleteDownload(DownloadError):
    """The response ended normally but more bytes are still required."""

    pass


class StaleDownload(DownloadError):
    """The attempt was superseded by a later download in the collection."""

    pass

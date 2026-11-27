"""
pyexcel.internal.core
~~~~~~~~~~~~~~~~~~~~~~

elementary functions to read and write generic excel content

:copyright: (c) 2015-2021 by Onni Software Ltd.
:license: New BSD License
"""

from io import UnsupportedOperation

from pyexcel.internal import SOURCE
from pyexcel.constants import DEFAULT_NO_DATA
from pyexcel.internal.generators import BookStream, SheetStream


def get_sheet_stream(**keywords):
    """
    Get an instance of SheetStream from an excel source
    """
    a_source = SOURCE.get_source(**keywords)
    filename, path = a_source.get_source_info()
    sheets = a_source.get_data()
    if sheets:
        sheet_name, data = _one_sheet_tuple(sheets.items())
        sheet_stream = SheetStream(sheet_name, data)
    else:
        sheet_stream = SheetStream(DEFAULT_NO_DATA, [[]])
    _attach_download(sheet_stream, a_source)
    return sheet_stream


def get_book_stream(**keywords):
    """
    Get an instance of BookStream from an excel source

    Where the dictionary should have text as keys and two dimensional
    array as values.
    """
    a_source = SOURCE.get_book_source(**keywords)
    filename, path = a_source.get_source_info()
    sheets = a_source.get_data()
    book_stream = BookStream(sheets, filename=filename, path=path)
    _attach_download(book_stream, a_source)
    return book_stream


def _attach_download(stream, a_source):
    get_progress = getattr(a_source, "get_download_progress", None)
    if get_progress is None:
        return
    progress = get_progress()
    if progress is None:
        return
    stream.download_progress = progress
    stream.download_report = a_source.get_download_report()


def save_sheet(sheet, **keywords):
    """
    Save a sheet instance to any source
    """
    a_source = SOURCE.get_writable_source(**keywords)
    return _save_any(a_source, sheet)


def save_book(book, **keywords):
    """
    Save a book instance to any source
    """
    a_source = SOURCE.get_writable_book_source(**keywords)
    return _save_any(a_source, book)


def _save_any(a_source, instance):
    a_source.write_data(instance)
    try:
        content_stream = a_source.get_content()
        _seek_at_zero(content_stream)
        return content_stream
    except AttributeError:
        return None


def _seek_at_zero(a_stream):
    try:
        a_stream.seek(0)
    except UnsupportedOperation:
        pass


def _one_sheet_tuple(items):
    items = list(items)
    return items[0][0], items[0][1]

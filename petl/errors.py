from __future__ import absolute_import, print_function, division


class DuplicateKeyError(Exception):

    def __init__(self, key):
        self.key = key

    def __str__(self):
        return 'duplicate key: %r' % (self.key,)


class FieldSelectionError(Exception):

    def __init__(self, value):
        self.value = value

    def __str__(self):
        return 'selection is not a field or valid field index: %r' % (self.value,)


class ArgumentError(Exception):

    def __init__(self, message):
        self.message = message

    def __str__(self):
        return 'argument error: %s' % self.message


class ResumeError(Exception):
    """A resumable source stream could not be continued.

    `sequence` is the sequence number of the row where verification failed
    (or where the stream broke), if known. `reason` is a short description
    of the failure. `position`, `index` and `seq` are aliases of `sequence`
    that are kept for callers that look for a location. `error` is the
    original exception when verification continued a stream that broke.
    """

    def __init__(self, sequence, reason, error=None):
        self.sequence = sequence
        self.position = sequence
        self.index = sequence
        self.seq = sequence
        self.reason = reason
        self.error = error

    def __str__(self):
        return 'resume failed at sequence %r: %s' % (self.sequence,
                                                      self.reason)

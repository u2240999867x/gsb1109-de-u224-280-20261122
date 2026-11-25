class ArchiveError(Exception):
    """Base class for all archive errors."""


class CommitConflictError(ArchiveError):
    """A commit id was re-submitted with different content."""


class UnknownCommitError(ArchiveError):
    """Referenced commit id does not exist in the archive."""


class UnknownLineError(ArchiveError):
    """Referenced business line number does not exist in the archive."""


class UnknownReceiptError(ArchiveError):
    """Referenced receipt id does not exist in the archive."""


class UnknownManifestError(ArchiveError):
    """Referenced manifest id does not exist in the archive."""


class ManifestStateError(ArchiveError):
    """Manifest id is already registered or the manifest is already revoked."""


class ReceiptStateError(ArchiveError):
    """Receipt is already withdrawn from the current ingestion scope."""


class InvalidLaneTypeError(ArchiveError):
    """Lane type declaration is not supported or cannot be applied."""

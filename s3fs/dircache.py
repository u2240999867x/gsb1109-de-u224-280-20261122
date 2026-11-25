"""Coverage metadata for directory listing caches.

A single S3 ``ListObjectsV2`` query can prove very different facts depending
on how it was scoped:

* a full, unpaginated delimiter listing proves the complete set of immediate
  names in a directory, including that an empty result means an empty
  directory;
* a limited sample (``MaxKeys`` / ``max_items``) or an interrupted pagination
  only proves the names that were actually returned;
* a query restricted to a key prefix only proves facts for names sharing that
  prefix, so an empty result only negates the queried prefix;
* a recursive ``find`` restricted by ``maxdepth`` or a stem ``prefix`` only
  covers the requested subtree or stem.

``ListingCoverage`` records, per directory, the relative name prefixes that
have been proven complete by an actual query. The ordinary entry list stays
in the fsspec ``DirCache``; this structure only adds the knowledge of which
absences are actually provable.

Each proven prefix is stored with a ``recursive`` flag: delimiter-based
listings only prove immediate children, while recursive (``Delimiter=""``)
listings prove every descendant below the prefix.
"""


class ListingCoverage:
    """Tracks which name prefixes of each directory are fully known.

    A directory maps to a set of ``(prefix, recursive)`` proofs. An empty
    prefix stands for the whole directory, so once the whole directory is
    known complete there is no need to keep narrower prefixes.
    """

    def __init__(self):
        self._complete = {}

    def mark_complete(self, path, prefix="", recursive=False):
        """Record that every name below ``path`` sharing ``prefix`` is known.

        An empty result still counts as a proof that the queried prefix
        contains nothing. ``recursive`` says whether the query had no
        delimiter and therefore also proved nested descendants.
        """
        specs = self._complete.setdefault(path, set())
        if prefix == "":
            if recursive:
                # The strongest proof subsumes every narrower one, including
                # an earlier delimiter-only whole-directory proof.
                self._complete[path] = {("", True)}
            elif ("", True) not in specs:
                specs.add(("", False))
            return
        if ("", True) in specs:
            # A recursive whole-directory proof covers every descendant.
            return
        if ("", False) in specs and not recursive:
            # A delimiter whole-directory proof already covers the queried
            # immediate name, but not nested descendants of a narrower stem.
            return
        specs.add((prefix, bool(recursive)))

    def is_complete(self, path, prefix="", recursive=False):
        """Return True if the queried scope of ``path`` is fully known.

        A delimiter-scoped query (``recursive=False``) is satisfied by any
        proof whose prefix the query starts with; a recursive query needs a
        recursive proof.
        """
        specs = self._complete.get(path)
        if not specs:
            return False
        for spec_prefix, spec_recursive in specs:
            if prefix.startswith(spec_prefix):
                if not recursive or spec_recursive:
                    return True
        return False

    def proves_absence(self, path, relative):
        """Return True if a missing name is genuinely proven absent.

        ``relative`` is the looked-up name relative to ``path``. A
        delimiter (non-recursive) proof with prefix ``p`` covers names
        starting with ``p`` whose remaining suffix has no further path
        separator; recursive proofs cover every descendant.
        """
        specs = self._complete.get(path)
        if not specs:
            return False
        for spec_prefix, spec_recursive in specs:
            if relative.startswith(spec_prefix):
                suffix = relative[len(spec_prefix) :]
                if spec_recursive or "/" not in suffix:
                    return True
        return False

    def pop(self, path):
        """Forget every coverage fact recorded for ``path``."""
        self._complete.pop(path, None)

    def invalidate_scope(self, path, stem=""):
        """Drop proofs that would answer names under ``stem`` of ``path``.

        Used when a forced refresh re-queries a scope: every earlier proof
        whose prefix the refreshed stem starts with could be stale, so they
        are removed before the fresh query records its own proof.
        """
        specs = self._complete.get(path)
        if not specs:
            return
        self._complete[path] = {
            (prefix, recursive)
            for prefix, recursive in specs
            if not stem.startswith(prefix)
        }
        if not self._complete[path]:
            del self._complete[path]

    def clear(self):
        self._complete.clear()

    def __contains__(self, path):
        return path in self._complete

    def __len__(self):
        return len(self._complete)

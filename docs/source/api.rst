API
===

.. currentmodule:: s3fs.core

.. autosummary::
   S3FileSystem
   S3FileSystem.cat
   S3FileSystem.du
   S3FileSystem.exists
   S3FileSystem.find
   S3FileSystem.get
   S3FileSystem.glob
   S3FileSystem.info
   S3FileSystem.ls
   S3FileSystem.mkdir
   S3FileSystem.mv
   S3FileSystem.open
   S3FileSystem.put
   S3FileSystem.read_block
   S3FileSystem.rm
   S3FileSystem.tail
   S3FileSystem.touch
   S3FileSystem.publish_directory
   S3FileSystem.publication
   S3FileSystem.begin_merge

.. autosummary::
   S3File
   S3File.close
   S3File.flush
   S3File.info
   S3File.read
   S3File.seek
   S3File.tell
   S3File.write

.. currentmodule:: s3fs.mapping

.. autosummary::
   S3Map

.. currentmodule:: s3fs.core

.. autoclass:: S3FileSystem
   :members:
   :inherited-members:

.. autoclass:: S3File
   :members:
   :inherited-members:

.. currentmodule:: s3fs.mapping

.. autofunction:: S3Map

.. currentmodule:: s3fs.utils

.. autoclass:: ParamKwargsHelper

.. autoclass:: SSEParams

.. currentmodule:: s3fs.publication

.. autosummary::
   Publication
   PublicationStore
   PublicationMerger
   MergeDraft
   Decision
   MergeIssue
   MergeConflictError
   PublicationNotFound
   PublicationLayoutError
   publish_directory
   publication_for
   begin_merge
   check_merge
   commit_merge
   merge_directories

.. autoclass:: Publication
   :members:

.. autoclass:: PublicationStore
   :members:

.. autoclass:: PublicationMerger
   :members:

.. autoclass:: MergeDraft
   :members:

.. autofunction:: publish_directory

.. autofunction:: publication_for

.. autofunction:: begin_merge

.. autofunction:: merge_directories

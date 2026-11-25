.. change::
    :tags: feature, runtime

    Added support for sharing a single Alembic version table across several
    independent migration environments.  The new
    :paramref:`.EnvironmentContext.configure.version_table_pipeline` parameter
    adds a ``pipeline`` discriminator column to the version table (using a
    composite ``(pipeline, version_num)`` primary key), and scopes all reads
    and writes of version rows to the configured identifier, so that e.g. a
    pre-built table can track multiple pipelines side by side.

.. change::
    :tags: bug, environment

    The module-level proxies behind ``alembic.context`` and ``alembic.op``
    now save and restore their previous target when environments are nested,
    so one migration environment may invoke another environment's
    ``upgrade`` within the same connection and transaction (for example via
    ``config.attributes["connection"]``); when the inner environment
    finishes or fails, the proxies once again point at the outer
    environment.  :meth:`.Operations.context` now also removes its proxy
    when the block exits with an exception.

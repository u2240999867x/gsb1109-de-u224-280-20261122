================================
Security and File Storage Types
================================

Advanced Alchemy includes column types for symmetric field encryption, password hashing, TOTP seeds, one-time verification codes, and transaction-synchronized object storage.

Encrypted Text and Password Hashing
-----------------------------------

Use ``EncryptedString`` or ``EncryptedText`` when your app needs to decrypt a secret later (like an API token), and ``PasswordHash`` when storing one-way user passwords:

.. tab-set::

    .. tab-item:: Encrypted Columns

        .. code-block:: python

            from sqlalchemy.orm import Mapped, mapped_column
            from advanced_alchemy.base import BigIntBase
            from advanced_alchemy.types import EncryptedString, EncryptedText


            class SecretRecord(BigIntBase):
                __tablename__ = "secret_record"

                api_token: Mapped[str] = mapped_column(EncryptedString(key="my-secret-key"))
                private_notes: Mapped[str] = mapped_column(EncryptedText(key="my-secret-key"))

    .. tab-item:: Password Hashing

        .. code-block:: python

            from sqlalchemy.orm import Mapped, mapped_column
            from advanced_alchemy.base import BigIntBase
            from advanced_alchemy.types import HashedPassword, PasswordHash
            from advanced_alchemy.types.password_hash.argon2 import Argon2Hasher

            hasher = Argon2Hasher()


            class AccountRecord(BigIntBase):
                __tablename__ = "account_record"

                password: Mapped[str] = mapped_column(PasswordHash(backend=hasher))


            stored = HashedPassword(hasher.hash("s3cret-passphrase"), hasher)
            verified, _ = stored.verify_and_update("s3cret-passphrase")
            assert verified is True

Two-Factor Secrets and One-Time Codes
-------------------------------------

For multi-factor login flows, ``TOTPSecret`` encrypts authenticator app seeds (Google Authenticator, 1Password), while ``OneTimeCode`` hashes short-lived email or SMS verification codes and tracks retry limits:

.. tab-set::

    .. tab-item:: Authenticator App (TOTP)

        .. code-block:: python

            from sqlalchemy.orm import Mapped, mapped_column
            from advanced_alchemy.base import BigIntBase
            from advanced_alchemy.types import TOTPProvider, TOTPSecret, generate_totp_secret


            class MfaEnrollment(BigIntBase):
                __tablename__ = "mfa_enrollment"

                seed: Mapped[str] = mapped_column(TOTPSecret(key="my-secret-key", issuer="ACME"))


            provider = TOTPProvider(generate_totp_secret(), issuer="ACME")
            uri = provider.provisioning_uri(name="alice@example.com")
            assert uri.startswith("otpauth://totp/")

    .. tab-item:: Email / SMS One-Time Code

        .. code-block:: python

            from typing import Any
            from sqlalchemy.orm import Mapped, mapped_column
            from advanced_alchemy.base import BigIntBase
            from advanced_alchemy.types import HashedOneTimeCode, OneTimeCode, generate_one_time_code
            from advanced_alchemy.types.password_hash.argon2 import Argon2Hasher

            otp_hasher = Argon2Hasher()


            class LoginCode(BigIntBase):
                __tablename__ = "login_code"

                code: Mapped[Any] = mapped_column(
                    OneTimeCode(backend=otp_hasher, ttl_seconds=600, max_attempts=3),
                )


            raw_code = generate_one_time_code()
            otp = HashedOneTimeCode(otp_hasher.hash(raw_code), otp_hasher, max_attempts=3)
            ok, otp = otp.redeem(raw_code)
            assert ok is True and otp.is_used is True

File and Object Storage
-----------------------

Instead of storing binary payloads inside SQL columns, ``StoredObject`` persists file metadata in the table and writes object bytes to local disk, S3, Google Cloud Storage, or Azure Blob Storage through ``storages``:

.. code-block:: python

    from pathlib import Path
    import tempfile
    from typing import Optional
    from sqlalchemy.orm import Mapped, mapped_column
    from advanced_alchemy.base import UUIDBase
    from advanced_alchemy.types import FileObject, FileObjectList, StoredObject, storages

    tmp_storage = tempfile.mkdtemp()
    storages.register_backend(f"file://{Path(tmp_storage)}", key="documents")


    class DocumentRecord(UUIDBase):
        __tablename__ = "document_record"

        attachment: Mapped[Optional[FileObject]] = mapped_column(
            StoredObject(backend="documents"),
            nullable=True,
        )
        images: Mapped[Optional[FileObjectList]] = mapped_column(
            StoredObject(backend="documents", multiple=True),
            nullable=True,
        )


    file_obj = FileObject(backend="documents", filename="report.txt", metadata={"category": "report"})
    file_obj.update_metadata({"priority": "high"})
    storages.clear_backends()

See :doc:`/usage/frameworks/litestar_storage` and :doc:`/usage/frameworks/fastapi_storage` for complete file-upload web examples.

Shared retention
~~~~~~~~~~~~~~~~

When several rows (or several fields) reference the same stored object, detaching one of them would normally delete the bytes out from under the others. Opt a field into shared retention with ``shared=True`` to coordinate cleanup through the process-wide ``shared_objects`` registry:

.. code-block:: python

    from typing import Optional
    from sqlalchemy.orm import Mapped, mapped_column
    from advanced_alchemy.base import UUIDBase
    from advanced_alchemy.types import FileObject, FileObjectList, StoredObject


    class ReworkOrder(UUIDBase):
        __tablename__ = "rework_order"

        attachment: Mapped[Optional[FileObject]] = mapped_column(
            StoredObject(backend="documents", shared=True),
            nullable=True,
        )


    class QualityOrder(UUIDBase):
        __tablename__ = "quality_order"

        attachments: Mapped[Optional[FileObjectList]] = mapped_column(
            StoredObject(backend="documents", multiple=True, shared=True),
            nullable=True,
        )

Both single (``multiple=False``) and list (``multiple=True``) fields are supported. With shared retention:

- References are acquired at flush time and released after commit; the object is only deleted once the last reference is released.
- Attaching an object whose delete is already being handed to the storage backend fails the transaction with :class:`SharedObjectUnavailableError <advanced_alchemy.exceptions.SharedObjectUnavailableError>` instead of committing an unreadable attachment.
- Every save of new bytes to a tracked path starts a new generation, so a stale or replayed cleanup request never deletes newer bytes stored at the same path.
- Tracking is keyed per storage backend, so identical paths in different backends are cleaned up independently.

The registry is process-local; use it for fields whose cleanup must be coordinated within a single application process.


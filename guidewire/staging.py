"""Staging executor for Guidewire CDA Unity Catalog governance.

Why this exists
---------------

Most Guidewire CDA SaaS deployments place source parquet files in an S3
bucket the customer cannot register as a Unity Catalog external location:
the bucket is owned by Guidewire's AWS account, and the customer holds only
a cross-account read-only IAM grant. UC governance, by contrast, requires
the target Delta table's data to be enclosed under a UC external location
the customer controls (so vended credentials can govern reads).

This module bridges the two by copying source parquets into a customer-owned
target prefix that *can* be a UC external location. After staging, the
accelerator writes the Delta log against the staged target -- AddAction paths
become relative to the table root, reads stay inside the UC perimeter, and
``system.access.audit`` attributes them to the UC principal.

Design contract
---------------

- ``StagingExecutor`` is opt-in. When a ``Batch`` is constructed without one,
  behavior is bit-identical to the legacy upstream output (absolute s3a://
  AddAction paths, no copy step, no UC involvement).
- Reads use the source's :class:`AWSStorage` filesystem (existing
  ``AWS_SOURCE_*`` env vars). Writes use the target's
  :class:`UCStorage` filesystem (vended UC creds for the target table).
  The copy streams bytes between the two filesystems; no third party (and
  in particular, no static AWS keys with cross-bucket access) sees the data.
- Idempotency: each per-file copy first checks ``head_object`` on the target.
  If the target already exists with a matching size, the copy is skipped.
  This makes re-runs cheap and crash-safe.
- Atomicity: the existing accelerator already commits one Delta transaction
  per timestamp folder. Staging happens *within* that transaction's
  ``_get_parquet_list`` call, so a failed copy aborts the transaction
  before the watermark advances. Retry resumes naturally.

v1 limitations
--------------

- AWS source -> AWS UC target only. Cross-cloud (Azure CDA -> AWS UC, etc.)
  is rejected with ``NotImplementedError``.
- Serial copy within a table. Cross-table parallelism is unchanged (Ray).
- Bytes are streamed; no boto3 runtime dependency is added.
"""

from typing import Optional
from pyarrow.fs import FileType
from guidewire.logging import logger as L
from guidewire.storage import BaseStorage, AWSStorage, UCStorage


class StagingExecutor:
    """Copies CDA parquets from a source filesystem into a UC-governed target.

    Args:
        source_storage: Filesystem authenticated for reading the source CDA
            bucket (typically ``AWSStorage(prefix="SOURCE")``).
        target_storage: Filesystem authenticated for writing into the target
            UC-governed bucket (typically a :class:`UCStorage` instance with
            ``operation="READ_WRITE"``).
        target_table_root: Bucket-relative prefix that encloses the staged
            data (e.g. ``"customer-bucket/staged/cc_claim"``). Files are
            staged at ``target_table_root/<schema_hash>/<timestamp>/*.parquet``.
            This must match the AddAction table root for the relative-path
            log to resolve correctly under UC.
        source_table_root: Bucket-relative prefix where source files currently
            live (e.g. ``"guidewire-bucket/cc_claim"``). The staged path of
            each file is computed as ``target_table_root + (file.path - source_table_root)``.
        chunk_size: Streaming chunk size for the cross-filesystem copy
            (default 8 MB).

    Raises:
        ValueError: If ``target_storage`` is not a :class:`UCStorage`. Staging
            into a non-UC target makes no sense -- the whole point is to put
            data under UC governance.
        NotImplementedError: If a non-AWS source is passed (cross-cloud staging
            is out of scope for v1).
    """

    DEFAULT_CHUNK_SIZE = 8 * 1024 * 1024  # 8 MB

    def __init__(
        self,
        source_storage: BaseStorage,
        target_storage: BaseStorage,
        target_table_root: str,
        source_table_root: str,
        chunk_size: int = DEFAULT_CHUNK_SIZE,
    ):
        if not isinstance(target_storage, UCStorage):
            raise ValueError(
                "StagingExecutor requires target_storage to be a UCStorage instance "
                "(staging only makes sense when the target is UC-governed)."
            )
        if not isinstance(source_storage, AWSStorage):
            raise NotImplementedError(
                "Cross-cloud staging is not supported in v1. "
                f"Got source_storage={type(source_storage).__name__}; expected AWSStorage."
            )
        if not target_table_root or not source_table_root:
            raise ValueError("target_table_root and source_table_root must be non-empty.")

        self.source = source_storage
        self.target = target_storage
        # Normalize: bucket-relative, no leading/trailing slash.
        self._target_root = self._strip_scheme(target_table_root).rstrip("/")
        self._source_root = self._strip_scheme(source_table_root).rstrip("/")
        self._chunk_size = chunk_size

    def stage_file(self, source_path: str, expected_size: int) -> str:
        """Copy one source file to the corresponding target location.

        Args:
            source_path: Bucket-relative source path as returned by PyArrow's
                S3 filesystem (e.g.
                ``"guidewire-bucket/cc_claim/abc123/20260101T000000Z/part-0.parquet"``).
            expected_size: Source file size in bytes. Used for the
                idempotency check on the target side.

        Returns:
            The bucket-relative target path the file was staged to (e.g.
            ``"customer-bucket/staged/cc_claim/abc123/20260101T000000Z/part-0.parquet"``).

        Raises:
            ValueError: If ``source_path`` is not enclosed by the configured
                ``source_table_root`` -- this would mean the manifest is
                inconsistent with the staging configuration.
        """
        target_path = self._map_path(source_path)
        if self._already_staged(target_path, expected_size):
            L.debug(f"Skip already-staged: {target_path}")
            return target_path

        L.debug(f"Staging {source_path} -> {target_path}")
        with self.source.filesystem.open_input_stream(source_path) as src:
            with self.target.filesystem.open_output_stream(target_path) as dst:
                while True:
                    chunk = src.read(self._chunk_size)
                    if not chunk:
                        break
                    dst.write(chunk)
        return target_path

    def _map_path(self, source_path: str) -> str:
        """Translate a source bucket-relative path to its target location.

        The source root is stripped and the result is appended to the target
        root, preserving the per-table CDA layout
        (``<schema_hash>/<timestamp>/*.parquet``).
        """
        normalized = self._strip_scheme(source_path)
        if not normalized.startswith(self._source_root):
            raise ValueError(
                f"Source file {source_path!r} is not enclosed by source_table_root "
                f"{self._source_root!r}; cannot map to a target location."
            )
        suffix = normalized[len(self._source_root):].lstrip("/")
        return f"{self._target_root}/{suffix}"

    def _already_staged(self, target_path: str, expected_size: int) -> bool:
        """Return True if the target already has a file of the expected size."""
        try:
            info = self.target.filesystem.get_file_info(target_path)
        except Exception:
            return False
        return info.type == FileType.File and info.size == expected_size

    @staticmethod
    def _strip_scheme(uri: str) -> str:
        """Strip ``s3://``, ``s3a://``, or ``abfss://`` from the start of a URI."""
        for scheme in ("s3://", "s3a://", "abfss://"):
            if uri.startswith(scheme):
                return uri[len(scheme):]
        return uri

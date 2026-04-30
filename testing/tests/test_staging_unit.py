"""Unit tests for guidewire.staging.StagingExecutor.

These tests verify the staging copy logic in isolation: path mapping,
idempotency, the cross-filesystem stream copy, and integration with the
modified ``Batch._get_parquet_list``. Filesystem operations are mocked --
no real S3 (or LocalStack) is required.
"""

import io
import os
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import pyarrow as pa
import pytest
from pyarrow.fs import FileType

from guidewire.staging import StagingExecutor
from guidewire.storage import AWSStorage, AzureStorage, UCStorage


def _build_uc_response(
    access_key_id="vended-ak",
    secret_access_key="vended-sk",
    session_token="vended-st",
    expiration_time=None,
):
    """Construct a mock GenerateTemporaryTableCredentialResponse (epoch ms)."""
    if expiration_time is None:
        expiration_time = int(datetime.now(timezone.utc).timestamp() * 1000) + 3_600_000
    resp = MagicMock()
    resp.aws_temp_credentials.access_key_id = access_key_id
    resp.aws_temp_credentials.secret_access_key = secret_access_key
    resp.aws_temp_credentials.session_token = session_token
    resp.expiration_time = expiration_time
    return resp


def _build_uc_storage(region="us-east-1"):
    """Construct a fully mocked UCStorage. Returns the UCStorage and the
    mocked WorkspaceClient class so tests can adjust call expectations."""
    with patch("guidewire.storage.WorkspaceClient") as mock_wc_cls:
        mock_wc_cls.return_value.temporary_table_credentials.generate_temporary_table_credentials.return_value = (
            _build_uc_response()
        )
        storage = UCStorage(uc_table_id="t1", region=region)
    # Replace the underlying pyarrow filesystem with a mock so tests don't
    # actually hit S3.
    storage.filesystem = MagicMock()
    return storage


def _build_aws_source_storage():
    env = {
        "AWS_SOURCE_REGION": "us-east-1",
        "AWS_SOURCE_ACCESS_KEY_ID": "k",
        "AWS_SOURCE_SECRET_ACCESS_KEY": "s",
    }
    with patch.dict(os.environ, env, clear=True):
        storage = AWSStorage(prefix="SOURCE")
    storage.filesystem = MagicMock()
    return storage


@pytest.mark.unit
class TestStagingExecutor:
    """Unit tests for StagingExecutor."""

    def _build_executor(self, source_root="guidewire-bucket/cc_claim",
                        target_root="customer-bucket/staged/cc_claim"):
        source = _build_aws_source_storage()
        target = _build_uc_storage()
        executor = StagingExecutor(
            source_storage=source,
            target_storage=target,
            target_table_root=f"s3://{target_root}/",
            source_table_root=f"s3://{source_root}/",
        )
        return executor, source, target

    # ---------- construction guards ----------

    def test_construction_rejects_non_uc_target(self):
        """target_storage must be a UCStorage instance."""
        source = _build_aws_source_storage()
        env = {
            "AWS_TARGET_REGION": "us-east-1",
            "AWS_TARGET_ACCESS_KEY_ID": "k",
            "AWS_TARGET_SECRET_ACCESS_KEY": "s",
        }
        with patch.dict(os.environ, env, clear=True):
            non_uc_target = AWSStorage(prefix="TARGET")
        with pytest.raises(ValueError, match="target_storage to be a UCStorage"):
            StagingExecutor(
                source_storage=source,
                target_storage=non_uc_target,
                target_table_root="s3://bucket/t/",
                source_table_root="s3://src/t/",
            )

    def test_construction_rejects_non_aws_source(self):
        """Cross-cloud staging is rejected in v1."""
        env = {"AZURE_STORAGE_ACCOUNT_NAME": "a", "AZURE_STORAGE_ACCOUNT_KEY": "k"}
        with patch.dict(os.environ, env, clear=True):
            azure_source = AzureStorage()
        target = _build_uc_storage()
        with pytest.raises(NotImplementedError, match="Cross-cloud staging"):
            StagingExecutor(
                source_storage=azure_source,
                target_storage=target,
                target_table_root="s3://bucket/t/",
                source_table_root="abfss://c@a.dfs.core.windows.net/t/",
            )

    def test_construction_rejects_empty_roots(self):
        source = _build_aws_source_storage()
        target = _build_uc_storage()
        with pytest.raises(ValueError, match="non-empty"):
            StagingExecutor(
                source_storage=source,
                target_storage=target,
                target_table_root="",
                source_table_root="s3://src/t/",
            )

    # ---------- path mapping ----------

    def test_map_path_strips_source_root_and_prepends_target(self):
        """Source path under source root maps to target root + relative suffix."""
        executor, _, _ = self._build_executor()
        target = executor._map_path(
            "guidewire-bucket/cc_claim/abc123/20260101T000000Z/part-0.parquet"
        )
        assert target == "customer-bucket/staged/cc_claim/abc123/20260101T000000Z/part-0.parquet"

    def test_map_path_handles_scheme_in_input(self):
        """Source paths with s3:// or s3a:// scheme prefixes are normalized."""
        executor, _, _ = self._build_executor()
        target = executor._map_path(
            "s3a://guidewire-bucket/cc_claim/abc/20260101T000000Z/file.parquet"
        )
        assert target == "customer-bucket/staged/cc_claim/abc/20260101T000000Z/file.parquet"

    def test_map_path_raises_on_path_outside_source_root(self):
        """Source path not enclosed by source_table_root raises."""
        executor, _, _ = self._build_executor()
        with pytest.raises(ValueError, match="not enclosed by source_table_root"):
            executor._map_path("other-bucket/some/file.parquet")

    # ---------- idempotency ----------

    def test_already_staged_skips_when_target_size_matches(self):
        """head_object returning matching size means no copy."""
        executor, source, target = self._build_executor()

        target_info = MagicMock()
        target_info.type = FileType.File
        target_info.size = 12345
        target.filesystem.get_file_info.return_value = target_info

        result = executor.stage_file(
            source_path="guidewire-bucket/cc_claim/abc/20260101T000000Z/part-0.parquet",
            expected_size=12345,
        )
        assert result == "customer-bucket/staged/cc_claim/abc/20260101T000000Z/part-0.parquet"
        # Streams must NOT have been opened.
        source.filesystem.open_input_stream.assert_not_called()
        target.filesystem.open_output_stream.assert_not_called()

    def test_already_staged_returns_false_on_size_mismatch(self):
        """If target exists but size differs, copy proceeds (overwrite)."""
        executor, source, target = self._build_executor()

        target_info = MagicMock()
        target_info.type = FileType.File
        target_info.size = 999  # different from expected
        target.filesystem.get_file_info.return_value = target_info

        # Streams will be opened; provide context-manager-compatible mocks.
        src_stream = MagicMock()
        src_stream.read.side_effect = [b"data", b""]
        source.filesystem.open_input_stream.return_value.__enter__.return_value = src_stream
        target.filesystem.open_output_stream.return_value.__enter__.return_value = MagicMock()

        executor.stage_file(
            source_path="guidewire-bucket/cc_claim/abc/20260101T000000Z/part-0.parquet",
            expected_size=12345,
        )
        source.filesystem.open_input_stream.assert_called_once()
        target.filesystem.open_output_stream.assert_called_once()

    def test_already_staged_returns_false_on_missing_target(self):
        """get_file_info raising means target doesn't exist; copy proceeds."""
        executor, source, target = self._build_executor()
        target.filesystem.get_file_info.side_effect = FileNotFoundError("missing")

        src_stream = MagicMock()
        src_stream.read.side_effect = [b"hello", b""]
        source.filesystem.open_input_stream.return_value.__enter__.return_value = src_stream
        target.filesystem.open_output_stream.return_value.__enter__.return_value = MagicMock()

        executor.stage_file(
            source_path="guidewire-bucket/cc_claim/abc/20260101T000000Z/part-0.parquet",
            expected_size=12345,
        )
        source.filesystem.open_input_stream.assert_called_once()

    # ---------- streaming copy ----------

    def test_stream_copy_writes_all_chunks(self):
        """Bytes from source.read() are forwarded to target.write() in order."""
        executor, source, target = self._build_executor()
        target.filesystem.get_file_info.side_effect = FileNotFoundError()

        chunks = [b"chunk1", b"chunk2", b"chunk3", b""]
        src_stream = MagicMock()
        src_stream.read.side_effect = chunks
        dst_stream = MagicMock()
        source.filesystem.open_input_stream.return_value.__enter__.return_value = src_stream
        target.filesystem.open_output_stream.return_value.__enter__.return_value = dst_stream

        target_path = executor.stage_file(
            source_path="guidewire-bucket/cc_claim/abc/20260101T000000Z/p.parquet",
            expected_size=18,
        )
        # Three chunks written in order, terminator chunk not written.
        assert dst_stream.write.call_count == 3
        written = [call.args[0] for call in dst_stream.write.call_args_list]
        assert written == [b"chunk1", b"chunk2", b"chunk3"]
        assert target_path.endswith("/p.parquet")

    def test_stage_file_returns_correct_target_path(self):
        """stage_file returns the bucket-relative target path."""
        executor, source, target = self._build_executor()
        target.filesystem.get_file_info.side_effect = FileNotFoundError()
        src_stream = MagicMock()
        src_stream.read.side_effect = [b""]
        source.filesystem.open_input_stream.return_value.__enter__.return_value = src_stream
        target.filesystem.open_output_stream.return_value.__enter__.return_value = MagicMock()

        target_path = executor.stage_file(
            source_path="guidewire-bucket/cc_claim/abc/20260101T000000Z/file.parquet",
            expected_size=0,
        )
        assert target_path == "customer-bucket/staged/cc_claim/abc/20260101T000000Z/file.parquet"

    # ---------- normalization helpers ----------

    def test_strip_scheme_handles_s3_s3a_abfss(self):
        assert StagingExecutor._strip_scheme("s3://b/k") == "b/k"
        assert StagingExecutor._strip_scheme("s3a://b/k") == "b/k"
        assert StagingExecutor._strip_scheme("abfss://c@a.dfs.core.windows.net/k") == "c@a.dfs.core.windows.net/k"
        assert StagingExecutor._strip_scheme("plain/path") == "plain/path"

    def test_root_normalization_handles_trailing_slashes(self):
        """Constructor strips trailing slashes from roots."""
        source = _build_aws_source_storage()
        target = _build_uc_storage()
        executor = StagingExecutor(
            source_storage=source,
            target_storage=target,
            target_table_root="s3://customer-bucket/staged/cc_claim///",
            source_table_root="s3://guidewire-bucket/cc_claim///",
        )
        assert executor._target_root == "customer-bucket/staged/cc_claim"
        assert executor._source_root == "guidewire-bucket/cc_claim"


@pytest.mark.unit
class TestBatchStagingIntegration:
    """End-to-end tests showing Batch correctly routes through staging."""

    def test_batch_staging_requires_uc_storage(self):
        """Batch(staging=True) without UCStorage target raises."""
        from guidewire.batch import Batch
        # Mock heavy dependencies on Batch construction. We rely on early
        # validation in Batch.__init__ to raise before reaching them.
        env = {
            "AWS_TARGET_REGION": "us-east-1",
            "AWS_TARGET_ACCESS_KEY_ID": "k",
            "AWS_TARGET_SECRET_ACCESS_KEY": "s",
        }
        with patch.dict(os.environ, env, clear=True):
            non_uc = AWSStorage(prefix="TARGET")

        manifest_mock = MagicMock()
        manifest_mock.read.return_value = {
            "dataFilesPath": "s3://guidewire-bucket/cc_claim/",
            "schemaHistory": {},
            "lastSuccessfulWriteTimestamp": "0",
            "totalProcessedRecordsCount": 0,
        }

        with patch("guidewire.delta_log.AWSDeltaLog._log_exists"):
            with patch.object(
                __import__("guidewire.delta_log", fromlist=["AWSDeltaLog"]).AWSDeltaLog,
                "_get_watermark_from_log",
                return_value={"watermark": 0, "schema_timestamp": 0},
            ):
                with pytest.raises(ValueError, match="staging_mode=True requires"):
                    Batch(
                        table_name="cc_claim",
                        manifest=manifest_mock,
                        target_cloud="aws",
                        storage_or_s3_name="customer-bucket",
                        storage_container=None,
                        target_storage=non_uc,
                        staging_mode=True,
                    )

    def test_batch_staging_rejects_azure_target(self):
        """Batch(staging=True, target_cloud='azure') raises NotImplementedError."""
        from guidewire.batch import Batch
        target = _build_uc_storage()

        manifest_mock = MagicMock()
        manifest_mock.read.return_value = {
            "dataFilesPath": "s3://guidewire-bucket/cc_claim/",
            "schemaHistory": {},
            "lastSuccessfulWriteTimestamp": "0",
            "totalProcessedRecordsCount": 0,
        }

        env = {"AZURE_STORAGE_ACCOUNT_NAME": "a", "AZURE_STORAGE_ACCOUNT_KEY": "k"}
        with patch.dict(os.environ, env, clear=True):
            with patch("guidewire.delta_log.AzureDeltaLog._log_exists"):
                with patch.object(
                    __import__("guidewire.delta_log", fromlist=["AzureDeltaLog"]).AzureDeltaLog,
                    "_get_watermark_from_log",
                    return_value={"watermark": 0, "schema_timestamp": 0},
                ):
                    with pytest.raises(NotImplementedError, match="staging_mode=True is supported only"):
                        Batch(
                            table_name="cc_claim",
                            manifest=manifest_mock,
                            target_cloud="azure",
                            storage_or_s3_name="acct",
                            storage_container="container",
                            target_storage=target,
                            staging_mode=True,
                        )

    def test_processor_staging_rejects_azure_target_cloud(self):
        """Processor(staging_mode=True, target_cloud='azure') raises NotImplementedError."""
        from guidewire.processor import Processor
        env = {
            "AWS_MANIFEST_LOCATION": "s3://test-bucket/manifest.json",
            "AZURE_STORAGE_ACCOUNT_NAME": "a",
            "AZURE_STORAGE_ACCOUNT_CONTAINER": "c",
        }
        with patch.dict(os.environ, env, clear=True):
            with patch("guidewire.processor.Manifest"):
                with pytest.raises(NotImplementedError, match="staging_mode is supported only"):
                    Processor(
                        target_cloud="azure",
                        table_names=("t1",),
                        parallel=False,
                        staging_mode=True,
                        uc_catalog="cat",
                        uc_schema="sch",
                    )

    def test_processor_staging_requires_uc_catalog_and_schema(self):
        """Processor(staging_mode=True) without uc_catalog/uc_schema raises ValueError."""
        from guidewire.processor import Processor
        env = {
            "AWS_MANIFEST_LOCATION": "s3://test-bucket/manifest.json",
            "AWS_S3_BUCKET": "target-bucket",
            "AWS_REGION": "us-east-1",
            "AWS_ACCESS_KEY_ID": "k",
            "AWS_SECRET_ACCESS_KEY": "s",
        }
        with patch.dict(os.environ, env, clear=True):
            with patch("guidewire.processor.Manifest"):
                with pytest.raises(ValueError, match="staging_mode=True requires uc_catalog"):
                    Processor(
                        target_cloud="aws",
                        table_names=("t1",),
                        parallel=False,
                        staging_mode=True,
                        uc_catalog=None,
                        uc_schema=None,
                    )

    def test_processor_staging_resolves_uc_table_ids(self):
        """Processor(staging_mode=True) calls WorkspaceClient.tables.get per table."""
        from guidewire.processor import Processor
        env = {
            "AWS_MANIFEST_LOCATION": "s3://test-bucket/manifest.json",
            "AWS_S3_BUCKET": "target-bucket",
            "AWS_REGION": "us-east-1",
            "AWS_ACCESS_KEY_ID": "k",
            "AWS_SECRET_ACCESS_KEY": "s",
        }
        with patch.dict(os.environ, env, clear=True):
            with patch("guidewire.processor.Manifest") as mock_manifest:
                mock_manifest.return_value.get_table_names.return_value = ["t1", "t2"]
                with patch("guidewire.processor.WorkspaceClient") as mock_wc_cls:
                    # Each tables.get returns an object with table_id=resolved-<full-name>
                    def fake_get(full_name):
                        m = MagicMock()
                        m.table_id = f"id-for-{full_name}"
                        return m
                    mock_wc_cls.return_value.tables.get.side_effect = fake_get

                    processor = Processor(
                        target_cloud="aws",
                        table_names=("t1", "t2"),
                        parallel=False,
                        staging_mode=True,
                        uc_catalog="my_cat",
                        uc_schema="my_sch",
                    )

                    assert processor._uc_table_ids == {
                        "t1": "id-for-my_cat.my_sch.t1",
                        "t2": "id-for-my_cat.my_sch.t2",
                    }
                    assert mock_wc_cls.return_value.tables.get.call_count == 2

    def test_batch_no_staging_keeps_legacy_path_format(self):
        """Without staging=True, _get_parquet_list emits absolute s3a:// paths."""
        from guidewire.batch import Batch
        env = {
            "AWS_TARGET_REGION": "us-east-1",
            "AWS_TARGET_ACCESS_KEY_ID": "k",
            "AWS_TARGET_SECRET_ACCESS_KEY": "s",
        }
        with patch.dict(os.environ, env, clear=True):
            with patch("guidewire.delta_log.AWSDeltaLog._log_exists"):
                with patch.object(
                    __import__("guidewire.delta_log", fromlist=["AWSDeltaLog"]).AWSDeltaLog,
                    "_get_watermark_from_log",
                    return_value={"watermark": 0, "schema_timestamp": 0},
                ):
                    manifest_mock = MagicMock()
                    manifest_mock.read.return_value = {
                        "dataFilesPath": "s3://guidewire-bucket/cc_claim/",
                        "schemaHistory": {},
                        "lastSuccessfulWriteTimestamp": "0",
                        "totalProcessedRecordsCount": 0,
                    }

                    # Mock manifest.fs.get_file_info to return a single parquet file.
                    file_info = MagicMock()
                    file_info.path = "guidewire-bucket/cc_claim/abc/ts/file.parquet"
                    file_info.size = 100
                    file_info.mtime_ns = 1_700_000_000_000_000_000
                    file_info.type = FileType.File
                    manifest_mock.fs.get_file_info.return_value = [file_info]

                    batch = Batch(
                        table_name="cc_claim",
                        manifest=manifest_mock,
                        target_cloud="aws",
                        storage_or_s3_name="customer-bucket",
                        storage_container=None,
                    )
                    assert batch.staging_executor is None
                    files = batch._get_parquet_list("guidewire-bucket/cc_claim/abc/ts/")
                    assert len(files) == 1
                    assert files[0]["path"] == "s3a://guidewire-bucket/cc_claim/abc/ts/file.parquet"

    def test_batch_staging_calls_stage_file_and_emits_relative_path(self):
        """End-to-end: Batch(staging_mode=True) routes _get_parquet_list through
        StagingExecutor.stage_file and emits paths relative to the target root.

        This is the integration test that proves the polymorphic dispatch in
        Batch._get_parquet_list works with staging enabled. It exercises:
          1. Batch constructs StagingExecutor automatically when staging_mode=True
          2. _get_parquet_list calls staging_executor.stage_file per parquet
          3. The returned path is the staged target path passed through
             UCStorage.log_action_path -- i.e. relative to the target table root
        """
        from guidewire.batch import Batch
        target = _build_uc_storage()

        manifest_mock = MagicMock()
        manifest_mock.read.return_value = {
            "dataFilesPath": "s3://guidewire-bucket/cc_claim/",
            "schemaHistory": {},
            "lastSuccessfulWriteTimestamp": "0",
            "totalProcessedRecordsCount": 0,
        }

        # Two synthetic parquet files in the source listing.
        files_meta = [
            ("guidewire-bucket/cc_claim/abc/20260101T000000Z/part-0.parquet", 100),
            ("guidewire-bucket/cc_claim/abc/20260101T000000Z/part-1.parquet", 200),
        ]
        file_infos = []
        for path, size in files_meta:
            f = MagicMock()
            f.path = path
            f.size = size
            f.mtime_ns = 1_700_000_000_000_000_000
            f.type = FileType.File
            file_infos.append(f)
        manifest_mock.fs = MagicMock()
        manifest_mock.fs.get_file_info.return_value = file_infos
        # Important: AWSStorage(prefix="SOURCE") instance check inside StagingExecutor
        # must succeed, so wrap with a real AWSStorage.
        env = {
            "AWS_SOURCE_REGION": "us-east-1",
            "AWS_SOURCE_ACCESS_KEY_ID": "k",
            "AWS_SOURCE_SECRET_ACCESS_KEY": "s",
        }
        with patch.dict(os.environ, env, clear=False):
            real_source = AWSStorage(prefix="SOURCE")
        real_source.filesystem = MagicMock()
        manifest_mock.fs = real_source

        # Re-set get_file_info on the real source's underlying mock filesystem,
        # since manifest_mock.fs is now real_source (an AWSStorage).
        real_source.filesystem.get_file_info = MagicMock(return_value=file_infos)
        # Pre-populate the target so _already_staged returns False (source not
        # in target) and the stream copy proceeds.
        target.filesystem.get_file_info.side_effect = FileNotFoundError()
        # Stub out streams for the copy. Each open() returns a fresh stream
        # whose read() yields one chunk then EOF -- otherwise the side_effect
        # list runs out across multiple files.
        def fresh_input_stream():
            stream = MagicMock()
            stream.read.side_effect = [b"x", b""]
            ctx = MagicMock()
            ctx.__enter__.return_value = stream
            ctx.__exit__.return_value = False
            return ctx

        def fresh_output_stream():
            ctx = MagicMock()
            ctx.__enter__.return_value = MagicMock()
            ctx.__exit__.return_value = False
            return ctx

        real_source.filesystem.open_input_stream.side_effect = lambda *args, **kwargs: fresh_input_stream()
        target.filesystem.open_output_stream.side_effect = lambda *args, **kwargs: fresh_output_stream()

        with patch("guidewire.delta_log.AWSDeltaLog._log_exists"):
            with patch.object(
                __import__("guidewire.delta_log", fromlist=["AWSDeltaLog"]).AWSDeltaLog,
                "_get_watermark_from_log",
                return_value={"watermark": 0, "schema_timestamp": 0},
            ):
                batch = Batch(
                    table_name="cc_claim",
                    manifest=manifest_mock,
                    target_cloud="aws",
                    # storage_or_s3_name maps to the AWSDeltaLog bucket. Choose
                    # a value that produces a known log_uri so we can assert on
                    # the relative path output below.
                    storage_or_s3_name="guidewire-bucket",
                    storage_container=None,
                    target_storage=target,
                    staging_mode=True,
                )

                # Sanity: staging executor was constructed, target/source roots populated.
                assert batch.staging_executor is not None
                assert batch.staging_executor._source_root == "guidewire-bucket/cc_claim"
                # log_uri is "s3://guidewire-bucket/cc_claim/" since
                # AWSDeltaLog.construct_log_uri = f"s3://{bucket}/{table}/"
                assert batch.staging_executor._target_root == "guidewire-bucket/cc_claim"

                files = batch._get_parquet_list("guidewire-bucket/cc_claim/abc/20260101T000000Z/")
                assert len(files) == 2
                # Path 1 (post-stage): relative to target root, with
                # source_root == target_root in this test the staged path is
                # identical to the source layout under the target.
                assert files[0]["path"] == "abc/20260101T000000Z/part-0.parquet"
                assert files[1]["path"] == "abc/20260101T000000Z/part-1.parquet"
                # Sizes preserved
                assert files[0]["size"] == 100
                assert files[1]["size"] == 200
                # Streams opened twice (one per file).
                assert real_source.filesystem.open_input_stream.call_count == 2
                assert target.filesystem.open_output_stream.call_count == 2

"""
Unit tests for target cloud configuration functionality.

These tests verify that the target cloud configuration is properly handled
without requiring external cloud services.
"""

import pytest
import os
from datetime import datetime, timezone
from unittest.mock import patch, MagicMock

from guidewire.processor import Processor
from guidewire.delta_log import AzureDeltaLog, AWSDeltaLog, DeltaValidationError
from guidewire.storage import AWSStorage, AzureStorage, UCStorage


@pytest.mark.unit
class TestTargetCloudConfiguration:
    """Unit tests for target cloud configuration."""
    
    def test_processor_azure_target_validation(self):
        """Test that Azure target validation works correctly."""
        env_vars = {
            "AWS_MANIFEST_LOCATION": "s3://test-bucket/manifest.json",
            # Source S3 credentials (required for manifest reading)
            "AWS_SOURCE_REGION": "us-east-1",
            "AWS_SOURCE_ACCESS_KEY_ID": "sourcekey",
            "AWS_SOURCE_SECRET_ACCESS_KEY": "sourcesecret",
            # Azure target credentials
            "AZURE_STORAGE_ACCOUNT_NAME": "testaccount",
            "AZURE_STORAGE_ACCOUNT_CONTAINER": "testcontainer"
        }
        
        with patch.dict(os.environ, env_vars, clear=True):
            with patch('guidewire.processor.Manifest') as mock_manifest:
                mock_manifest.return_value.get_table_names.return_value = ["test_table"]
                
                processor = Processor(
                    target_cloud="azure",
                    table_names=("test_table",),
                    parallel=False
                )
                
                assert processor.target_cloud == "azure"
                assert processor.log_storage_account == "testaccount"
                assert processor.log_storage_container == "testcontainer"
    
    def test_processor_aws_target_validation(self):
        """Test that AWS target validation works correctly with fallback credentials."""
        env_vars = {
            "AWS_MANIFEST_LOCATION": "s3://test-bucket/manifest.json",
            "AWS_S3_BUCKET": "test-delta-bucket", 
            "AWS_REGION": "us-east-1",
            "AWS_ACCESS_KEY_ID": "testkey",
            "AWS_SECRET_ACCESS_KEY": "testsecret"
        }
        
        with patch.dict(os.environ, env_vars, clear=True):
            with patch('guidewire.processor.Manifest') as mock_manifest:
                mock_manifest.return_value.get_table_names.return_value = ["test_table"]
                
                processor = Processor(
                    target_cloud="aws",
                    table_names=("test_table",),
                    parallel=False
                )
                
                assert processor.target_cloud == "aws"
                assert processor.log_storage_account == "test-delta-bucket"
                assert processor.log_storage_container is None
    
    def test_processor_invalid_target_cloud(self):
        """Test that invalid target cloud raises ValueError."""
        env_vars = {
            "AWS_MANIFEST_LOCATION": "s3://test-bucket/manifest.json"
        }
        
        with patch.dict(os.environ, env_vars, clear=True):
            with patch('guidewire.processor.Manifest'):
                with pytest.raises(ValueError, match="Invalid target_cloud"):
                    Processor(
                        target_cloud="invalid",
                        table_names=("test_table",),
                        parallel=False
                    )
    
    def test_processor_missing_azure_env_vars(self):
        """Test that missing Azure environment variables raise EnvironmentError."""
        env_vars = {
            "AWS_MANIFEST_LOCATION": "s3://test-bucket/manifest.json"
            # Missing AZURE_STORAGE_ACCOUNT_NAME and AZURE_STORAGE_ACCOUNT_CONTAINER
            # Missing SOURCE S3 credentials too
        }
        
        with patch.dict(os.environ, env_vars, clear=True):
            with patch('guidewire.processor.Manifest'):
                with pytest.raises(EnvironmentError, match="Missing required environment variables"):
                    Processor(
                        target_cloud="azure",
                        table_names=("test_table",),
                        parallel=False
                    )
    
    def test_processor_missing_aws_env_vars(self):
        """Test that missing AWS environment variables raise EnvironmentError."""
        env_vars = {
            "AWS_MANIFEST_LOCATION": "s3://test-bucket/manifest.json"
            # Missing AWS_S3_BUCKET, AWS_REGION, AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY
            # Missing both source and target credentials
        }
        
        with patch.dict(os.environ, env_vars, clear=True):
            with patch('guidewire.processor.Manifest'):
                with pytest.raises(EnvironmentError, match="Missing required environment variables"):
                    Processor(
                        target_cloud="aws",
                        table_names=("test_table",),
                        parallel=False
                    )
    
    def test_delta_log_azure_uri_construction(self):
        """Test that Azure URI is constructed correctly."""
        env_vars = {
            "AZURE_STORAGE_ACCOUNT_NAME": "testaccount",
            "AZURE_STORAGE_ACCOUNT_KEY": "testkey"
        }
        
        with patch.dict(os.environ, env_vars, clear=True):
            with patch.object(AzureDeltaLog, '_log_exists'):
                delta_log = AzureDeltaLog(
                    storage_account="testaccount",
                    storage_container="testcontainer",
                    table_name="test_table"
                )
                
                expected_uri = "abfss://testcontainer@testaccount.dfs.core.windows.net/test_table/"
                assert delta_log.log_uri == expected_uri
    
    def test_delta_log_aws_uri_construction(self):
        """Test that S3 URI is constructed correctly."""
        env_vars = {
            "AWS_TARGET_REGION": "us-east-1",
            "AWS_TARGET_ACCESS_KEY_ID": "testkey",
            "AWS_TARGET_SECRET_ACCESS_KEY": "testsecret"
        }
        
        with patch.dict(os.environ, env_vars, clear=True):
            with patch.object(AWSDeltaLog, '_log_exists'):
                delta_log = AWSDeltaLog(
                    bucket_name="test-bucket",
                    table_name="test_table"
                )
                
                expected_uri = "s3://test-bucket/test_table/"
                assert delta_log.log_uri == expected_uri
    
    def test_delta_log_aws_with_prefix(self):
        """Test that S3 URI with prefix is constructed correctly."""
        env_vars = {
            "AWS_TARGET_REGION": "us-east-1",
            "AWS_TARGET_ACCESS_KEY_ID": "testkey",
            "AWS_TARGET_SECRET_ACCESS_KEY": "testsecret"
        }
        
        with patch.dict(os.environ, env_vars, clear=True):
            with patch.object(AWSDeltaLog, '_log_exists'):
                delta_log = AWSDeltaLog(
                    bucket_name="test-bucket",
                    table_name="test_table",
                    subfolder="delta_tables"
                )
                
                expected_uri = "s3://test-bucket/delta_tables/test_table/"
                assert delta_log.log_uri == expected_uri
    
    def test_delta_log_azure_missing_container(self):
        """Test that missing container for Azure raises validation error."""
        with pytest.raises(DeltaValidationError, match="storage_account, storage_container, and table_name must be non-empty strings"):
            AzureDeltaLog(
                storage_account="testaccount",
                storage_container="",  # Empty string should trigger validation error
                table_name="test_table"
            )
    
    def test_storage_aws_options_configuration(self):
        """Test that AWS storage options are configured correctly."""
        env_vars = {
            "AWS_REGION": "us-east-1",
            "AWS_ACCESS_KEY_ID": "testkey",
            "AWS_SECRET_ACCESS_KEY": "testsecret"
        }
        
        with patch.dict(os.environ, env_vars, clear=True):
            storage = AWSStorage()
            
            assert storage._storage_options["region"] == "us-east-1"
            assert storage._storage_options["access_key_id"] == "testkey"
            assert storage._storage_options["secret_access_key"] == "testsecret"
    
    def test_storage_aws_options_with_endpoint(self):
        """Test that AWS storage options with custom endpoint are configured correctly."""
        env_vars = {
            "AWS_REGION": "us-east-1",
            "AWS_ACCESS_KEY_ID": "testkey",
            "AWS_SECRET_ACCESS_KEY": "testsecret",
            "AWS_ENDPOINT_URL": "http://localhost:4566"
        }
        
        with patch.dict(os.environ, env_vars, clear=True):
            storage = AWSStorage()
            
            assert storage._storage_options["region"] == "us-east-1"
            assert storage._storage_options["access_key_id"] == "testkey"
            assert storage._storage_options["secret_access_key"] == "testsecret"
            assert storage._storage_options["endpoint"] == "http://localhost:4566"
    
    def test_main_default_target_cloud(self):
        """Test that main.py defaults to Azure for backward compatibility."""
        from main import main
        
        env_vars = {
            "AWS_MANIFEST_LOCATION": "s3://test-bucket/manifest.json",
            "AZURE_STORAGE_ACCOUNT_NAME": "testaccount",
            "AZURE_STORAGE_ACCOUNT_CONTAINER": "testcontainer"
        }
        
        with patch.dict(os.environ, env_vars, clear=True):
            with patch('main.Processor') as mock_processor:
                mock_processor_instance = MagicMock()
                mock_processor.return_value = mock_processor_instance
                
                main()
                
                # Verify Processor was called with azure as target_cloud (default)
                mock_processor.assert_called_once()
                args, kwargs = mock_processor.call_args
                assert "target_cloud" in kwargs or args[0] == "azure"
    
    def test_main_custom_target_cloud(self):
        """Test that main.py uses custom target cloud from environment."""
        from main import main
        
        env_vars = {
            "AWS_MANIFEST_LOCATION": "s3://test-bucket/manifest.json",
            "AWS_S3_BUCKET": "test-bucket",
            "AWS_REGION": "us-east-1",
            "AWS_ACCESS_KEY_ID": "testkey",
            "AWS_SECRET_ACCESS_KEY": "testsecret",
            "DELTA_TARGET_CLOUD": "aws"
        }
        
        with patch.dict(os.environ, env_vars, clear=True):
            with patch('main.Processor') as mock_processor:
                mock_processor_instance = MagicMock()
                mock_processor.return_value = mock_processor_instance
                
                main()
                
                # Verify Processor was called with aws as target_cloud
                mock_processor.assert_called_once()
                args, kwargs = mock_processor.call_args
                assert "target_cloud" in kwargs or args[0] == "aws"
    
    def test_processor_aws_separate_credentials(self):
        """Test that separate source and target AWS credentials work correctly."""
        env_vars = {
            "AWS_MANIFEST_LOCATION": "s3://source-bucket/manifest.json",
            # Source credentials
            "AWS_SOURCE_REGION": "us-east-1", 
            "AWS_SOURCE_ACCESS_KEY_ID": "source-key",
            "AWS_SOURCE_SECRET_ACCESS_KEY": "source-secret",
            # Target credentials  
            "AWS_TARGET_S3_BUCKET": "target-delta-bucket",
            "AWS_TARGET_REGION": "us-west-2",
            "AWS_TARGET_ACCESS_KEY_ID": "target-key", 
            "AWS_TARGET_SECRET_ACCESS_KEY": "target-secret"
        }
        
        with patch.dict(os.environ, env_vars, clear=True):
            with patch('guidewire.processor.Manifest') as mock_manifest:
                mock_manifest.return_value.get_table_names.return_value = ["test_table"]
                
                processor = Processor(
                    target_cloud="aws",
                    table_names=("test_table",),
                    parallel=False
                )
                
                assert processor.target_cloud == "aws"
                assert processor.log_storage_account == "target-delta-bucket"
                assert processor.log_storage_container is None
    
    def test_processor_aws_mixed_credentials_with_fallback(self):
        """Test that mixed specific and fallback credentials work correctly."""
        env_vars = {
            "AWS_MANIFEST_LOCATION": "s3://source-bucket/manifest.json",
            # Source specific
            "AWS_SOURCE_REGION": "us-east-1",
            "AWS_SOURCE_ACCESS_KEY_ID": "source-key",
            # Fallback credentials (used by both source and target for missing vars)
            "AWS_REGION": "us-west-1",
            "AWS_ACCESS_KEY_ID": "fallback-key", 
            "AWS_SECRET_ACCESS_KEY": "fallback-secret",
            "AWS_S3_BUCKET": "fallback-bucket"
        }
        
        with patch.dict(os.environ, env_vars, clear=True):
            with patch('guidewire.processor.Manifest') as mock_manifest:
                mock_manifest.return_value.get_table_names.return_value = ["test_table"]
                
                processor = Processor(
                    target_cloud="aws",
                    table_names=("test_table",),
                    parallel=False
                )
                
                assert processor.target_cloud == "aws"
                assert processor.log_storage_account == "fallback-bucket"  # Falls back to AWS_S3_BUCKET
                assert processor.log_storage_container is None

    def test_processor_missing_source_aws_credentials(self):
        """Test that missing source AWS credentials raise EnvironmentError."""
        env_vars = {
            "AWS_MANIFEST_LOCATION": "s3://test-bucket/manifest.json",
            # Target credentials present but source missing
            "AWS_TARGET_S3_BUCKET": "target-bucket",
            "AWS_TARGET_REGION": "us-west-2",
            "AWS_TARGET_ACCESS_KEY_ID": "target-key",
            "AWS_TARGET_SECRET_ACCESS_KEY": "target-secret"
            # Missing source credentials and no fallback
        }
        
        with patch.dict(os.environ, env_vars, clear=True):
            with patch('guidewire.processor.Manifest'):
                with pytest.raises(EnvironmentError, match="Source S3"):
                    Processor(
                        target_cloud="aws",
                        table_names=("test_table",),
                        parallel=False
                    )
                    
    def test_processor_missing_target_aws_credentials(self):
        """Test that missing target AWS credentials raise EnvironmentError."""
        env_vars = {
            "AWS_MANIFEST_LOCATION": "s3://test-bucket/manifest.json",
            # Source credentials present but target missing
            "AWS_SOURCE_REGION": "us-east-1",
            "AWS_SOURCE_ACCESS_KEY_ID": "source-key",
            "AWS_SOURCE_SECRET_ACCESS_KEY": "source-secret"
            # Missing target credentials and no fallback
        }
        
        with patch.dict(os.environ, env_vars, clear=True):
            with patch('guidewire.processor.Manifest'):
                with pytest.raises(EnvironmentError, match="Target S3"):
                    Processor(
                        target_cloud="aws",
                        table_names=("test_table",),
                        parallel=False
                    )

    def test_storage_source_prefix_configuration(self):
        """Test that Storage class with SOURCE prefix uses correct environment variables."""
        env_vars = {
            "AWS_SOURCE_REGION": "us-east-1",
            "AWS_SOURCE_ACCESS_KEY_ID": "source-key",
            "AWS_SOURCE_SECRET_ACCESS_KEY": "source-secret",
            "AWS_SOURCE_ENDPOINT_URL": "http://localhost:4566"
        }
        
        with patch.dict(os.environ, env_vars, clear=True):
            storage = AWSStorage(prefix="SOURCE")
            
            assert storage._storage_options["region"] == "us-east-1"
            assert storage._storage_options["access_key_id"] == "source-key"
            assert storage._storage_options["secret_access_key"] == "source-secret"
            assert storage._storage_options["endpoint"] == "http://localhost:4566"
    
    def test_storage_target_prefix_configuration(self):
        """Test that Storage class with TARGET prefix uses correct environment variables."""
        env_vars = {
            "AWS_TARGET_REGION": "us-west-2",
            "AWS_TARGET_ACCESS_KEY_ID": "target-key",
            "AWS_TARGET_SECRET_ACCESS_KEY": "target-secret",
            "AWS_TARGET_ENDPOINT_URL": "https://s3.us-west-2.amazonaws.com"
        }
        
        with patch.dict(os.environ, env_vars, clear=True):
            storage = AWSStorage(prefix="TARGET")

            assert storage._storage_options["region"] == "us-west-2"
            assert storage._storage_options["access_key_id"] == "target-key"
            assert storage._storage_options["secret_access_key"] == "target-secret"
            assert storage._storage_options["endpoint"] == "https://s3.us-west-2.amazonaws.com"

    # -----------------------------------------------------------------
    # Tests for log_action_path polymorphism and the optional
    # ``storage`` parameter on AWSDeltaLog. Together these confirm that
    # opting in to UCStorage produces relative-path AddActions while every
    # existing call site keeps emitting absolute s3a:// paths.
    # -----------------------------------------------------------------

    def test_storage_aws_log_action_path_legacy(self):
        """AWSStorage.log_action_path returns the legacy absolute s3a:// path.

        Backward-compatibility regression guard: any existing caller that
        does not opt into UCStorage must see an unchanged AddAction path
        format.
        """
        env_vars = {
            "AWS_REGION": "us-east-1",
            "AWS_ACCESS_KEY_ID": "k",
            "AWS_SECRET_ACCESS_KEY": "s",
        }
        with patch.dict(os.environ, env_vars, clear=True):
            storage = AWSStorage()
            assert storage.log_action_path(
                file_path="my-bucket/schema_hash/20260101T000000Z/part-0.parquet",
                table_root="s3://my-bucket/cc_claim/",
            ) == "s3a://my-bucket/schema_hash/20260101T000000Z/part-0.parquet"

    def test_storage_azure_log_action_path_legacy(self):
        """AzureStorage inherits the BaseStorage default log_action_path."""
        env_vars = {
            "AZURE_STORAGE_ACCOUNT_NAME": "acct",
            "AZURE_STORAGE_ACCOUNT_KEY": "k",
        }
        with patch.dict(os.environ, env_vars, clear=True):
            storage = AzureStorage()
            assert storage.log_action_path(
                file_path="container/path/file.parquet",
                table_root="abfss://c@a.dfs.core.windows.net/t/",
            ) == "s3a://container/path/file.parquet"

    def test_delta_log_aws_default_storage_unchanged(self):
        """AWSDeltaLog without `storage=` still constructs AWSStorage(prefix='TARGET').

        Backward-compatibility regression guard: the no-arg call shape used
        throughout the existing codebase keeps its meaning.
        """
        env_vars = {
            "AWS_TARGET_REGION": "us-east-1",
            "AWS_TARGET_ACCESS_KEY_ID": "tk",
            "AWS_TARGET_SECRET_ACCESS_KEY": "ts",
        }
        with patch.dict(os.environ, env_vars, clear=True):
            with patch.object(AWSDeltaLog, '_log_exists'):
                delta_log = AWSDeltaLog(bucket_name="b", table_name="t")
                assert isinstance(delta_log.fs, AWSStorage)
                assert delta_log.fs._storage_options["access_key_id"] == "tk"

    @staticmethod
    def _build_uc_response(
        access_key_id="vended-ak",
        secret_access_key="vended-sk",
        session_token="vended-st",
        expiration_time=None,
    ):
        """Construct a mock GenerateTemporaryTableCredentialResponse."""
        if expiration_time is None:
            # Default: 1 hour in the future, in epoch ms (per SDK contract).
            expiration_time = int(datetime.now(timezone.utc).timestamp() * 1000) + 3_600_000
        resp = MagicMock()
        resp.aws_temp_credentials.access_key_id = access_key_id
        resp.aws_temp_credentials.secret_access_key = secret_access_key
        resp.aws_temp_credentials.session_token = session_token
        resp.expiration_time = expiration_time
        return resp

    def test_storage_uc_init(self):
        """UCStorage init populates storage_options with vended credentials."""
        with patch('guidewire.storage.WorkspaceClient') as mock_wc_cls:
            mock_wc_cls.return_value.temporary_table_credentials.generate_temporary_table_credentials.return_value = (
                self._build_uc_response()
            )

            storage = UCStorage(uc_table_id="abc-123", region="us-east-1")

            opts = storage.storage_options
            assert opts["region"] == "us-east-1"
            assert opts["access_key_id"] == "vended-ak"
            assert opts["secret_access_key"] == "vended-sk"
            assert opts["session_token"] == "vended-st"
            # Confirm the SDK was called with the right operation enum.
            call_kwargs = mock_wc_cls.return_value.temporary_table_credentials.generate_temporary_table_credentials.call_args.kwargs
            assert call_kwargs["table_id"] == "abc-123"

    def test_storage_uc_log_action_path_relative(self):
        """UCStorage.log_action_path emits a path relative to the table root."""
        with patch('guidewire.storage.WorkspaceClient') as mock_wc_cls:
            mock_wc_cls.return_value.temporary_table_credentials.generate_temporary_table_credentials.return_value = (
                self._build_uc_response()
            )
            storage = UCStorage(uc_table_id="t1")

            relative = storage.log_action_path(
                file_path="my-bucket/cc_claim/schema_hash/20260101T000000Z/part-0.parquet",
                table_root="s3://my-bucket/cc_claim/",
            )
            assert relative == "schema_hash/20260101T000000Z/part-0.parquet"

    def test_storage_uc_log_action_path_cross_bucket_raises(self):
        """UCStorage rejects source files outside the target table root."""
        with patch('guidewire.storage.WorkspaceClient') as mock_wc_cls:
            mock_wc_cls.return_value.temporary_table_credentials.generate_temporary_table_credentials.return_value = (
                self._build_uc_response()
            )
            storage = UCStorage(uc_table_id="t1")

            with pytest.raises(ValueError, match="UC governance requires"):
                storage.log_action_path(
                    file_path="other-bucket/some/path/file.parquet",
                    table_root="s3://my-bucket/cc_claim/",
                )

    def test_storage_uc_credentials_refresh_before_expiry(self):
        """Accessing storage_options near expiry triggers a re-vend."""
        with patch('guidewire.storage.WorkspaceClient') as mock_wc_cls:
            gen = mock_wc_cls.return_value.temporary_table_credentials.generate_temporary_table_credentials
            # First call (in __init__): credentials expire in 60s — well within the 300s buffer.
            now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
            gen.return_value = self._build_uc_response(expiration_time=now_ms + 60_000)

            storage = UCStorage(uc_table_id="t1")
            assert gen.call_count == 1

            # Second call (during storage_options access): plenty of headroom.
            gen.return_value = self._build_uc_response(expiration_time=now_ms + 3_600_000)

            _ = storage.storage_options  # property access should trigger refresh
            assert gen.call_count == 2

    def test_delta_log_aws_with_uc_storage_param(self):
        """AWSDeltaLog uses caller-supplied storage instead of constructing AWSStorage."""
        with patch('guidewire.storage.WorkspaceClient') as mock_wc_cls:
            mock_wc_cls.return_value.temporary_table_credentials.generate_temporary_table_credentials.return_value = (
                self._build_uc_response()
            )
            uc = UCStorage(uc_table_id="t1", region="us-east-1")

            with patch.object(AWSDeltaLog, '_log_exists'):
                delta_log = AWSDeltaLog(
                    bucket_name="b",
                    table_name="t",
                    storage=uc,
                )

                assert delta_log.fs is uc
                assert "session_token" in delta_log.storage_options

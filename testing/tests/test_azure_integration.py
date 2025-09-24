"""
Integration tests for Azure Blob Storage functionality using Azurite.

These tests require Docker Compose services to be running:
    docker-compose up -d

Run with: pytest testing/tests/test_azure_integration.py -v -m integration
"""

import pytest
import os
import time
import requests
import json
from pathlib import Path
from azure.storage.blob import BlobServiceClient, BlobClient
from azure.core.exceptions import ResourceNotFoundError

# Test configuration
AZURITE_ENDPOINT = "http://localhost:10000"
STORAGE_ACCOUNT_NAME = "testingstorage"
STORAGE_ACCOUNT_KEY = "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw=="
TEST_CONTAINER_PREFIX = "test-guidewire-cda"


class TestAzureIntegration:
    """Integration tests for Azure Blob Storage operations using Azurite."""
    
    @classmethod
    def setup_class(cls):
        """Set up class-level resources."""
        cls._check_azurite_running()
        
        # Create blob service client
        account_url = f"{AZURITE_ENDPOINT}/{STORAGE_ACCOUNT_NAME}"
        cls.blob_service_client = BlobServiceClient(
            account_url=account_url,
            credential=STORAGE_ACCOUNT_KEY
        )
        
    @classmethod
    def _check_azurite_running(cls):
        """Check if Azurite is running and accessible."""
        try:
            # Check if Azurite service is responding
            health_url = f"{AZURITE_ENDPOINT}/"
            response = requests.get(health_url, timeout=5)
            if response.status_code not in [200, 400]:  # 400 is acceptable for some Azurite responses
                pytest.skip("Azurite not running. Start with: docker-compose up -d")
        except requests.RequestException:
            pytest.skip("Azurite not running. Start with: docker-compose up -d")
    
    def setup_method(self):
        """Set up method-level resources."""
        self.test_container = f"{TEST_CONTAINER_PREFIX}-{os.getpid()}-{int(time.time())}"
        self.container_client = self.blob_service_client.create_container(self.test_container)
        
    def teardown_method(self):
        """Clean up method-level resources."""
        try:
            # Delete container and all blobs
            self.blob_service_client.delete_container(self.test_container)
        except ResourceNotFoundError:
            pass  # Ignore cleanup errors
    
    @pytest.mark.integration
    def test_create_and_list_container(self):
        """Test basic Azure Blob container operations."""
        # List containers - our test container should exist
        containers = list(self.blob_service_client.list_containers())
        container_names = [container['name'] for container in containers]
        
        assert self.test_container in container_names
        
    @pytest.mark.integration
    def test_upload_and_download_blob(self):
        """Test blob upload and download operations."""
        test_content = "Hello from Azure integration test!"
        blob_name = "test-blob.txt"
        
        # Upload blob
        blob_client = self.blob_service_client.get_blob_client(
            container=self.test_container,
            blob=blob_name
        )
        blob_client.upload_blob(test_content, overwrite=True)
        
        # Download and verify
        downloaded_content = blob_client.download_blob().readall().decode('utf-8')
        
        assert downloaded_content == test_content
        
    @pytest.mark.integration
    def test_upload_example_data(self):
        """Test uploading the example CDA data to Azure Blob Storage."""
        examples_dir = Path(__file__).parent.parent.parent / "examples"
        
        if not examples_dir.exists():
            pytest.skip("Examples directory not found")
        
        uploaded_blobs = []
        
        # Upload all example files
        for file_path in examples_dir.rglob('*'):
            if file_path.is_file():
                relative_path = file_path.relative_to(examples_dir)
                blob_name = str(relative_path).replace('\\', '/')
                
                blob_client = self.blob_service_client.get_blob_client(
                    container=self.test_container,
                    blob=blob_name
                )
                
                with open(file_path, 'rb') as file_data:
                    blob_client.upload_blob(file_data, overwrite=True)
                    
                uploaded_blobs.append(blob_name)
        
        # Verify blobs were uploaded
        blob_list = list(self.blob_service_client.get_container_client(
            self.test_container
        ).list_blobs())
        blob_names = [blob.name for blob in blob_list]
        
        assert len(uploaded_blobs) > 0, "No example files were uploaded"
        
        for uploaded_blob in uploaded_blobs:
            assert uploaded_blob in blob_names, f"Blob {uploaded_blob} not found in container"
            
        print(f"Successfully uploaded {len(uploaded_blobs)} example files to Azure Blob Storage")
        
    @pytest.mark.integration
    def test_manifest_upload_and_download(self):
        """Test uploading and downloading the manifest.json file."""
        examples_dir = Path(__file__).parent.parent.parent / "examples"
        manifest_path = examples_dir / "cda" / "manifest.json"
        
        if not manifest_path.exists():
            pytest.skip("Manifest file not found")
            
        # Upload manifest
        blob_name = "cda/manifest.json"
        blob_client = self.blob_service_client.get_blob_client(
            container=self.test_container,
            blob=blob_name
        )
        
        with open(manifest_path, 'rb') as file_data:
            blob_client.upload_blob(file_data, overwrite=True)
        
        # Download and verify content
        downloaded_content = blob_client.download_blob().readall().decode('utf-8')
        manifest_data = json.loads(downloaded_content)
        
        # Verify manifest structure
        assert isinstance(manifest_data, dict)
        assert 'policy_holders' in manifest_data
        assert 'lastSuccessfulWriteTimestamp' in manifest_data['policy_holders']
        assert 'dataFilesPath' in manifest_data['policy_holders']
        
        print(f"Manifest contains {len(manifest_data)} entities")
        
    @pytest.mark.integration
    def test_parquet_file_handling(self):
        """Test handling of parquet files from examples."""
        examples_dir = Path(__file__).parent.parent.parent / "examples"
        
        # Find a parquet file
        parquet_files = list(examples_dir.rglob('*.parquet'))
        
        if not parquet_files:
            pytest.skip("No parquet files found in examples")
            
        test_parquet = parquet_files[0]
        relative_path = test_parquet.relative_to(examples_dir)
        blob_name = str(relative_path).replace('\\', '/')
        
        # Upload parquet file
        blob_client = self.blob_service_client.get_blob_client(
            container=self.test_container,
            blob=blob_name
        )
        
        with open(test_parquet, 'rb') as file_data:
            blob_client.upload_blob(file_data, overwrite=True)
        
        # Verify upload
        blob_properties = blob_client.get_blob_properties()
        assert blob_properties.size > 0
        
        # Test file properties
        blob_list = list(self.blob_service_client.get_container_client(
            self.test_container
        ).list_blobs(name_starts_with=blob_name))
        
        found_blob = None
        for blob in blob_list:
            if blob.name == blob_name:
                found_blob = blob
                break
                
        assert found_blob is not None
        assert found_blob.size > 0
        assert blob_name.endswith('.parquet')
        
        print(f"Successfully handled parquet file: {blob_name} ({found_blob.size} bytes)")

    @pytest.mark.integration
    def test_directory_structure_preservation(self):
        """Test that directory structure is preserved in Azure Blob Storage."""
        examples_dir = Path(__file__).parent.parent.parent / "examples"
        
        if not examples_dir.exists():
            pytest.skip("Examples directory not found")
        
        expected_structure = {}
        
        # Upload files and track structure
        for file_path in examples_dir.rglob('*'):
            if file_path.is_file():
                relative_path = file_path.relative_to(examples_dir)
                blob_name = str(relative_path).replace('\\', '/')
                
                # Track directory levels
                parts = blob_name.split('/')
                if len(parts) > 1:
                    directory = '/'.join(parts[:-1])
                    if directory not in expected_structure:
                        expected_structure[directory] = []
                    expected_structure[directory].append(parts[-1])
                
                blob_client = self.blob_service_client.get_blob_client(
                    container=self.test_container,
                    blob=blob_name
                )
                
                with open(file_path, 'rb') as file_data:
                    blob_client.upload_blob(file_data, overwrite=True)
        
        # Verify structure in Azure Blob Storage
        all_blobs = list(self.blob_service_client.get_container_client(
            self.test_container
        ).list_blobs())
        blob_names = [blob.name for blob in all_blobs]
        
        # Check each directory has expected files
        for directory, files in expected_structure.items():
            directory_files = [
                blob_name.split('/')[-1] for blob_name in blob_names 
                if blob_name.startswith(directory + '/') and blob_name.count('/') == directory.count('/') + 1
            ]
            
            for expected_file in files:
                assert expected_file in directory_files, f"File {expected_file} not found in directory {directory}"
        
        print(f"Directory structure preserved with {len(expected_structure)} directories")
        
    @pytest.mark.integration
    def test_blob_metadata_and_properties(self):
        """Test setting and getting blob metadata and properties."""
        test_content = "Test content for metadata"
        blob_name = "metadata-test.txt"
        
        # Upload blob with metadata
        blob_client = self.blob_service_client.get_blob_client(
            container=self.test_container,
            blob=blob_name
        )
        
        metadata = {
            'source': 'integration_test',
            'created_by': 'pytest',
            'data_type': 'text'
        }
        
        from azure.storage.blob import ContentSettings
        
        blob_client.upload_blob(
            test_content, 
            metadata=metadata,
            content_settings=ContentSettings(content_type='text/plain'),
            overwrite=True
        )
        
        # Verify metadata
        blob_properties = blob_client.get_blob_properties()
        assert blob_properties.metadata == metadata
        assert blob_properties.content_settings.content_type == 'text/plain'
        assert blob_properties.size == len(test_content)
        
        print(f"Blob metadata verified: {blob_properties.metadata}")

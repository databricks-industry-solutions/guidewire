.PHONY: help test test-integration test-s3 test-azure test-storage test-manifest test-e2e test-slow test-quick docker-up docker-down clean

help:  ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-20s\033[0m %s\n", $$1, $$2}'


# Docker management
docker-up:  ## Start LocalStack and Azurite service
	docker-compose up -d
	@echo "Waiting for services to be ready..."
	@for i in {1..30}; do \
		if curl -s http://localhost:4566/health > /dev/null 2>&1 && \
		   curl -s "http://localhost:10000/testingstorage?comp=properties&restype=service" > /dev/null 2>&1; then \
			echo "✓ Services are ready!"; \
			break; \
		fi; \
		echo "Waiting for services... ($$i/30)"; \
		sleep 1; \
	done

docker-down:  ## Stop all Docker services
	docker-compose down

docker-restart: docker-down docker-up  ## Restart all Docker services

# Testing
test: test-unit test-integration  ## Run all tests (unit + integration)

test-unit:  ## Run unit tests (fast, no external services required)
	pytest testing/tests/ -v -m unit

test-integration: docker-up  ## Run all integration tests with real LocalStack/Azurite
	pytest testing/tests/ -v -m integration

test-s3: docker-up  ## Run S3 integration tests only
	pytest testing/tests/test_s3_integration.py -v -m integration

test-azure: docker-up  ## Run Azure integration tests only
	pytest testing/tests/test_azure_integration.py -v -m integration

test-storage: docker-up  ## Run Storage class integration tests
	pytest testing/tests/test_storage_integration.py -v -m integration

test-manifest: docker-up  ## Run Manifest class integration tests
	pytest testing/tests/test_manifest_integration.py -v -m integration

test-e2e: docker-up  ## Run end-to-end workflow tests
	pytest testing/tests/test_end_to_end_integration.py -v -m integration

test-slow: docker-up  ## Run slow/comprehensive tests
	pytest testing/tests/ -v -m slow

test-quick: docker-up  ## Run quick integration tests (exclude slow tests)
	pytest testing/tests/ -v -m integration -m "not slow"

# Development
install-deps:  ## Install test dependencies
	pip install pytest boto3 requests pyarrow azure-storage-blob

clean:  ## Clean up temporary files and Docker volumes
	docker-compose down -v
	rm -rf testing/tmp/*
	rm -rf tmp/*
	find . -name "*.pyc" -delete
	find . -name "__pycache__" -type d -exec rm -rf {} +

# CI/CD ready commands
ci-test: install-deps test-unit test-quick  ## CI-friendly tests (unit + quick integration)

ci-integration: install-deps docker-up test-integration docker-down  ## Full integration test for CI

ci-e2e: install-deps docker-up test-e2e docker-down  ## End-to-end tests for CI

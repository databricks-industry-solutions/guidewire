"""
Guidewire CDA Delta Clone package.

This package provides tools for processing Guidewire CDA data and creating Delta Lake tables.
"""

from .processor import Processor
from .batch import Batch
from .manifest import Manifest
from .results import Result
from .delta_log import AzureDeltaLog, AWSDeltaLog, DeltaError, DeltaValidationError
from .storage import AzureStorage, AWSStorage

__version__ = "0.0.4"

# Main classes for easy access
__all__ = [
    "Processor",
    "Batch", 
    "Manifest",
    "Result",
    "AzureDeltaLog",
    "AWSDeltaLog",
    "DeltaError",
    "DeltaValidationError",
    "AzureStorage",
    "AWSStorage",
]

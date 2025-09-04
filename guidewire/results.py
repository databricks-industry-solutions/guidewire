from datetime import datetime
from typing import Optional
import dataclasses

@dataclasses.dataclass
class Result:
    table: str
    process_start_time: datetime
    process_start_watermark: int
    process_start_version: int
    manifest_records: int
    manifest_watermark: int
    process_finish_time: Optional[datetime]
    process_finish_watermark: Optional[int]
    process_finish_version: Optional[int]
    watermarks: list[int]
    schema_timestamps: list[int]
    errors: Optional[list[str]]
    warnings: Optional[list[str]]

    def update(self, **kwargs) -> None:
        """Update the result object with the provided key-value pairs."""
        for key, value in kwargs.items():
            if hasattr(self, key):
                setattr(self, key, value)

    def add_error(self, error_message: str) -> None:
        """Add an error message to the result's errors list."""
        if self.errors is None:
            self.errors = []
        self.errors.append(error_message)

    def add_warning(self, warning_message: str) -> None:
        """Add a warning message to the result's warnings list."""
        if self.warnings is None:
            self.warnings = []
        self.warnings.append(warning_message)

    def add_watermark(self, watermark: int) -> None:
        """Add a watermark to the result's watermarks list."""
        self.watermarks.append(watermark)

    def add_schema_timestamp(self, schema_timestamp: int) -> None:
        """Add a schema timestamp to the result's schema_timestamps list."""
        self.schema_timestamps.append(schema_timestamp)

"""
Custom exceptions for Rosetta Compute Engine.
"""


class RosettaException(Exception):
    """Base exception for all Rosetta errors."""
    
    def __init__(self, message: str, details: dict | None = None):
        super().__init__(message)
        self.message = message
        self.details = details or {}


class DatabaseException(RosettaException):
    """Exception for database-related errors."""
    pass


class PipelineException(RosettaException):
    """Exception for pipeline-related errors."""
    pass


class SourceException(RosettaException):
    """Exception for source-related errors."""
    pass


class DestinationException(RosettaException):
    """Exception for destination-related errors."""
    pass


class ConfigurationException(RosettaException):
    """Exception for configuration-related errors."""
    pass


class ValidationException(RosettaException):
    """Exception for validation errors."""
    pass

"""
Exceptions for the MallardDataVault library.
"""
from typing import Optional, Any


class DVException(Exception):
    """Base exception for MallardDataVault"""
    pass


class DVSQLError(DVException):
    """Exception raised for SQL execution errors"""
    def __init__(self, message: str, sql: str, original_error: Optional[Exception] = None):
        self.sql = sql
        self.original_error = original_error
        super().__init__(f"{message}: {str(original_error) if original_error else ''}")


class DVMetadataError(DVException):
    """Exception raised for metadata-related errors"""
    pass


class DVEntityError(DVException):
    """Exception raised for issues with Data Vault entities"""
    pass


class DVConfigurationError(DVException):
    """Exception raised for configuration issues"""
    pass


class DVETLError(DVException):
    """Exception raised during ETL processes"""
    pass
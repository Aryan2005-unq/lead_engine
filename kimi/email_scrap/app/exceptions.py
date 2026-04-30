"""
Custom Exception Classes for Better Error Handling and Categorization
Based on Python's built-in error types with enhanced context and categorization
"""


class BaseAppException(Exception):
    """Base exception class for all application exceptions"""
    
    def __init__(self, message: str, context: dict = None, severity: str = "medium"):
        """
        Initialize base exception
        
        Args:
            message: Error message
            context: Additional context information
            severity: Error severity (critical, high, medium, low)
        """
        super().__init__(message)
        self.message = message
        self.context = context or {}
        self.severity = severity
        self.error_type = self.__class__.__name__
        self.category = self._get_category()
    
    def _get_category(self) -> str:
        """Determine error category based on exception type"""
        category_map = {
            'SyntaxError': 'Validation',
            'NameError': 'Validation',
            'TypeError': 'Validation',
            'ValueError': 'Validation',
            'IndexError': 'Validation',
            'KeyError': 'Validation',
            'AttributeError': 'Validation',
            'ZeroDivisionError': 'Validation',
            'FileSystemError': 'File System',
            'DatabaseError': 'Database',
            'NetworkError': 'Network',
            'AuthenticationError': 'Authentication',
            'PermissionError': 'Permission',
            'ResourceError': 'Resource',
        }
        return category_map.get(self.error_type, 'General')
    
    def to_dict(self) -> dict:
        """Convert exception to dictionary for logging"""
        return {
            'error_type': self.error_type,
            'message': self.message,
            'category': self.category,
            'severity': self.severity,
            'context': self.context
        }


# ============================================
# Python Standard Error Types (Customized)
# ============================================

class SyntaxError(BaseAppException):
    """Raised when the parser encounters a syntax error"""
    
    def __init__(self, message: str = "Syntax error in code", context: dict = None, 
                 line_number: int = None, file_path: str = None):
        if context is None:
            context = {}
        if line_number:
            context['line_number'] = line_number
        if file_path:
            context['file_path'] = file_path
        super().__init__(message, context, severity="high")


class NameError(BaseAppException):
    """Raised when a local or global name is not found"""
    
    def __init__(self, message: str = "Name not found", context: dict = None, 
                 name: str = None, scope: str = None):
        if context is None:
            context = {}
        if name:
            context['name'] = name
        if scope:
            context['scope'] = scope
        super().__init__(message, context, severity="high")


class TypeError(BaseAppException):
    """Raised when an operation or function is applied to an object of inappropriate type"""
    
    def __init__(self, message: str = "Type error", context: dict = None, 
                 expected_type: str = None, actual_type: str = None):
        if context is None:
            context = {}
        if expected_type:
            context['expected_type'] = expected_type
        if actual_type:
            context['actual_type'] = actual_type
        super().__init__(message, context, severity="high")


class ValueError(BaseAppException):
    """Raised when a function receives an argument of the right type but an inappropriate value"""
    
    def __init__(self, message: str = "Invalid value", context: dict = None, 
                 parameter: str = None, value: str = None):
        if context is None:
            context = {}
        if parameter:
            context['parameter'] = parameter
        if value:
            context['value'] = str(value)
        super().__init__(message, context, severity="medium")


class IndexError(BaseAppException):
    """Raised when an index is not found in a sequence"""
    
    def __init__(self, message: str = "Index out of range", context: dict = None, 
                 index: int = None, sequence_length: int = None):
        if context is None:
            context = {}
        if index is not None:
            context['index'] = index
        if sequence_length is not None:
            context['sequence_length'] = sequence_length
        super().__init__(message, context, severity="medium")


class KeyError(BaseAppException):
    """Raised when a dictionary key is not found"""
    
    def __init__(self, message: str = "Key not found", context: dict = None, 
                 key: str = None, available_keys: list = None):
        if context is None:
            context = {}
        if key:
            context['key'] = key
        if available_keys:
            context['available_keys'] = available_keys
        super().__init__(message, context, severity="medium")


class AttributeError(BaseAppException):
    """Raised when an attribute reference or assignment fails"""
    
    def __init__(self, message: str = "Attribute not found", context: dict = None, 
                 object_type: str = None, attribute: str = None):
        if context is None:
            context = {}
        if object_type:
            context['object_type'] = object_type
        if attribute:
            context['attribute'] = attribute
        super().__init__(message, context, severity="medium")


class ZeroDivisionError(BaseAppException):
    """Raised when division by zero is attempted"""
    
    def __init__(self, message: str = "Division by zero", context: dict = None, 
                 numerator: float = None, denominator: float = None):
        if context is None:
            context = {}
        if numerator is not None:
            context['numerator'] = numerator
        if denominator is not None:
            context['denominator'] = denominator
        super().__init__(message, context, severity="high")


# ============================================
# Application-Specific Error Types
# ============================================

class FileSystemError(BaseAppException):
    """Raised when file system operations fail"""
    
    def __init__(self, message: str = "File system error", context: dict = None, 
                 file_path: str = None, operation: str = None):
        if context is None:
            context = {}
        if file_path:
            context['file_path'] = file_path
        if operation:
            context['operation'] = operation
        super().__init__(message, context, severity="high")


class DatabaseError(BaseAppException):
    """Raised when database operations fail"""
    
    def __init__(self, message: str = "Database error", context: dict = None, 
                 query: str = None, table: str = None):
        if context is None:
            context = {}
        if query:
            context['query'] = query
        if table:
            context['table'] = table
        super().__init__(message, context, severity="critical")


class NetworkError(BaseAppException):
    """Raised when network operations fail"""
    
    def __init__(self, message: str = "Network error", context: dict = None, 
                 url: str = None, status_code: int = None):
        if context is None:
            context = {}
        if url:
            context['url'] = url
        if status_code:
            context['status_code'] = status_code
        super().__init__(message, context, severity="high")


class AuthenticationError(BaseAppException):
    """Raised when authentication fails"""
    
    def __init__(self, message: str = "Authentication failed", context: dict = None, 
                 user_email: str = None, reason: str = None):
        if context is None:
            context = {}
        if user_email:
            context['user_email'] = user_email
        if reason:
            context['reason'] = reason
        super().__init__(message, context, severity="high")


class PermissionError(BaseAppException):
    """Raised when permission is denied"""
    
    def __init__(self, message: str = "Permission denied", context: dict = None, 
                 resource: str = None, required_permission: str = None):
        if context is None:
            context = {}
        if resource:
            context['resource'] = resource
        if required_permission:
            context['required_permission'] = required_permission
        super().__init__(message, context, severity="high")


class ResourceError(BaseAppException):
    """Raised when resource operations fail (memory, disk, etc.)"""
    
    def __init__(self, message: str = "Resource error", context: dict = None, 
                 resource_type: str = None, limit: str = None):
        if context is None:
            context = {}
        if resource_type:
            context['resource_type'] = resource_type
        if limit:
            context['limit'] = limit
        super().__init__(message, context, severity="critical")


# ============================================
# Error Handler Utilities
# ============================================

def categorize_exception(exception: Exception) -> tuple[str, str]:
    """
    Categorize an exception and determine its severity
    
    Returns:
        tuple: (category, severity)
    """
    if isinstance(exception, BaseAppException):
        return exception.category, exception.severity
    
    # Map built-in Python exceptions to categories
    exception_type = type(exception).__name__
    
    category_map = {
        'SyntaxError': ('Validation', 'high'),
        'NameError': ('Validation', 'high'),
        'TypeError': ('Validation', 'high'),
        'ValueError': ('Validation', 'medium'),
        'IndexError': ('Validation', 'medium'),
        'KeyError': ('Validation', 'medium'),
        'AttributeError': ('Validation', 'medium'),
        'ZeroDivisionError': ('Validation', 'high'),
        'FileNotFoundError': ('File System', 'high'),
        'PermissionError': ('Permission', 'high'),
        'ConnectionError': ('Network', 'high'),
        'TimeoutError': ('Network', 'medium'),
        'OSError': ('File System', 'medium'),
        'IOError': ('File System', 'medium'),
    }
    
    return category_map.get(exception_type, ('General', 'medium'))


def format_exception_for_logging(exception: Exception) -> dict:
    """
    Format exception for logging with all relevant information
    
    Returns:
        dict: Formatted exception data
    """
    if isinstance(exception, BaseAppException):
        return exception.to_dict()
    
    category, severity = categorize_exception(exception)
    
    return {
        'error_type': type(exception).__name__,
        'message': str(exception),
        'category': category,
        'severity': severity,
        'context': {}
    }





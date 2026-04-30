"""
Logging System for tracking all changes and activities
"""
import logging
from datetime import datetime, timedelta
from typing import Optional, Dict, Any, List
from app.database import get_db_connection
from app.exceptions import (
    BaseAppException, categorize_exception, format_exception_for_logging
)
import json
import traceback
import re

# Configure Python logging
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

class ActivityLogger:
    """Logs all user activities and system changes to database"""
    
    @staticmethod
    def log_activity(
        user_id: Optional[int],
        user_email: Optional[str],
        action: str,
        resource_type: str,
        resource_id: Optional[str] = None,
        details: Optional[Dict[str, Any]] = None,
        status: str = "success",
        error_message: Optional[str] = None,
        error_traceback: Optional[str] = None,
        ip_address: Optional[str] = None,
        exception: Optional[Exception] = None
    ):
        """
        Log an activity to the database
        
        Args:
            user_id: ID of the user performing the action
            user_email: Email of the user
            action: Action performed (e.g., 'login', 'view_file', 'run_script')
            resource_type: Type of resource (e.g., 'user', 'file', 'script')
            resource_id: ID or identifier of the resource
            details: Additional details as dictionary
            status: 'success' or 'error'
            error_message: Error message if status is 'error'
            error_traceback: Full traceback if available
            ip_address: IP address of the user
            exception: Exception object if available (will be used for better categorization)
        """
        try:
            conn = get_db_connection()
            cursor = conn.cursor()
            
            try:
                # Include traceback in details if provided
                if details is None:
                    details = {}
                if error_traceback:
                    details['traceback'] = error_traceback
                
                # If exception is provided, extract enhanced error information
                if exception:
                    error_info = format_exception_for_logging(exception)
                    details['error_type'] = error_info.get('error_type')
                    details['error_category'] = error_info.get('category')
                    details['error_severity'] = error_info.get('severity')
                    details['error_context'] = error_info.get('context', {})
                    
                    # Enhance error message if not provided
                    if not error_message:
                        error_message = error_info.get('message', str(exception))
                
                details_json = json.dumps(details) if details else None
                
                cursor.execute("""
                    INSERT INTO activity_logs (
                        user_id, user_email, action, resource_type, 
                        resource_id, details, status, error_message, ip_address, created_at
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, (
                    user_id,
                    user_email,
                    action,
                    resource_type,
                    resource_id,
                    details_json,
                    status,
                    error_message,
                    ip_address,
                    datetime.now()
                ))
                
                conn.commit()
                
                # Also log to Python logger
                log_message = f"[{action}] {resource_type}"
                if resource_id:
                    log_message += f" ({resource_id})"
                if user_email:
                    log_message += f" by {user_email}"
                if status == "error" and error_message:
                    log_message += f" - ERROR: {error_message}"
                
                logger.info(log_message)
                
            except Exception as e:
                conn.rollback()
                logger.error(f"Failed to log activity: {e}")
                logger.error(traceback.format_exc())
            finally:
                cursor.close()
                conn.close()
                
        except Exception as e:
            logger.error(f"Database connection error in logging: {e}")
    
    @staticmethod
    def get_logs(
        limit: int = 100,
        user_id: Optional[int] = None,
        action: Optional[str] = None,
        resource_type: Optional[str] = None,
        status: Optional[str] = None
    ) -> list:
        """Retrieve activity logs from database"""
        try:
            conn = get_db_connection()
            cursor = conn.cursor()
            
            try:
                query = "SELECT * FROM activity_logs WHERE 1=1"
                params = []
                
                if user_id:
                    query += " AND user_id = %s"
                    params.append(user_id)
                
                if action:
                    query += " AND action = %s"
                    params.append(action)
                
                if resource_type:
                    query += " AND resource_type = %s"
                    params.append(resource_type)
                
                if status:
                    query += " AND status = %s"
                    params.append(status)
                
                query += " ORDER BY created_at DESC LIMIT %s"
                params.append(limit)
                
                cursor.execute(query, params)
                results = cursor.fetchall()
                
                # Convert datetime objects to strings for JSON serialization
                logs = []
                for row in results:
                    log_dict = dict(row)
                    if log_dict.get('created_at'):
                        log_dict['created_at'] = log_dict['created_at'].isoformat()
                    if log_dict.get('details'):
                        try:
                            log_dict['details'] = json.loads(log_dict['details'])
                        except:
                            pass
                    logs.append(log_dict)
                
                return logs
                
            finally:
                cursor.close()
                conn.close()
                
        except Exception as e:
            logger.error(f"Error retrieving logs: {e}")
            return []
    
    @staticmethod
    def get_user_activity_summary(user_id: Optional[int] = None, days: int = 7) -> Dict[str, Any]:
        """Get activity summary statistics"""
        try:
            conn = get_db_connection()
            cursor = conn.cursor()
            
            try:
                query = """
                    SELECT 
                        COUNT(*) as total_actions,
                        COUNT(DISTINCT DATE(created_at)) as active_days,
                        COUNT(CASE WHEN status = 'error' THEN 1 END) as errors,
                        COUNT(CASE WHEN action = 'login' THEN 1 END) as logins,
                        COUNT(CASE WHEN action = 'view_file' THEN 1 END) as file_views,
                        COUNT(CASE WHEN action = 'run_script' THEN 1 END) as scripts_run
                    FROM activity_logs
                    WHERE created_at >= DATE_SUB(NOW(), INTERVAL %s DAY)
                """
                params = [days]
                
                if user_id:
                    query = query.replace("WHERE", "WHERE user_id = %s AND")
                    params.insert(0, user_id)
                
                cursor.execute(query, params)
                result = cursor.fetchone()
                
                return dict(result) if result else {}
                
            finally:
                cursor.close()
                conn.close()
                
        except Exception as e:
            logger.error(f"Error getting activity summary: {e}")
            return {}
    
    @staticmethod
    def get_errors(
        limit: int = 100,
        user_id: Optional[int] = None,
        action: Optional[str] = None,
        resource_type: Optional[str] = None,
        search: Optional[str] = None,
        days: Optional[int] = None
    ) -> List[Dict[str, Any]]:
        """Retrieve error logs with enhanced formatting"""
        try:
            conn = get_db_connection()
            cursor = conn.cursor()
            
            try:
                query = "SELECT * FROM activity_logs WHERE status = 'error'"
                params = []
                
                if user_id:
                    query += " AND user_id = %s"
                    params.append(user_id)
                
                if action:
                    query += " AND action = %s"
                    params.append(action)
                
                if resource_type:
                    query += " AND resource_type = %s"
                    params.append(resource_type)
                
                if days:
                    query += " AND created_at >= %s"
                    params.append(datetime.now() - timedelta(days=days))
                
                query += " ORDER BY created_at DESC LIMIT %s"
                params.append(limit)
                
                cursor.execute(query, params)
                results = cursor.fetchall()
                
                errors = []
                for row in results:
                    error_dict = dict(row)
                    
                    # Format timestamp - handle multiple date formats
                    if error_dict.get('created_at'):
                        if isinstance(error_dict['created_at'], datetime):
                            error_dict['created_at_formatted'] = error_dict['created_at'].strftime('%Y-%m-%d %H:%M:%S')
                            error_dict['created_at'] = error_dict['created_at'].isoformat()
                        else:
                            # Try to parse various date formats
                            date_str = str(error_dict['created_at'])
                            error_dict['created_at'] = date_str
                            try:
                                # Try ISO format first
                                if 'T' in date_str or '+' in date_str or 'Z' in date_str:
                                    dt = datetime.fromisoformat(date_str.replace('Z', '+00:00'))
                                else:
                                    # Try common MySQL datetime format
                                    dt = datetime.strptime(date_str, '%Y-%m-%d %H:%M:%S')
                                error_dict['created_at_formatted'] = dt.strftime('%Y-%m-%d %H:%M:%S')
                            except:
                                # Fallback: use as-is or try to extract date parts
                                try:
                                    # Try to extract date from string using dateutil if available
                                    try:
                                        from dateutil import parser
                                        dt = parser.parse(date_str)
                                        error_dict['created_at_formatted'] = dt.strftime('%Y-%m-%d %H:%M:%S')
                                    except ImportError:
                                        # dateutil not available, use fallback
                                        error_dict['created_at_formatted'] = date_str[:19] if len(date_str) >= 19 else date_str
                                except:
                                    error_dict['created_at_formatted'] = date_str[:19] if len(date_str) >= 19 else date_str
                    else:
                        error_dict['created_at'] = ''
                        error_dict['created_at_formatted'] = 'Unknown'
                    
                    # Parse details JSON
                    details = {}
                    if error_dict.get('details'):
                        try:
                            if isinstance(error_dict['details'], str):
                                details = json.loads(error_dict['details'])
                            else:
                                details = error_dict['details']
                        except:
                            pass
                    error_dict['details'] = details
                    
                    # Extract traceback if available
                    error_dict['traceback'] = details.get('traceback', '')
                    
                    # Extract error type and context if available
                    error_dict['error_type'] = details.get('error_type', '')
                    error_dict['error_context'] = details.get('error_context', {})
                    
                    # Ensure action and resource_type are not None
                    error_dict['action'] = error_dict.get('action') or 'unknown'
                    error_dict['resource_type'] = error_dict.get('resource_type') or 'unknown'
                    
                    # Extract error message - try multiple sources
                    error_message = error_dict.get('error_message', '')
                    if not error_message:
                        # Try to extract from details
                        if details.get('error_message'):
                            error_message = details['error_message']
                        elif details.get('message'):
                            error_message = details['message']
                        elif error_dict.get('error_type'):
                            error_message = f"{error_dict['error_type']} occurred"
                    
                    error_dict['error_message'] = error_message or 'No error message available'
                    
                    # Categorize error (use details if available for better categorization)
                    error_dict['error_category'] = ActivityLogger._categorize_error(error_message, details)
                    error_dict['error_severity'] = ActivityLogger._get_severity(error_message, error_dict.get('action', ''), details)
                    
                    # If error_type is missing, try to infer from error_message
                    if not error_dict['error_type']:
                        error_dict['error_type'] = ActivityLogger._infer_error_type(error_message)
                    
                    # Search filter
                    if search:
                        search_lower = search.lower()
                        if (search_lower not in error_message.lower() and
                            search_lower not in str(details).lower() and
                            search_lower not in error_dict.get('action', '').lower()):
                            continue
                    
                    errors.append(error_dict)
                
                return errors
                
            finally:
                cursor.close()
                conn.close()
                
        except Exception as e:
            logger.error(f"Error retrieving errors: {e}")
            return []
    
    @staticmethod
    def _categorize_error(error_message: str, details: Optional[Dict[str, Any]] = None) -> str:
        """Categorize error based on message and details"""
        # First check if category is already in details (from custom exception)
        if details and isinstance(details, dict):
            if 'error_category' in details:
                return details['error_category']
        
        if not error_message:
            return 'General'
        
        error_lower = error_message.lower()
        
        # Check for specific Python error types
        if any(x in error_lower for x in ['syntaxerror', 'syntax error']):
            return 'Validation'
        elif any(x in error_lower for x in ['nameerror', 'name not found', 'is not defined']):
            return 'Validation'
        elif any(x in error_lower for x in ['typeerror', 'type error', 'unsupported operand']):
            return 'Validation'
        elif any(x in error_lower for x in ['valueerror', 'value error', 'invalid value']):
            return 'Validation'
        elif any(x in error_lower for x in ['indexerror', 'index out of range', 'list index']):
            return 'Validation'
        elif any(x in error_lower for x in ['keyerror', 'key not found']):
            return 'Validation'
        elif any(x in error_lower for x in ['attributeerror', 'attribute not found', 'has no attribute']):
            return 'Validation'
        elif any(x in error_lower for x in ['zerodivisionerror', 'division by zero']):
            return 'Validation'
        # Application-specific errors
        elif any(x in error_lower for x in ['connection', 'timeout', 'network', 'dns', 'connectionerror']):
            return 'Network'
        elif any(x in error_lower for x in ['database', 'sql', 'query', 'mysql', 'databaseerror']):
            return 'Database'
        elif any(x in error_lower for x in ['authentication', 'login', 'password', 'unauthorized', 'authenticationerror']):
            return 'Authentication'
        elif any(x in error_lower for x in ['permission', 'forbidden', 'access denied', 'permissionerror']):
            return 'Permission'
        elif any(x in error_lower for x in ['file', 'directory', 'path', 'not found', 'filenotfounderror', 'filesystemerror']):
            return 'File System'
        elif any(x in error_lower for x in ['validation', 'invalid', 'format']):
            return 'Validation'
        elif any(x in error_lower for x in ['memory', 'out of memory', 'resourceerror']):
            return 'Resource'
        else:
            return 'General'
    
    @staticmethod
    def _infer_error_type(error_message: str) -> str:
        """Infer error type from error message"""
        if not error_message:
            return 'UnknownError'
        
        error_lower = error_message.lower()
        
        # Check for common error patterns
        if 'unicodenencodeerror' in error_lower or 'unicode' in error_lower:
            return 'UnicodeEncodeError'
        elif 'filenotfounderror' in error_lower or 'file not found' in error_lower:
            return 'FileNotFoundError'
        elif 'permissionerror' in error_lower or 'permission denied' in error_lower:
            return 'PermissionError'
        elif 'connectionerror' in error_lower or 'connection' in error_lower:
            return 'ConnectionError'
        elif 'timeouterror' in error_lower or 'timeout' in error_lower:
            return 'TimeoutError'
        elif 'valueerror' in error_lower:
            return 'ValueError'
        elif 'typeerror' in error_lower:
            return 'TypeError'
        elif 'keyerror' in error_lower:
            return 'KeyError'
        elif 'attributeerror' in error_lower:
            return 'AttributeError'
        elif 'indexerror' in error_lower:
            return 'IndexError'
        elif 'zerodivisionerror' in error_lower or 'division by zero' in error_lower:
            return 'ZeroDivisionError'
        elif 'syntaxerror' in error_lower:
            return 'SyntaxError'
        elif 'nameerror' in error_lower:
            return 'NameError'
        else:
            # Extract from common patterns
            match = re.search(r'(\w+Error|\w+Exception)', error_message)
            if match:
                return match.group(1)
            return 'Exception'
    
    @staticmethod
    def _get_severity(error_message: str, action: str, details: Optional[Dict[str, Any]] = None) -> str:
        """Determine error severity"""
        # First check if severity is already in details (from custom exception)
        if details and isinstance(details, dict):
            if 'error_severity' in details:
                return details['error_severity']
        
        if not error_message:
            return 'low'
        error_lower = error_message.lower()
        action_lower = action.lower() if action else ''
        
        # Check for critical errors
        if any(x in error_lower for x in ['critical', 'fatal', 'crash', 'cannot connect', 'databaseerror', 'zerodivisionerror']):
            return 'critical'
        # Check for high severity errors
        elif any(x in error_lower for x in ['syntaxerror', 'nameerror', 'typeerror', 'authenticationerror', 
                                             'permissionerror', 'filesystemerror', 'networkerror']):
            return 'high'
        elif any(x in error_lower for x in ['failed', 'error', 'exception']):
            return 'high'
        # Check for medium severity errors
        elif any(x in error_lower for x in ['valueerror', 'indexerror', 'keyerror', 'attributeerror', 
                                            'warning', 'timeout', 'retry']):
            return 'medium'
        else:
            return 'low'
    
    @staticmethod
    def get_error_by_id(error_id: int, user_id: Optional[int] = None) -> Optional[Dict[str, Any]]:
        """Get a specific error by ID"""
        try:
            conn = get_db_connection()
            cursor = conn.cursor()
            
            try:
                query = "SELECT * FROM activity_logs WHERE id = %s AND status = 'error'"
                params = [error_id]
                
                if user_id:
                    query += " AND user_id = %s"
                    params.append(user_id)
                
                cursor.execute(query, params)
                result = cursor.fetchone()
                
                if result:
                    error_dict = dict(result)
                    
                    # Format timestamp - handle multiple date formats
                    if error_dict.get('created_at'):
                        if isinstance(error_dict['created_at'], datetime):
                            error_dict['created_at_formatted'] = error_dict['created_at'].strftime('%Y-%m-%d %H:%M:%S')
                            error_dict['created_at'] = error_dict['created_at'].isoformat()
                        else:
                            date_str = str(error_dict['created_at'])
                            error_dict['created_at'] = date_str
                            try:
                                if 'T' in date_str or '+' in date_str or 'Z' in date_str:
                                    dt = datetime.fromisoformat(date_str.replace('Z', '+00:00'))
                                else:
                                    dt = datetime.strptime(date_str, '%Y-%m-%d %H:%M:%S')
                                error_dict['created_at_formatted'] = dt.strftime('%Y-%m-%d %H:%M:%S')
                            except:
                                try:
                                    from dateutil import parser
                                    dt = parser.parse(date_str)
                                    error_dict['created_at_formatted'] = dt.strftime('%Y-%m-%d %H:%M:%S')
                                except:
                                    error_dict['created_at_formatted'] = date_str[:19] if len(date_str) >= 19 else date_str
                    else:
                        error_dict['created_at'] = ''
                        error_dict['created_at_formatted'] = 'Unknown'
                    
                    # Parse details
                    details = {}
                    if error_dict.get('details'):
                        try:
                            if isinstance(error_dict['details'], str):
                                details = json.loads(error_dict['details'])
                            else:
                                details = error_dict['details']
                        except:
                            pass
                    error_dict['details'] = details
                    
                    # Ensure action and resource_type are not None
                    error_dict['action'] = error_dict.get('action') or 'unknown'
                    error_dict['resource_type'] = error_dict.get('resource_type') or 'unknown'
                    
                    # Extract error message - try multiple sources
                    error_message = error_dict.get('error_message', '')
                    if not error_message:
                        if details.get('error_message'):
                            error_message = details['error_message']
                        elif details.get('message'):
                            error_message = details['message']
                        elif error_dict.get('error_type'):
                            error_message = f"{error_dict['error_type']} occurred"
                    
                    error_dict['error_message'] = error_message or 'No error message available'
                    
                    error_dict['traceback'] = details.get('traceback', '')
                    error_dict['error_type'] = details.get('error_type', '')
                    error_dict['error_context'] = details.get('error_context', {})
                    
                    # If error_type is missing, try to infer from error_message
                    if not error_dict['error_type']:
                        error_dict['error_type'] = ActivityLogger._infer_error_type(error_message)
                    
                    error_dict['error_category'] = ActivityLogger._categorize_error(error_message, details)
                    error_dict['error_severity'] = ActivityLogger._get_severity(error_message, error_dict.get('action', ''), details)
                    
                    return error_dict
                
                return None
                
            finally:
                cursor.close()
                conn.close()
                
        except Exception as e:
            logger.error(f"Error retrieving error by ID: {e}")
            return None
    
    @staticmethod
    def get_error_statistics(user_id: Optional[int] = None, days: int = 7) -> Dict[str, Any]:
        """Get error statistics"""
        try:
            conn = get_db_connection()
            cursor = conn.cursor()
            
            try:
                query = """
                    SELECT 
                        COUNT(*) as total_errors,
                        COUNT(DISTINCT DATE(created_at)) as error_days,
                        COUNT(DISTINCT action) as unique_actions,
                        COUNT(DISTINCT resource_type) as unique_resources,
                        COUNT(CASE WHEN created_at >= DATE_SUB(NOW(), INTERVAL 1 DAY) THEN 1 END) as errors_today,
                        COUNT(CASE WHEN created_at >= DATE_SUB(NOW(), INTERVAL 7 DAY) THEN 1 END) as errors_week
                    FROM activity_logs
                    WHERE status = 'error' AND created_at >= DATE_SUB(NOW(), INTERVAL %s DAY)
                """
                params = [days]
                
                if user_id:
                    query = query.replace("WHERE", "WHERE user_id = %s AND")
                    params.insert(0, user_id)
                
                cursor.execute(query, params)
                result = cursor.fetchone()
                
                # Get errors by category
                category_query = """
                    SELECT 
                        CASE 
                            WHEN error_message LIKE '%%connection%%' OR error_message LIKE '%%timeout%%' THEN 'Network'
                            WHEN error_message LIKE '%%database%%' OR error_message LIKE '%%sql%%' THEN 'Database'
                            WHEN error_message LIKE '%%auth%%' OR error_message LIKE '%%login%%' THEN 'Authentication'
                            WHEN error_message LIKE '%%file%%' OR error_message LIKE '%%path%%' THEN 'File System'
                            ELSE 'General'
                        END as category,
                        COUNT(*) as count
                    FROM activity_logs
                    WHERE status = 'error' AND created_at >= DATE_SUB(NOW(), INTERVAL %s DAY)
                """
                category_params = [days]
                
                if user_id:
                    category_query = category_query.replace("WHERE", "WHERE user_id = %s AND")
                    category_params.insert(0, user_id)
                
                category_query += " GROUP BY category ORDER BY count DESC"
                cursor.execute(category_query, category_params)
                categories = cursor.fetchall()
                
                stats = dict(result) if result else {}
                stats['by_category'] = [dict(cat) for cat in categories]
                
                return stats
                
            finally:
                cursor.close()
                conn.close()
                
        except Exception as e:
            logger.error(f"Error getting error statistics: {e}")
            return {}


def get_client_ip(request) -> Optional[str]:
    """Extract client IP address from request"""
    try:
        # Check for forwarded IP (when behind proxy)
        forwarded = request.headers.get("X-Forwarded-For")
        if forwarded:
            return forwarded.split(",")[0].strip()
        
        # Check for real IP
        real_ip = request.headers.get("X-Real-IP")
        if real_ip:
            return real_ip
        
        # Fallback to client host
        if hasattr(request, "client") and request.client:
            return request.client.host
        
        return None
    except:
        return None


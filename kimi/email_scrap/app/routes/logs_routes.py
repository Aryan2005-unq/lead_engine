from fastapi import APIRouter, HTTPException, Request
from app.auth import get_current_user
from app.logging_system import ActivityLogger, get_client_ip
from app.exceptions import BaseAppException, DatabaseError, format_exception_for_logging, ValueError as AppValueError
from typing import Optional
import traceback

router = APIRouter()

@router.get("/api/logs")
async def get_logs(
    request: Request,
    limit: int = 100,
    action: Optional[str] = None,
    resource_type: Optional[str] = None,
    status: Optional[str] = None
):
    """Get activity logs"""
    try:
        user = get_current_user(request)
        
        logs = ActivityLogger.get_logs(
            limit=limit,
            user_id=user.get("id"),
            action=action,
            resource_type=resource_type,
            status=status
        )
        
        return {"logs": logs, "count": len(logs)}
    except BaseAppException as e:
        # Log custom exception
        ActivityLogger.log_activity(
            user_id=None,
            user_email=None,
            action="get_logs",
            resource_type="logs",
            status="error",
            error_message=str(e),
            error_traceback=traceback.format_exc(),
            ip_address=get_client_ip(request),
            exception=e
        )
        raise HTTPException(status_code=500, detail=str(e))
    except Exception as e:
        # Log unexpected exception
        ActivityLogger.log_activity(
            user_id=None,
            user_email=None,
            action="get_logs",
            resource_type="logs",
            status="error",
            error_message=str(e),
            error_traceback=traceback.format_exc(),
            ip_address=get_client_ip(request),
            exception=e
        )
        raise HTTPException(status_code=500, detail="Internal server error")

@router.get("/api/logs/summary")
async def get_logs_summary(request: Request, days: int = 7):
    """Get activity summary statistics"""
    try:
        user = get_current_user(request)
        
        summary = ActivityLogger.get_user_activity_summary(
            user_id=user.get("id"),
            days=days
        )
        
        return {"summary": summary}
    except BaseAppException as e:
        ActivityLogger.log_activity(
            user_id=None,
            user_email=None,
            action="get_logs_summary",
            resource_type="logs",
            status="error",
            error_message=str(e),
            error_traceback=traceback.format_exc(),
            ip_address=get_client_ip(request),
            exception=e
        )
        raise HTTPException(status_code=500, detail=str(e))
    except Exception as e:
        ActivityLogger.log_activity(
            user_id=None,
            user_email=None,
            action="get_logs_summary",
            resource_type="logs",
            status="error",
            error_message=str(e),
            error_traceback=traceback.format_exc(),
            ip_address=get_client_ip(request),
            exception=e
        )
        raise HTTPException(status_code=500, detail="Internal server error")

@router.get("/api/errors")
async def get_errors(
    request: Request,
    limit: int = 100,
    action: Optional[str] = None,
    resource_type: Optional[str] = None,
    search: Optional[str] = None,
    days: Optional[int] = None
):
    """Get error logs with enhanced formatting for debugging - Public access"""
    try:
        # Try to get user, but don't require it
        user = None
        try:
            user = get_current_user(request)
        except:
            pass
        
        errors = ActivityLogger.get_errors(
            limit=limit,
            user_id=user.get("id") if user else None,
            action=action,
            resource_type=resource_type,
            search=search,
            days=days
        )
        
        return {"errors": errors, "count": len(errors)}
    except BaseAppException as e:
        ActivityLogger.log_activity(
            user_id=None,
            user_email=None,
            action="get_errors",
            resource_type="errors",
            status="error",
            error_message=str(e),
            error_traceback=traceback.format_exc(),
            ip_address=get_client_ip(request),
            exception=e
        )
        raise HTTPException(status_code=500, detail=str(e))
    except Exception as e:
        ActivityLogger.log_activity(
            user_id=None,
            user_email=None,
            action="get_errors",
            resource_type="errors",
            status="error",
            error_message=str(e),
            error_traceback=traceback.format_exc(),
            ip_address=get_client_ip(request),
            exception=e
        )
        raise HTTPException(status_code=500, detail="Internal server error")

@router.get("/api/errors/stats")
async def get_error_stats(request: Request, days: int = 7):
    """Get error statistics for dashboard - Public access"""
    try:
        # Try to get user, but don't require it
        user = None
        try:
            user = get_current_user(request)
        except:
            pass
        
        stats = ActivityLogger.get_error_statistics(
            user_id=user.get("id") if user else None,
            days=days
        )
        
        return {"stats": stats}
    except BaseAppException as e:
        ActivityLogger.log_activity(
            user_id=None,
            user_email=None,
            action="get_error_stats",
            resource_type="errors",
            status="error",
            error_message=str(e),
            error_traceback=traceback.format_exc(),
            ip_address=get_client_ip(request),
            exception=e
        )
        raise HTTPException(status_code=500, detail=str(e))
    except Exception as e:
        ActivityLogger.log_activity(
            user_id=None,
            user_email=None,
            action="get_error_stats",
            resource_type="errors",
            status="error",
            error_message=str(e),
            error_traceback=traceback.format_exc(),
            ip_address=get_client_ip(request),
            exception=e
        )
        raise HTTPException(status_code=500, detail="Internal server error")

@router.get("/api/errors/{error_id}")
async def get_error_details(request: Request, error_id: int):
    """Get detailed information about a specific error - Public access"""
    try:
        # Try to get user, but don't require it
        user = None
        try:
            user = get_current_user(request)
        except:
            pass
        
        error = ActivityLogger.get_error_by_id(error_id, user_id=user.get("id") if user else None)
        
        if not error:
            raise AppValueError(
                f"Error with ID {error_id} not found",
                context={"error_id": error_id, "user_id": user.get("id") if user else None}
            )
        
        # Ensure all fields are properly set with defaults
        error.setdefault('error_message', 'No error message available')
        error.setdefault('action', 'unknown')
        error.setdefault('resource_type', 'unknown')
        error.setdefault('error_category', 'General')
        error.setdefault('error_severity', 'low')
        error.setdefault('error_type', '')
        error.setdefault('error_context', {})
        error.setdefault('traceback', '')
        error.setdefault('details', {})
        error.setdefault('created_at_formatted', 'Unknown')
        error.setdefault('user_email', None)
        error.setdefault('ip_address', None)
        error.setdefault('resource_id', None)
        
        # Ensure details is a dict if it's a string
        if isinstance(error.get('details'), str):
            try:
                import json
                error['details'] = json.loads(error['details'])
            except:
                error['details'] = {}
        
        # Extract nested data from details if needed
        if isinstance(error.get('details'), dict):
            details = error['details']
            if not error.get('error_type') and details.get('error_type'):
                error['error_type'] = details['error_type']
            if not error.get('error_context') and details.get('error_context'):
                error['error_context'] = details['error_context']
            if not error.get('traceback') and details.get('traceback'):
                error['traceback'] = details['traceback']
        
        return {"error": error}
    except AppValueError as e:
        ActivityLogger.log_activity(
            user_id=None,
            user_email=None,
            action="get_error_details",
            resource_type="errors",
            resource_id=str(error_id),
            status="error",
            error_message=str(e),
            error_traceback=traceback.format_exc(),
            ip_address=get_client_ip(request),
            exception=e
        )
        raise HTTPException(status_code=404, detail=str(e))
    except BaseAppException as e:
        ActivityLogger.log_activity(
            user_id=None,
            user_email=None,
            action="get_error_details",
            resource_type="errors",
            resource_id=str(error_id),
            status="error",
            error_message=str(e),
            error_traceback=traceback.format_exc(),
            ip_address=get_client_ip(request),
            exception=e
        )
        raise HTTPException(status_code=500, detail=str(e))
    except Exception as e:
        ActivityLogger.log_activity(
            user_id=None,
            user_email=None,
            action="get_error_details",
            resource_type="errors",
            resource_id=str(error_id),
            status="error",
            error_message=str(e),
            error_traceback=traceback.format_exc(),
            ip_address=get_client_ip(request),
            exception=e
        )
        raise HTTPException(status_code=500, detail="Internal server error")


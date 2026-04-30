from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import JSONResponse, HTMLResponse
from fastapi.staticfiles import StaticFiles
from starlette.templating import Jinja2Templates
from fastapi.exceptions import RequestValidationError
from starlette.middleware.sessions import SessionMiddleware
from starlette.exceptions import HTTPException as StarletteHTTPException
from app.database import init_db
from app.routes import auth_routes, file_routes, script_routes, logs_routes, email_routes, distribution_routes, pipeline_routes, queue_monitor_routes
from app.auth import get_current_user
from app.logging_system import ActivityLogger, get_client_ip
from app.exceptions import BaseAppException, format_exception_for_logging
import traceback
import os

app = FastAPI(
    title="CRWM Email Scraping API",
    description="Backend API for CRWM Email Scraping System",
    version="1.0.0"
)

# Setup templates and static files
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
templates = Jinja2Templates(directory=os.path.join(BASE_DIR, "templates"))
app.mount("/static", StaticFiles(directory=os.path.join(BASE_DIR, "static")), name="static")

from fastapi.middleware.cors import CORSMiddleware

SECRET_KEY = os.getenv("SECRET_KEY", "your-secret-key-change-in-production")
app.add_middleware(SessionMiddleware, secret_key=SECRET_KEY)

# 🌐 CORS Mitigation (Allow dashboard domains access safely)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], 
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


app.include_router(auth_routes.router)
app.include_router(file_routes.router)
app.include_router(script_routes.router)
app.include_router(logs_routes.router)
app.include_router(email_routes.router)
app.include_router(distribution_routes.router)
app.include_router(pipeline_routes.router)
app.include_router(queue_monitor_routes.router)


# 🌐 Mount frontend after API routes so it acts as fallback only
# app.mount("/", StaticFiles(directory=os.path.join(BASE_DIR, "frontend"), html=True), name="frontend")


@app.on_event("startup")
async def startup():
    from app.services.pipeline_service import init_pipeline_state
    init_db()
    init_pipeline_state() # Trigger Background Task if continuous enabled

@app.get("/", response_class=HTMLResponse)
async def root():
    """Serve the main dashboard page."""
    from fastapi.responses import FileResponse
    file_path = os.path.join(BASE_DIR, "templates", "index.html")
    if os.path.exists(file_path):
        return FileResponse(file_path)
    return HTMLResponse(content="<h1>Index Not Found</h1>", status_code=404)

@app.get("/favicon.ico")
async def favicon():
    """Handle favicon requests to prevent 404 errors in logs."""
    from fastapi.responses import Response
    return Response(status_code=204)

from fastapi import Depends

@app.get("/errors", response_class=HTMLResponse)
async def error_dashboard(
    request: Request,
    current_user_dict: dict = Depends(get_current_user)
):
    """Error log dashboard page - Protected Admin access only"""
    if current_user_dict.get("role") != 'admin':
        raise HTTPException(status_code=403, detail="Requires Admin privileges")
    return templates.TemplateResponse("error_dashboard.html", {"request": request})



# ============================================
# Global Exception Handlers
# ============================================

@app.exception_handler(BaseAppException)
async def custom_exception_handler(request: Request, exc: BaseAppException):
    """Handle custom application exceptions"""
    try:
        # Get user info if available
        user = None
        try:
            user = get_current_user(request)
        except:
            pass
        
        # Log the exception
        ActivityLogger.log_activity(
            user_id=user.get("id") if user else None,
            user_email=user.get("email") if user else None,
            action=request.url.path.split("/")[-1] or "unknown",
            resource_type="api",
            status="error",
            error_message=str(exc),
            error_traceback=traceback.format_exc(),
            ip_address=get_client_ip(request),
            exception=exc
        )
        
        # Return formatted error response
        error_info = exc.to_dict()
        return JSONResponse(
            status_code=500,
            content={
                "error": True,
                "error_type": error_info["error_type"],
                "message": error_info["message"],
                "category": error_info["category"],
                "severity": error_info["severity"],
                "context": error_info["context"]
            }
        )
    except Exception as e:
        # Fallback if logging fails
        return JSONResponse(
            status_code=500,
            content={
                "error": True,
                "error_type": type(exc).__name__,
                "message": str(exc)
            }
        )


@app.exception_handler(RequestValidationError)
async def validation_exception_handler(request: Request, exc: RequestValidationError):
    """Handle request validation errors"""
    try:
        user = None
        try:
            user = get_current_user(request)
        except:
            pass
        
        from app.exceptions import ValueError as AppValueError
        validation_error = AppValueError(
            "Request validation failed",
            context={"errors": exc.errors()}
        )
        
        ActivityLogger.log_activity(
            user_id=user.get("id") if user else None,
            user_email=user.get("email") if user else None,
            action=request.url.path.split("/")[-1] or "unknown",
            resource_type="api",
            status="error",
            error_message=str(validation_error),
            error_traceback=str(exc.errors()),
            ip_address=get_client_ip(request),
            exception=validation_error
        )
        
        return JSONResponse(
            status_code=422,
            content={
                "error": True,
                "error_type": "ValidationError",
                "message": "Request validation failed",
                "category": "Validation",
                "severity": "medium",
                "details": exc.errors()
            }
        )
    except:
        return JSONResponse(
            status_code=422,
            content={"error": True, "message": "Validation error", "details": exc.errors()}
        )


@app.exception_handler(StarletteHTTPException)
async def http_exception_handler(request: Request, exc: StarletteHTTPException):
    """Handle HTTP exceptions"""
    try:
        user = None
        try:
            user = get_current_user(request)
        except:
            pass
        
        # Only log server errors (5xx), not client errors (4xx)
        if exc.status_code >= 500:
            from app.exceptions import NetworkError
            http_error = NetworkError(
                f"HTTP {exc.status_code}: {exc.detail}",
                context={"status_code": exc.status_code, "path": str(request.url)}
            )
            
            ActivityLogger.log_activity(
                user_id=user.get("id") if user else None,
                user_email=user.get("email") if user else None,
                action=request.url.path.split("/")[-1] or "unknown",
                resource_type="api",
                status="error",
                error_message=str(http_error),
                error_traceback=traceback.format_exc(),
                ip_address=get_client_ip(request),
                exception=http_error
            )
        
        return JSONResponse(
            status_code=exc.status_code,
            content={
                "error": True,
                "error_type": "HTTPException",
                "message": exc.detail,
                "status_code": exc.status_code
            }
        )
    except:
        return JSONResponse(
            status_code=exc.status_code,
            content={"error": True, "message": exc.detail}
        )


@app.exception_handler(Exception)
async def general_exception_handler(request: Request, exc: Exception):
    """Handle all other unhandled exceptions"""
    try:
        user = None
        try:
            user = get_current_user(request)
        except:
            pass
        
        # Format exception for logging
        error_info = format_exception_for_logging(exc)
        
        ActivityLogger.log_activity(
            user_id=user.get("id") if user else None,
            user_email=user.get("email") if user else None,
            action=request.url.path.split("/")[-1] or "unknown",
            resource_type="api",
            status="error",
            error_message=str(exc),
            error_traceback=traceback.format_exc(),
            ip_address=get_client_ip(request),
            exception=exc
        )
        
        return JSONResponse(
            status_code=500,
            content={
                "error": True,
                "error_type": error_info["error_type"],
                "message": error_info["message"],
                "category": error_info["category"],
                "severity": error_info["severity"]
            }
        )
    except:
        # Ultimate fallback
        return JSONResponse(
            status_code=500,
            content={"error": True, "message": "Internal server error"}
        )


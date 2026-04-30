from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import FileResponse, StreamingResponse
from app.auth import get_current_user
from app.logging_system import ActivityLogger, get_client_ip
import os
import csv
import json

router = APIRouter()

BASE_DIR = "Update-robocall-leads"

ALLOWED_DIRS = ["All", "verify", "logs", "Merging"]

@router.get("/api/files/list")
async def list_files(request: Request, directory: str = ""):
    user = get_current_user(request)
    ip_address = get_client_ip(request)
    
    if directory:
        parts = directory.split("/")
        if parts[0] not in ALLOWED_DIRS:
            raise HTTPException(status_code=400, detail="Invalid directory")
        target_path = os.path.join(BASE_DIR, directory)
    else:
        target_path = BASE_DIR
    
    if not os.path.exists(target_path):
        return {"files": [], "directories": []}
    
    if not os.path.abspath(target_path).startswith(os.path.abspath(BASE_DIR)):
        raise HTTPException(status_code=403, detail="Access denied")
    
    files = []
    directories = []
    
    try:
        for item in os.listdir(target_path):
            item_path = os.path.join(target_path, item)
            if os.path.isdir(item_path):
                directories.append({
                    "name": item,
                    "type": "directory"
                })
            else:
                files.append({
                    "name": item,
                    "type": "file",
                    "size": os.path.getsize(item_path)
                })
    except Exception as e:
        ActivityLogger.log_activity(
            user_id=user.get("id"),
            user_email=user.get("email"),
            action="list_files",
            resource_type="file",
            resource_id=directory,
            status="error",
            error_message=str(e),
            ip_address=ip_address
        )
        raise HTTPException(status_code=500, detail=str(e))
    
    ActivityLogger.log_activity(
        user_id=user.get("id"),
        user_email=user.get("email"),
        action="list_files",
        resource_type="file",
        resource_id=directory,
        details={"file_count": len(files), "directory_count": len(directories)},
        ip_address=ip_address
    )
    
    return {"files": files, "directories": directories, "path": directory}

@router.get("/api/files/view")
async def view_file(request: Request, path: str):
    user = get_current_user(request)
    ip_address = get_client_ip(request)
    
    full_path = os.path.join(BASE_DIR, path)
    
    if not os.path.exists(full_path):
        raise HTTPException(status_code=404, detail="File not found")
    
    if os.path.isdir(full_path):
        raise HTTPException(status_code=400, detail="Path is a directory")
    
    if not os.path.abspath(full_path).startswith(os.path.abspath(BASE_DIR)):
        raise HTTPException(status_code=403, detail="Access denied")
    
    ext = os.path.splitext(full_path)[1].lower()
    
    if ext == ".csv":
        try:
            with open(full_path, "r", encoding="utf-8") as f:
                reader = csv.DictReader(f)
                rows = list(reader)
                ActivityLogger.log_activity(
                    user_id=user.get("id"),
                    user_email=user.get("email"),
                    action="view_file",
                    resource_type="file",
                    resource_id=path,
                    details={"file_type": "csv", "row_count": len(rows)},
                    ip_address=ip_address
                )
                return {"type": "csv", "data": rows, "headers": list(rows[0].keys()) if rows else []}
        except Exception as e:
            ActivityLogger.log_activity(
                user_id=user.get("id"),
                user_email=user.get("email"),
                action="view_file",
                resource_type="file",
                resource_id=path,
                status="error",
                error_message=str(e),
                ip_address=ip_address
            )
            return {"type": "text", "data": f"Error reading CSV: {str(e)}"}
    
    elif ext in [".txt", ".log"]:
        try:
            with open(full_path, "r", encoding="utf-8") as f:
                content = f.read()
                ActivityLogger.log_activity(
                    user_id=user.get("id"),
                    user_email=user.get("email"),
                    action="view_file",
                    resource_type="file",
                    resource_id=path,
                    details={"file_type": ext, "file_size": len(content)},
                    ip_address=ip_address
                )
                return {"type": "text", "data": content}
        except Exception as e:
            ActivityLogger.log_activity(
                user_id=user.get("id"),
                user_email=user.get("email"),
                action="view_file",
                resource_type="file",
                resource_id=path,
                status="error",
                error_message=str(e),
                ip_address=ip_address
            )
            return {"type": "text", "data": f"Error reading file: {str(e)}"}
    
    else:
        ActivityLogger.log_activity(
            user_id=user.get("id"),
            user_email=user.get("email"),
            action="view_file",
            resource_type="file",
            resource_id=path,
            details={"file_type": ext},
            ip_address=ip_address
        )
        return FileResponse(full_path)

@router.get("/api/files/download")
async def download_file(request: Request, path: str):
    get_current_user(request)
    
    full_path = os.path.join(BASE_DIR, path)
    
    if not os.path.exists(full_path):
        raise HTTPException(status_code=404, detail="File not found")
    
    if not os.path.abspath(full_path).startswith(os.path.abspath(BASE_DIR)):
        raise HTTPException(status_code=403, detail="Access denied")
    
    return FileResponse(full_path, filename=os.path.basename(full_path))


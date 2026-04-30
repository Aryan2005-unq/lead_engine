from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel
from typing import Optional, Dict, List
from app.auth import get_current_user
from app.logging_system import ActivityLogger, get_client_ip
import subprocess
import os
import threading
import json
import traceback

router = APIRouter()

BASE_DIR = "Update-robocall-leads"
script_status = {}

class ScriptRequest(BaseModel):
    script_name: str
    args: Optional[Dict[str, str]] = None

@router.post("/api/scripts/run")
async def run_script(request: Request, script_req: ScriptRequest):
    script_name = script_req.script_name
    user = get_current_user(request)
    ip_address = get_client_ip(request)
    
    # Define scripts with their required arguments
    scripts_config = {
        "fetch_emails": {
            "path": os.path.join(BASE_DIR, "All", "fetch_emails", "fetch_emails_browser.py"),
            "required_args": [
                {"name": "verified_csv", "label": "Verified CSV", "example": "All/usa_list/usa_list.csv"},
                {"name": "listings_csv", "label": "Listings CSV", "example": "All/All_list/listings.csv"},
                {"name": "output_csv", "label": "Output CSV", "example": "All/fetch_emails/usa_email.csv"}
            ],
            "cwd": os.path.join(BASE_DIR, "All", "fetch_emails")
        },
        "verify_emails": {
            "path": os.path.join(BASE_DIR, "verify", "verify_csv.py"),
            "required_args": [],
            "cwd": os.path.join(BASE_DIR, "verify")
        },
        "merge_csv": {
            "path": os.path.join(BASE_DIR, "Merging", "all_merged_csv.py"),
            "required_args": [],
            "cwd": os.path.join(BASE_DIR, "Merging")
        },
        "usa_separation": {
            "path": os.path.join(BASE_DIR, "All", "usa_list", "verify_leads_fast.py"),
            "required_args": [
                {"name": "input_csv", "label": "Input CSV", "example": "All/All_list/listings.csv"},
                {"name": "output_csv", "label": "Output CSV", "example": "All/usa_list/usa_list.csv"}
            ],
            "cwd": os.path.join(BASE_DIR, "All", "usa_list")
        },
        "non_usa_separation": {
            "path": os.path.join(BASE_DIR, "All", "non_usa_list", "non_usa_list_creater.py"),
            "required_args": [],
            "cwd": os.path.join(BASE_DIR, "All", "non_usa_list")
        }
    }
    
    if script_name not in scripts_config:
        ActivityLogger.log_activity(
            user_id=user.get("id"),
            user_email=user.get("email"),
            action="run_script",
            resource_type="script",
            resource_id=script_name,
            status="error",
            error_message="Invalid script name",
            ip_address=ip_address
        )
        raise HTTPException(status_code=400, detail="Invalid script name")
    
    script_info = scripts_config[script_name]
    
    # Get required args list (handle both old format and new format)
    required_args_config = script_info.get("required_args", [])
    if required_args_config and isinstance(required_args_config[0], dict):
        # New format with metadata
        required_args = [arg["name"] for arg in required_args_config]
    else:
        # Old format (just list of names)
        required_args = required_args_config
    
    # Validate required arguments
    if required_args:
        if not script_req.args:
            raise HTTPException(
                status_code=400, 
                detail=f"Script requires arguments: {', '.join(required_args)}"
            )
        for arg_name in required_args:
            if arg_name not in script_req.args or not script_req.args[arg_name]:
                raise HTTPException(
                    status_code=400,
                    detail=f"Missing required argument: {arg_name}"
                )
    
    # Build script arguments list and resolve relative paths
    script_args = []
    if script_req.args:
        # Convert dict to positional arguments in the order defined
        for arg_name in required_args:
            if arg_name in script_req.args:
                arg_value = script_req.args[arg_name].strip()
                
                # Normalize path separators
                normalized_value = arg_value.replace("\\", "/")
                
                # If it's an absolute path, use as-is
                if os.path.isabs(arg_value):
                    script_args.append(arg_value)
                # If it contains directory separators, resolve relative to BASE_DIR
                elif "/" in normalized_value:
                    # Paths with directories should be relative to BASE_DIR
                    base_path = os.path.join(BASE_DIR, normalized_value)
                    if os.path.exists(base_path):
                        # Calculate relative path from script's cwd to the file
                        rel_path = os.path.relpath(base_path, script_info["cwd"])
                        script_args.append(rel_path.replace("\\", "/"))
                    else:
                        # Try as absolute path from project root
                        project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
                        full_path = os.path.join(project_root, BASE_DIR, normalized_value)
                        if os.path.exists(full_path):
                            rel_path = os.path.relpath(full_path, script_info["cwd"])
                            script_args.append(rel_path.replace("\\", "/"))
                        else:
                            # Use as-is, let the script handle it
                            script_args.append(normalized_value)
                else:
                    # Simple filename - check in cwd first, then BASE_DIR
                    cwd_path = os.path.join(script_info["cwd"], arg_value)
                    if os.path.exists(cwd_path):
                        script_args.append(arg_value)
                    else:
                        # Try in BASE_DIR
                        base_path = os.path.join(BASE_DIR, arg_value)
                        if os.path.exists(base_path):
                            # Calculate relative path from cwd to the file
                            rel_path = os.path.relpath(base_path, script_info["cwd"])
                            script_args.append(rel_path.replace("\\", "/"))
                        else:
                            # Use as-is
                            script_args.append(arg_value)
    
    if not os.path.exists(script_info["path"]):
        ActivityLogger.log_activity(
            user_id=user.get("id"),
            user_email=user.get("email"),
            action="run_script",
            resource_type="script",
            resource_id=script_name,
            status="error",
            error_message=f"Script not found: {script_info['path']}",
            ip_address=ip_address
        )
        raise HTTPException(status_code=404, detail="Script not found")
    
    if script_name in script_status and script_status[script_name].get("running"):
        ActivityLogger.log_activity(
            user_id=user.get("id"),
            user_email=user.get("email"),
            action="run_script",
            resource_type="script",
            resource_id=script_name,
            status="error",
            error_message="Script is already running",
            ip_address=ip_address
        )
        raise HTTPException(status_code=400, detail="Script is already running")
    
    script_status[script_name] = {
        "running": True,
        "output": [],
        "error": None
    }
    
    # Store user info for error logging in thread
    user_id = user.get("id")
    user_email = user.get("email")
    
    def run_script_thread():
        try:
            # If cwd is set, use just the basename to prevent path duplication
            if script_info["cwd"]:
                script_to_run = os.path.basename(script_info["path"])
            else:
                script_to_run = script_info["path"]
            
            process = subprocess.Popen(
                ["python", script_to_run] + script_args,
                cwd=script_info["cwd"],
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                bufsize=1,
                universal_newlines=True
            )
            
            # Collect output lines
            output_lines = []
            for line in process.stdout:
                line_stripped = line.strip()
                output_lines.append(line_stripped)
                script_status[script_name]["output"].append(line_stripped)
            
            process.wait()
            script_status[script_name]["running"] = False
            script_status[script_name]["exit_code"] = process.returncode
            
            # Log error if script failed (non-zero exit code)
            if process.returncode != 0:
                error_output = "\n".join(output_lines[-50:])  # Last 50 lines for context
                
                # Extract concise error message from output
                concise_error = None
                if error_output:
                    # Look for common error patterns
                    lines = error_output.split('\n')
                    for line in reversed(lines):
                        if any(keyword in line.lower() for keyword in ['error', 'exception', 'failed', 'traceback']):
                            concise_error = line.strip()
                            break
                
                error_message = concise_error or f"Script exited with code {process.returncode}"
                if error_output and not concise_error:
                    # Use first meaningful line as error message
                    for line in lines:
                        if line.strip() and len(line.strip()) < 200:
                            error_message = line.strip()
                            break
                
                # Create a custom exception for script errors
                from app.exceptions import FileSystemError
                script_error = FileSystemError(
                    error_message,
                    context={
                        "script_path": script_info["path"],
                        "exit_code": process.returncode,
                        "script_args": script_args,
                        "output_lines": len(output_lines)
                    },
                    file_path=script_info["path"],
                    operation="execute"
                )
                
                ActivityLogger.log_activity(
                    user_id=user_id,
                    user_email=user_email,
                    action="run_script",
                    resource_type="script",
                    resource_id=script_name,
                    status="error",
                    error_message=error_message,
                    error_traceback=error_output if error_output else None,
                    details={
                        "script_path": script_info["path"],
                        "exit_code": process.returncode,
                        "script_args": script_args,
                        "output_lines": len(output_lines),
                        "full_output": error_output
                    },
                    ip_address=ip_address,
                    exception=script_error
                )
            else:
                # Log successful completion
                ActivityLogger.log_activity(
                    user_id=user_id,
                    user_email=user_email,
                    action="run_script",
                    resource_type="script",
                    resource_id=script_name,
                    status="success",
                    details={
                        "script_path": script_info["path"],
                        "exit_code": 0,
                        "output_lines": len(output_lines)
                    },
                    ip_address=ip_address
                )
            
        except Exception as e:
            script_status[script_name]["running"] = False
            script_status[script_name]["error"] = str(e)
            
            # Log exception error with full traceback
            error_trace = traceback.format_exc()
            ActivityLogger.log_activity(
                user_id=user_id,
                user_email=user_email,
                action="run_script",
                resource_type="script",
                resource_id=script_name,
                status="error",
                error_message=str(e),
                error_traceback=error_trace,
                details={
                    "script_path": script_info["path"],
                    "script_args": script_args
                },
                ip_address=ip_address,
                exception=e  # Pass exception for proper categorization
            )
    
    thread = threading.Thread(target=run_script_thread, daemon=True)
    thread.start()
    
    ActivityLogger.log_activity(
        user_id=user.get("id"),
        user_email=user.get("email"),
        action="run_script",
        resource_type="script",
        resource_id=script_name,
        details={"script_path": script_info["path"]},
        ip_address=ip_address
    )
    
    return {"success": True, "message": f"Script {script_name} started"}

@router.get("/api/scripts/status/{script_name}")
async def get_script_status(request: Request, script_name: str):
    get_current_user(request)
    
    if script_name not in script_status:
        return {"running": False, "output": [], "error": None}
    
    status_info = script_status[script_name].copy()
    return status_info

@router.get("/api/scripts/list")
async def list_scripts(request: Request):
    get_current_user(request)
    
    scripts_config = {
        "fetch_emails": {
            "label": "Fetch Emails",
            "required_args": [
                {"name": "verified_csv", "label": "Verified CSV", "example": "All/usa_list/usa_list.csv"},
                {"name": "listings_csv", "label": "Listings CSV", "example": "All/All_list/listings.csv"},
                {"name": "output_csv", "label": "Output CSV", "example": "All/fetch_emails/usa_email.csv"}
            ]
        },
        "verify_emails": {
            "label": "Verify Emails",
            "required_args": []
        },
        "merge_csv": {
            "label": "Merge CSV Files",
            "required_args": []
        },
        "usa_separation": {
            "label": "USA Separation",
            "required_args": [
                {"name": "input_csv", "label": "Input CSV", "example": "All/All_list/listings.csv"},
                {"name": "output_csv", "label": "Output CSV", "example": "All/usa_list/usa_list.csv"}
            ]
        },
        "non_usa_separation": {
            "label": "Non-USA Separation",
            "required_args": []
        }
    }
    
    scripts = [
        {
            "name": name,
            "label": config["label"],
            "required_args": config["required_args"]
        }
        for name, config in scripts_config.items()
    ]
    
    return {"scripts": scripts}


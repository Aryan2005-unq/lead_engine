#!/usr/bin/env python3
"""
================================================================================
LINKEDIN PIPELINE SCRIPT - LinkedIn Email Scraping Workflow
================================================================================

This script orchestrates the LinkedIn scraping workflow.

LINKEDIN SECTION:
  1. scrape.py -> Creates apify_results_api*_*.csv files
  2. merge_csv.py -> Creates apify_results_merged_timestamp.csv
  3. scrape_emails.py -> Scrapes emails from merged results

USAGE:
    python run_linkedin_pipeline.py
================================================================================
"""

import os
import sys
import subprocess
import asyncio
import glob
import argparse
import shutil
import logging
import traceback
import csv
import socket
from datetime import datetime
from pathlib import Path
from typing import Optional, List, Dict, Any, Tuple

if hasattr(sys.stdout, 'reconfigure'):
    try: sys.stdout.reconfigure(encoding='utf-8', errors='ignore')
    except: pass
if hasattr(sys.stderr, 'reconfigure'):
    try: sys.stderr.reconfigure(encoding='utf-8', errors='ignore')
    except: pass

# Optional imports for network and process monitoring
try:
    import psutil
    PSUTIL_AVAILABLE = True
except ImportError:
    PSUTIL_AVAILABLE = False

try:
    import requests
    REQUESTS_AVAILABLE = True
except ImportError:
    REQUESTS_AVAILABLE = False

# Color codes for terminal output
class Colors:
    HEADER = '\033[95m'
    OKBLUE = '\033[94m'
    OKCYAN = '\033[96m'
    OKGREEN = '\033[92m'
    WARNING = '\033[93m'
    FAIL = '\033[91m'
    ENDC = '\033[0m'
    BOLD = '\033[1m'

# Error Logger Class
class ErrorLogger:
    """Comprehensive error logging system for all error categories"""
    
    def __init__(self, log_file: str = "pipeline_errors.log"):
        self.log_file = log_file
        self.logger = logging.getLogger("PipelineErrorLogger")
        self.logger.setLevel(logging.DEBUG)
        
        # Create logs directory if it doesn't exist
        log_dir = os.path.dirname(log_file) if os.path.dirname(log_file) else "."
        if log_dir and not os.path.exists(log_dir):
            try:
                os.makedirs(log_dir, exist_ok=True)
            except Exception:
                log_file = "pipeline_errors.log"  # Fallback to current directory
        
        # File handler for error log
        file_handler = logging.FileHandler(log_file, encoding='utf-8')
        file_handler.setLevel(logging.DEBUG)
        
        # Formatter
        formatter = logging.Formatter(
            '%(asctime)s | %(levelname)-8s | %(category)-20s | %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        file_handler.setFormatter(formatter)
        
        if not self.logger.handlers:
            self.logger.addHandler(file_handler)
    
    def _log_error(self, category: str, severity: str, message: str, exception: Optional[Exception] = None, 
                   traceback_str: Optional[str] = None, context: Optional[Dict[str, Any]] = None):
        """Internal method to log errors with full details"""
        log_message = message
        
        if context:
            context_str = " | ".join([f"{k}={v}" for k, v in context.items()])
            log_message += f" | Context: {context_str}"
        
        if exception:
            log_message += f" | Exception: {type(exception).__name__}: {str(exception)}"
        
        if traceback_str:
            log_message += f" | Traceback: {traceback_str}"
        
        extra = {'category': category}
        
        if severity.upper() == 'CRITICAL':
            self.logger.critical(log_message, extra=extra)
        elif severity.upper() == 'HIGH':
            self.logger.error(log_message, extra=extra)
        elif severity.upper() == 'MEDIUM':
            self.logger.warning(log_message, extra=extra)
        else:
            self.logger.info(log_message, extra=extra)
    
    # File System Errors
    def log_file_not_found(self, file_path: str, context: Optional[Dict[str, Any]] = None):
        """Log file not found error"""
        self._log_error(
            "File System",
            "HIGH",
            f"File not found: {file_path}",
            context=context
        )
    
    def log_directory_access_error(self, directory: str, operation: str, error: Exception, 
                                   context: Optional[Dict[str, Any]] = None):
        """Log directory access error"""
        self._log_error(
            "File System",
            "HIGH",
            f"Directory access error: {directory} | Operation: {operation}",
            exception=error,
            context=context
        )
    
    def log_csv_reading_error(self, file_path: str, error: Exception, context: Optional[Dict[str, Any]] = None):
        """Log CSV reading error"""
        self._log_error(
            "File System",
            "MEDIUM",
            f"CSV reading error: {file_path}",
            exception=error,
            traceback_str=traceback.format_exc(),
            context=context
        )
    
    def log_file_reading_error(self, file_path: str, error: Exception, context: Optional[Dict[str, Any]] = None):
        """Log file reading error"""
        self._log_error(
            "File System",
            "MEDIUM",
            f"File reading error: {file_path}",
            exception=error,
            traceback_str=traceback.format_exc(),
            context=context
        )
    
    # General/File System Errors
    def log_script_not_found(self, script_path: str, context: Optional[Dict[str, Any]] = None):
        """Log script not found error"""
        self._log_error(
            "General/File System",
            "CRITICAL",
            f"Script not found: {script_path}",
            context=context
        )
    
    def log_script_already_running(self, script_path: str, pid: int, context: Optional[Dict[str, Any]] = None):
        """Log script already running error"""
        self._log_error(
            "General/File System",
            "HIGH",
            f"Script already running: {script_path} | PID: {pid}",
            context=context
        )
    
    def log_script_execution_exception(self, script_path: str, error: Exception, 
                                      context: Optional[Dict[str, Any]] = None):
        """Log script execution exception with traceback"""
        self._log_error(
            "General/File System",
            "CRITICAL",
            f"Script execution exception: {script_path}",
            exception=error,
            traceback_str=traceback.format_exc(),
            context=context
        )
    
    def log_script_failure(self, script_path: str, exit_code: int, context: Optional[Dict[str, Any]] = None):
        """Log script failure (non-zero exit code)"""
        self._log_error(
            "General/File System",
            "HIGH",
            f"Script failure: {script_path} | Exit code: {exit_code}",
            context=context
        )
    
    def log_invalid_script_name(self, script_path: str, reason: str, context: Optional[Dict[str, Any]] = None):
        """Log invalid script name error"""
        self._log_error(
            "General/File System",
            "HIGH",
            f"Invalid script name: {script_path} | Reason: {reason}",
            context=context
        )
    
    # Network Errors
    def log_api_request_failure(self, url: str, status_code: Optional[int], error: Exception,
                                context: Optional[Dict[str, Any]] = None):
        """Log API request failure"""
        status_info = f" | Status: {status_code}" if status_code else ""
        self._log_error(
            "Network",
            "HIGH",
            f"API request failure: {url}{status_info}",
            exception=error,
            context=context
        )
    
    def log_connection_timeout(self, url: str, timeout: float, context: Optional[Dict[str, Any]] = None):
        """Log connection timeout"""
        self._log_error(
            "Network",
            "HIGH",
            f"Connection timeout: {url} | Timeout: {timeout}s",
            context=context
        )
    
    def log_dns_resolution_failure(self, hostname: str, error: Exception, 
                                   context: Optional[Dict[str, Any]] = None):
        """Log DNS resolution failure"""
        self._log_error(
            "Network",
            "CRITICAL",
            f"DNS resolution failure: {hostname}",
            exception=error,
            context=context
        )
    
    def log_external_service_error(self, service: str, error: Exception, 
                                  context: Optional[Dict[str, Any]] = None):
        """Log external service error"""
        self._log_error(
            "Network",
            "HIGH",
            f"External service error: {service}",
            exception=error,
            context=context
        )
    
    # Validation Errors
    def log_missing_required_fields(self, file_path: str, missing_fields: List[str],
                                   context: Optional[Dict[str, Any]] = None):
        """Log missing required fields"""
        self._log_error(
            "Validation",
            "MEDIUM",
            f"Missing required fields in {file_path}: {', '.join(missing_fields)}",
            context=context
        )
    
    def log_format_validation_failure(self, file_path: str, reason: str,
                                     context: Optional[Dict[str, Any]] = None):
        """Log format validation failure"""
        self._log_error(
            "Validation",
            "MEDIUM",
            f"Format validation failure: {file_path} | Reason: {reason}",
            context=context
        )
    
    def log_invalid_file_format(self, file_path: str, expected_format: str, actual_format: str,
                               context: Optional[Dict[str, Any]] = None):
        """Log invalid file format"""
        self._log_error(
            "Validation",
            "MEDIUM",
            f"Invalid file format: {file_path} | Expected: {expected_format} | Actual: {actual_format}",
            context=context
        )

# Global error logger instance
error_logger = ErrorLogger()

# Network Error Handling Helpers
def check_network_connectivity(hostname: str = "8.8.8.8", port: int = 53, timeout: float = 3.0) -> bool:
    """Check basic network connectivity"""
    try:
        socket.setdefaulttimeout(timeout)
        socket.socket(socket.AF_INET, socket.SOCK_STREAM).connect((hostname, port))
        return True
    except socket.timeout:
        error_logger.log_connection_timeout(f"{hostname}:{port}", timeout)
        return False
    except socket.gaierror as e:
        error_logger.log_dns_resolution_failure(hostname, e)
        return False
    except Exception as e:
        error_logger.log_external_service_error(f"Network connectivity check ({hostname})", e)
        return False

def safe_api_request(url: str, method: str = "GET", timeout: float = 30.0, **kwargs):
    """Make a safe API request with error handling"""
    if not REQUESTS_AVAILABLE:
        error_logger.log_external_service_error(
            f"API request ({url})",
            ImportError("requests library not available"),
            {'url': url, 'method': method}
        )
        return None
    
    context = {'url': url, 'method': method, 'timeout': timeout}
    
    try:
        response = requests.request(method, url, timeout=timeout, **kwargs)
        if response.status_code >= 400:
            error_logger.log_api_request_failure(url, response.status_code, 
                                                Exception(f"HTTP {response.status_code}"), context)
        return response
    except requests.exceptions.Timeout:
        error_logger.log_connection_timeout(url, timeout, context)
        return None
    except requests.exceptions.ConnectionError as e:
        error_logger.log_external_service_error(f"API connection ({url})", e, context)
        return None
    except requests.exceptions.RequestException as e:
        error_logger.log_api_request_failure(url, None, e, context)
        return None
    except Exception as e:
        error_logger.log_external_service_error(f"API request ({url})", e, context)
        return None

# CSV Validation Helper
def validate_csv_file(file_path: str, required_headers: Optional[List[str]] = None) -> Tuple[bool, Optional[str]]:
    """Validate CSV file format and required headers"""
    context = {'file_path': file_path, 'required_headers': required_headers}
    
    if not os.path.exists(file_path):
        error_logger.log_file_not_found(file_path, context)
        return False, "File not found"
    
    if not file_path.endswith('.csv'):
        error_logger.log_invalid_file_format(file_path, "CSV", os.path.splitext(file_path)[1], context)
        return False, "Not a CSV file"
    
    try:
        with open(file_path, 'r', encoding='utf-8', newline='') as f:
            # Try to read as CSV
            try:
                reader = csv.DictReader(f)
                headers = reader.fieldnames
                
                if headers is None:
                    error_logger.log_format_validation_failure(
                        file_path,
                        "CSV file has no headers",
                        context
                    )
                    return False, "No headers found"
                
                # Check required headers
                if required_headers:
                    missing_fields = [h for h in required_headers if h not in headers]
                    if missing_fields:
                        error_logger.log_missing_required_fields(file_path, missing_fields, context)
                        return False, f"Missing required fields: {', '.join(missing_fields)}"
                
                return True, None
            except csv.Error as e:
                error_logger.log_csv_reading_error(file_path, e, context)
                return False, f"CSV parsing error: {str(e)}"
    except PermissionError as e:
        error_logger.log_file_reading_error(file_path, e, context)
        return False, "Permission denied"
    except Exception as e:
        error_logger.log_file_reading_error(file_path, e, context)
        return False, f"Error reading file: {str(e)}"

def print_header(text):
    print(f"\n{Colors.HEADER}{Colors.BOLD}{'='*80}{Colors.ENDC}")
    print(f"{Colors.HEADER}{Colors.BOLD}{text.center(80)}{Colors.ENDC}")
    print(f"{Colors.HEADER}{Colors.BOLD}{'='*80}{Colors.ENDC}\n")

def print_step(step_num, total_steps, description):
    print(f"{Colors.OKCYAN}[{step_num}/{total_steps}] {description}{Colors.ENDC}")

def print_success(message):
    print(f"{Colors.OKGREEN}✓ {message}{Colors.ENDC}")

def print_error(message):
    print(f"{Colors.FAIL}✗ {message}{Colors.ENDC}")

def print_warning(message):
    print(f"{Colors.WARNING}⚠ {message}{Colors.ENDC}")

def is_script_running(script_path: str) -> Optional[int]:
    """Check if a script is already running and return its PID"""
    if not PSUTIL_AVAILABLE:
        # If psutil is not available, we can't check for running processes
        # Return None to indicate we can't determine (not an error)
        return None
    
    try:
        script_name = os.path.basename(script_path)
        for proc in psutil.process_iter(['pid', 'name', 'cmdline']):
            try:
                cmdline = proc.info.get('cmdline', [])
                if cmdline and len(cmdline) > 1:
                    # Check if this is a Python process running our script
                    if (sys.executable.lower() in cmdline[0].lower() and 
                        script_name in ' '.join(cmdline)):
                        # Don't match the current process
                        if proc.info['pid'] != os.getpid():
                            return proc.info['pid']
            except (psutil.NoSuchProcess, psutil.AccessDenied):
                continue
    except Exception:
        pass
    return None

def validate_script_name(script_path: str) -> Tuple[bool, Optional[str]]:
    """Validate script name and return (is_valid, error_reason)"""
    if not script_path:
        return False, "Script path is empty"
    
    if not script_path.endswith('.py'):
        return False, "Script must be a Python file (.py)"
    
    if not os.path.isabs(script_path):
        # Check for invalid characters in relative path
        if '..' in script_path and script_path.count('..') > 3:
            return False, "Too many parent directory references"
    
    return True, None

async def run_script(script_path, args=None, cwd=None):
    """Run a Python script asynchronously and return success status"""
    context = {
        'script_path': script_path,
        'args': args,
        'cwd': cwd,
        'timestamp': datetime.now().isoformat()
    }
    
    # Validate script name
    is_valid, reason = validate_script_name(script_path)
    if not is_valid:
        error_logger.log_invalid_script_name(script_path, reason or "Unknown", context)
        print_error(f"Invalid script name: {script_path} - {reason}")
        return False
    
    # Check if script exists (use full path for existence check)
    if not os.path.exists(script_path):
        error_logger.log_script_not_found(script_path, context)
        print_error(f"Script not found: {script_path}")
        return False
    
    # Check if script is already running
    running_pid = is_script_running(script_path)
    if running_pid:
        error_logger.log_script_already_running(script_path, running_pid, context)
        print_error(f"Script already running: {script_path} (PID: {running_pid})")
        return False
    
    # If cwd is set, use just the basename for execution
    # This prevents path duplication when cwd is already set to the script's directory
    if cwd:
        script_to_run = os.path.basename(script_path)
    else:
        script_to_run = script_path
    
    cmd = [sys.executable, script_to_run]
    if args:
        cmd.extend(args)
    
    try:
        # Run script asynchronously using asyncio.create_subprocess_exec
        print(f"{Colors.OKBLUE}  Executing: {' '.join(cmd)}{Colors.ENDC}")
        process = await asyncio.create_subprocess_exec(
            *cmd,
            cwd=cwd or os.path.dirname(script_path),
            stdout=sys.stdout,
            stderr=sys.stderr
        )
        
        # Wait for process to complete with timeout
        try:
            returncode = await asyncio.wait_for(process.wait(), timeout=3600)  # 1 hour timeout
        except asyncio.TimeoutError:
            process.kill()
            await process.wait()
            error_logger.log_script_execution_exception(
                script_path, 
                Exception("Script execution timeout (1 hour)"),
                context
            )
            print_error(f"Script timeout: {script_path}")
            return False
        
        if returncode == 0:
            print(f"{Colors.OKGREEN}  ✓ Script completed successfully{Colors.ENDC}")
            return True
        else:
            error_logger.log_script_failure(script_path, returncode, context)
            print_error(f"Script failed: {script_path} (exit code: {returncode})")
            return False
    except Exception as e:
        error_logger.log_script_execution_exception(script_path, e, context)
        print_error(f"Error running script: {e}")
        return False

def find_latest_file(pattern, directory="."):
    """Find the most recent file matching a pattern"""
    context = {'pattern': pattern, 'directory': directory}
    
    try:
        # Check directory access
        if not os.path.exists(directory):
            error_logger.log_directory_access_error(
                directory, 
                "read", 
                FileNotFoundError(f"Directory not found: {directory}"),
                context
            )
            return None
        
        if not os.access(directory, os.R_OK):
            error_logger.log_directory_access_error(
                directory,
                "read",
                PermissionError(f"No read access to directory: {directory}"),
                context
            )
            return None
        
        files = glob.glob(os.path.join(directory, pattern))
        if not files:
            return None
        
        return max(files, key=os.path.getmtime)
    except Exception as e:
        error_logger.log_directory_access_error(directory, "glob_search", e, context)
        return None

def get_relative_path(file_path, from_dir):
    """Get relative path from a directory to a file"""
    file_abs = os.path.abspath(file_path)
    from_abs = os.path.abspath(from_dir)
    try:
        rel_path = os.path.relpath(file_abs, from_abs)
        # Normalize to forward slashes for cross-platform compatibility
        return rel_path.replace("\\", "/")
    except ValueError:
        # If paths are on different drives (Windows), return absolute path
        return file_abs.replace("\\", "/")

def ensure_file_exists(file_path, headers=None, create_empty=True):
    """Ensure a file exists, creating it with headers if missing"""
    context = {'file_path': file_path, 'headers': headers, 'create_empty': create_empty}
    
    if os.path.exists(file_path):
        # Validate existing file format if it's a CSV
        if file_path.endswith('.csv') and headers:
            try:
                with open(file_path, 'r', encoding='utf-8', newline='') as f:
                    reader = csv.DictReader(f)
                    existing_headers = reader.fieldnames or []
                    missing_fields = [h for h in headers if h not in existing_headers]
                    if missing_fields:
                        error_logger.log_missing_required_fields(file_path, missing_fields, context)
            except Exception as e:
                error_logger.log_csv_reading_error(file_path, e, context)
        return True
    
    if not create_empty:
        error_logger.log_file_not_found(file_path, context)
        return False
    
    try:
        # Create directory if it doesn't exist
        dir_path = os.path.dirname(file_path)
        if dir_path:
            try:
                os.makedirs(dir_path, exist_ok=True)
            except Exception as e:
                error_logger.log_directory_access_error(
                    dir_path,
                    "create",
                    e,
                    context
                )
                return False
        
        # Create file with headers
        with open(file_path, 'w', newline='', encoding='utf-8') as f:
            if headers:
                try:
                    writer = csv.DictWriter(f, fieldnames=headers)
                    writer.writeheader()
                except Exception as e:
                    error_logger.log_format_validation_failure(
                        file_path,
                        f"Failed to write CSV headers: {str(e)}",
                        context
                    )
                    return False
            else:
                # If no headers provided, create empty file
                pass
        
        print_warning(f"Created default empty file: {file_path}")
        return True
    except PermissionError as e:
        error_logger.log_directory_access_error(
            os.path.dirname(file_path) or ".",
            "write",
            e,
            context
        )
        print_error(f"Failed to create default file {file_path}: Permission denied")
        return False
    except Exception as e:
        error_logger.log_file_reading_error(file_path, e, context)
        print_error(f"Failed to create default file {file_path}: {e}")
        return False

def create_default_listings_csv(file_path):
    """Create default listings.csv with proper headers"""
    headers = [
        "Business Name", "FCC Registration Number (FRN)", "Previous Business Names",
        "Business Address", "Other DBA Name(s)", "Foreign Voice Service Provider",
        "Implementation", "Voice Service Provider", "Gateway Provider",
        "Non-Gateway Intermediate Provider", "Robocall Mitigation Contact Name",
        "Contact Title", "Contact Department", "Contact Business Address",
        "Contact Telephone Number", "Attachment Link", "sys_id"
    ]
    return ensure_file_exists(file_path, headers)

def create_default_usa_list_csv(file_path):
    """Create default usa_list.csv with proper headers"""
    headers = [
        "frn", "sys_id", "company_name", "service_type", "contact_email",
        "contact_phone", "website", "fcc_499_status", "filer_id",
        "legal_name_499", "dba_499", "verification_link", "error"
    ]
    return ensure_file_exists(file_path, headers)

def create_default_email_csv(file_path):
    """Create default email CSV with company_name and email columns"""
    headers = ["company_name", "email"]
    return ensure_file_exists(file_path, headers)

def update_scrape_emails_input(input_file):
    """Update the INPUT_CSV in scrape_emails.py"""
    script_path = "linkdin/scrape_emails.py"
    context = {'script_path': script_path, 'input_file': input_file}
    
    if not os.path.exists(script_path):
        error_logger.log_file_not_found(script_path, context)
        print_warning(f"Could not update {script_path} - file not found")
        return False
    
    try:
        # Read file
        try:
            with open(script_path, 'r', encoding='utf-8') as f:
                content = f.read()
        except PermissionError as e:
            error_logger.log_file_reading_error(script_path, e, context)
            print_error(f"Permission denied reading {script_path}")
            return False
        except Exception as e:
            error_logger.log_file_reading_error(script_path, e, context)
            print_error(f"Failed to read {script_path}: {e}")
            return False
        
        # Find and replace the INPUT_CSV line
        import re
        pattern = r"INPUT_CSV\s*=\s*['\"].*?['\"]"
        replacement = f'INPUT_CSV = \'{os.path.basename(input_file)}\''
        new_content = re.sub(pattern, replacement, content)
        
        # Validate that replacement was made
        if new_content == content:
            error_logger.log_format_validation_failure(
                script_path,
                "INPUT_CSV pattern not found in file",
                context
            )
            print_warning("INPUT_CSV pattern not found, file may not have been updated")
        
        try:
            with open(script_path, 'w', encoding='utf-8') as f:
                f.write(new_content)
        except PermissionError as e:
            error_logger.log_directory_access_error(
                os.path.dirname(script_path),
                "write",
                e,
                context
            )
            print_error(f"Permission denied writing to {script_path}")
            return False
        except Exception as e:
            error_logger.log_file_reading_error(script_path, e, context)
            print_error(f"Failed to write {script_path}: {e}")
            return False
        
        print_success(f"Updated scrape_emails.py to use: {os.path.basename(input_file)}")
        return True
    except Exception as e:
        error_logger.log_file_reading_error(script_path, e, context)
        print_error(f"Failed to update scrape_emails.py: {e}")
        return False

async def run_linkedin_section():
    """Run LinkedIn scraping section - one step at a time"""
    print_header("LINKEDIN SECTION")
    
    # Step 1: scrape.py
    print_step(1, 3, "Running scrape.py (LinkedIn data scraping)")
    if not await run_script("linkdin/scrape.py", cwd="linkdin"):
        print_warning("LinkedIn scraping failed, checking for existing files...")
    else:
        print_success("LinkedIn scraping completed")
    print(f"{Colors.OKBLUE}  Waiting for step 1 to fully complete...{Colors.ENDC}\n")
    
    # Step 2: merge_csv.py
    print_step(2, 3, "Running merge_csv.py (Merging API results)")
    if not await run_script("linkdin/merge_csv.py", cwd="linkdin"):
        print_warning("CSV merging failed, will check for existing files...")
    
    # Find the latest merged file
    merged_file = find_latest_file("apify_results_merged_*.csv", "linkdin")
    if not merged_file:
        print_warning("Could not find merged CSV file, checking for any LinkedIn CSV files...")
        # Look for any apify results files
        any_csv = find_latest_file("apify_results_*.csv", "linkdin")
        if any_csv:
            print_warning(f"Using existing file: {os.path.basename(any_csv)}")
            merged_file = any_csv
        else:
            print_warning("No LinkedIn CSV files found, skipping email scraping step")
            return True  # Continue pipeline even if LinkedIn section is incomplete
    
    print_success(f"Merged file ready: {os.path.basename(merged_file)}")
    print(f"{Colors.OKBLUE}  Waiting for step 2 to fully complete...{Colors.ENDC}\n")
    
    # Step 3: scrape_emails.py
    print_step(3, 3, "Running scrape_emails.py (Email scraping)")
    # Update the input file in scrape_emails.py
    if not update_scrape_emails_input(merged_file):
        print_warning("Continuing with scrape_emails.py anyway...")
    
    if not await run_script("linkdin/scrape_emails.py", cwd="linkdin"):
        print_warning("Email scraping failed, but continuing pipeline...")
    else:
        print_success("Email scraping completed")
    print(f"{Colors.OKBLUE}  Waiting for step 3 to fully complete...{Colors.ENDC}\n")
    
    return True

def get_base_directory():
    """Detect and return the base directory containing linkdin, All, Merging"""
    # Get the directory where this script is located
    script_dir = os.path.dirname(os.path.abspath(__file__))
    
    # 1. Check parent of script_dir (e.g., if script is in Pipeline/)
    parent_dir = os.path.dirname(script_dir)
    if os.path.exists(os.path.join(parent_dir, "linkdin")) and \
       os.path.exists(os.path.join(parent_dir, "All")) and \
       os.path.exists(os.path.join(parent_dir, "Merging")):
        return parent_dir
    
    # 2. Check script_dir itself
    if os.path.exists(os.path.join(script_dir, "linkdin")) and \
       os.path.exists(os.path.join(script_dir, "All")) and \
       os.path.exists(os.path.join(script_dir, "Merging")):
        return script_dir
    
    # 3. Check current working directory
    cwd = os.getcwd()
    if os.path.exists(os.path.join(cwd, "linkdin")) and \
       os.path.exists(os.path.join(cwd, "All")) and \
       os.path.exists(os.path.join(cwd, "Merging")):
        return cwd
    
    # 4. Walk up from script directory to find any folder containing linkdin and All
    current = script_dir
    while current and current != os.path.dirname(current):
        if os.path.exists(os.path.join(current, "linkdin")) and \
           os.path.exists(os.path.join(current, "All")):
            return current
        current = os.path.dirname(current)
    
    # Last resort: return parent directory of Pipeline
    return os.path.dirname(script_dir)


async def main():
    parser = argparse.ArgumentParser(
        description="LinkedIn Pipeline Script for Email Scraping Workflow",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python run_linkedin_pipeline.py
        """
    )
    
    args = parser.parse_args()
    
    base_dir = get_base_directory()
    original_cwd = os.getcwd()
    
    required_dirs = ["linkdin"]
    missing_dirs = []
    
    for d in required_dirs:
        dir_path = os.path.join(base_dir, d)
        if not os.path.exists(dir_path):
            missing_dirs.append(d)
        elif not os.access(dir_path, os.R_OK):
            missing_dirs.append(d)
    
    if missing_dirs:
        print_error(f"Could not find LinkedIn directory in: {base_dir}")
        return 1
    
    os.chdir(base_dir)
    print(f"{Colors.OKBLUE}Working directory: {os.getcwd()}{Colors.ENDC}\n")
    
    try:
        print_header("LINKEDIN PIPELINE - Email Scraping Workflow")
        print(f"Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        
        success = await run_linkedin_section()
        
        print_header("PIPELINE COMPLETED")
        if success:
            print_success("LinkedIn section completed successfully!")
        else:
            print_error("Pipeline completed with errors. Check logs above.")
        
        log_file_path = os.path.abspath(error_logger.log_file)
        if os.path.exists(log_file_path):
            log_size = os.path.getsize(log_file_path)
            if log_size > 0:
                print_warning(f"Error log file: {log_file_path} ({log_size} bytes)")
            else:
                print_success(f"No errors logged. Log file: {log_file_path}")
        
        return 0 if success else 1
    finally:
        os.chdir(original_cwd)

if __name__ == "__main__":
    sys.exit(asyncio.run(main()))

"""
Log format parsers for extracting structured data from log messages.
Supports JSON, logfmt, and other formats.
"""
import json
import re
import logging
from typing import Dict, Any, Optional, Tuple

logger = logging.getLogger(__name__)


def detect_format(message: str) -> str:
    """
    Detect the format of a log message.
    
    Args:
        message: Log message string
        
    Returns:
        Format name: 'json', 'logfmt', or 'plain'
    """
    if not message or not isinstance(message, str):
        return 'plain'
    
    message = message.strip()
    
    # Try JSON first
    if message.startswith('{') and message.endswith('}'):
        try:
            json.loads(message)
            return 'json'
        except (json.JSONDecodeError, ValueError):
            pass
    
    # Try logfmt (key=value pattern)
    # Logfmt typically has patterns like: key=value key2="value with spaces"
    # Check if message contains at least one key=value pattern
    logfmt_pattern = r'[a-zA-Z0-9_\-]+=(?:"[^"]*"|\'[^\']*\'|[^\s=]+)'
    if re.search(logfmt_pattern, message):
        # Make sure it's not JSON (JSON would have been caught above)
        return 'logfmt'
    
    return 'plain'


def parse_json(message: str) -> Optional[Dict[str, Any]]:
    """
    Parse a JSON log message.
    
    Args:
        message: JSON string
        
    Returns:
        Parsed dictionary or None if parsing fails
    """
    try:
        return json.loads(message)
    except (json.JSONDecodeError, ValueError, TypeError):
        return None


def parse_logfmt(message: str) -> Optional[Dict[str, Any]]:
    """
    Parse a logfmt log message (key=value pairs).
    
    Args:
        message: Logfmt string (e.g., 'level=error traceId=abc123 message="error occurred"')
        
    Returns:
        Parsed dictionary or None if parsing fails
    """
    if not message or not isinstance(message, str):
        return None
    
    result = {}
    # Pattern to match key=value pairs, handling quoted values and escaped quotes
    # Matches: key=value, key="value with spaces", key='value'
    pattern = r'([a-zA-Z0-9_\-]+)=(?:"([^"]*)"|\'([^\']*)\'|([^\s=]+))'
    
    for match in re.finditer(pattern, message):
        key = match.group(1)
        # Group 2 is double-quoted value, group 3 is single-quoted value, group 4 is unquoted value
        if match.group(2) is not None:
            value = match.group(2)  # Double-quoted
        elif match.group(3) is not None:
            value = match.group(3)  # Single-quoted
        else:
            value = match.group(4)  # Unquoted
        
        if value is not None:
            result[key] = value
    
    return result if result else None


def extract_trace_id_span_id(data: Dict[str, Any], case_sensitive: bool = False) -> Tuple[Optional[str], Optional[str]]:
    """
    Extract traceId and spanId from a dictionary, handling multiple field name variations.
    
    Args:
        data: Dictionary to search
        case_sensitive: If False, perform case-insensitive matching
        
    Returns:
        Tuple of (traceId, spanId) or (None, None) if not found
    """
    trace_id_variations = ['traceId', 'traceID', 'trace_id', 'TraceId', 'TRACE_ID', 'traceid']
    span_id_variations = ['spanId', 'spanID', 'span_id', 'SpanId', 'SPAN_ID', 'spanid']
    
    trace_id = None
    span_id = None
    
    if case_sensitive:
        # Direct lookup
        for var in trace_id_variations:
            if var in data:
                trace_id = data[var]
                break
        
        for var in span_id_variations:
            if var in data:
                span_id = data[var]
                break
    else:
        # Case-insensitive lookup
        data_lower = {k.lower(): v for k, v in data.items()}
        
        for var in trace_id_variations:
            if var.lower() in data_lower:
                # Get original key to preserve case
                try:
                    original_key = next(k for k in data.keys() if k.lower() == var.lower())
                    trace_id = data[original_key]
                    break
                except StopIteration:
                    continue
        
        for var in span_id_variations:
            if var.lower() in data_lower:
                try:
                    original_key = next(k for k in data.keys() if k.lower() == var.lower())
                    span_id = data[original_key]
                    break
                except StopIteration:
                    continue
    
    # Convert to string if not None
    if trace_id is not None:
        trace_id = str(trace_id)
    if span_id is not None:
        span_id = str(span_id)
    
    return trace_id, span_id

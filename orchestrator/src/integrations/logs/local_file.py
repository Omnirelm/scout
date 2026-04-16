"""
Local file log extractor for reading logs from local files.
"""
import re
from datetime import datetime, timedelta, timezone
from typing import Dict, Any, List, Optional, Union, Tuple
from pathlib import Path
from .base import LogExtractor, LogExtractorError, LogEntry
from .parsers import detect_format, parse_json, parse_logfmt, extract_trace_id_span_id


class LocalFileExtractor(LogExtractor):
    """Local file log extractor implementation"""
    
    def __init__(self, log_directory: str, tenant_id: Optional[str] = None, **kwargs):
        """
        Initialize Local File extractor.
        
        Args:
            log_directory: Base directory containing log files
            tenant_id: Optional tenant ID for multi-tenancy
            **kwargs: Additional configuration
        """
        # Set class attributes
        self.tenant_id = tenant_id
        self.log_directory = Path(log_directory)
        self.file_patterns = kwargs.get('file_patterns', ['*.log', '*.txt'])
        self.date_formats = kwargs.get('date_formats', [
            '%Y-%m-%d %H:%M:%S',
            '%Y-%m-%d %H:%M:%S.%f',
            '%Y/%m/%d %H:%M:%S',
            '%d/%b/%Y:%H:%M:%S',
            '%b %d %H:%M:%S'
        ])
        
        # Initialize parent with dummy base_url since we're not making HTTP requests
        super().__init__("file://localhost", {})
        
        if not self.log_directory.exists():
            raise LogExtractorError(f"Log directory does not exist: {log_directory}")
    
    def get_log_aggregator_name(self) -> str:
        """Get the log aggregator name."""
        return "LOCAL_FILE"
    
    def get_query_language(self) -> str:
        """Get the query language used by this extractor."""
        return "REGEX"
    
    def get_index_name(self) -> str:
        """Get the index name (not applicable for local files)."""
        return ""
    
    def get_label_names(self) -> List[str]:
        """Get available label names (file metadata fields)."""
        return ['filename', 'filepath', 'line_number', 'tenant_id']
    
    def get_field_names(self) -> List[str]:
        """Get field names (same as labels for local files)."""
        return self.get_label_names()
    
    def fetch_logs(self, query: str, start: Optional[datetime] = None, 
                  end: Optional[datetime] = None, limit: int = 100,
                  direction: str = "backward", **kwargs) -> List[LogEntry]:
        """
        Fetch logs from local files using pattern matching.
        
        Args:
            query: Pattern to search for (regex or simple string match)
            start: Start time for query 
            end: End time for query 
            limit: Maximum number of entries to return
            direction: Sort order ('forward' or 'backward')
            **kwargs: Additional query parameters like file_pattern, log_level
            
        Returns:
            List of log entries with timestamp and line
            
        Raises:
            LogExtractorError: If reading logs fails
        """
        try:
            logs = []
            file_pattern = kwargs.get('file_pattern', None)
            log_level = kwargs.get('log_level', None)
            
            # Normalize start/end to timezone-aware UTC datetimes if provided
            if start and start.tzinfo is None:
                start = start.replace(tzinfo=timezone.utc)
            if end and end.tzinfo is None:
                end = end.replace(tzinfo=timezone.utc)

            # Get all log files
            log_files = self._get_log_files(file_pattern)
            
            for log_file in log_files:
                file_logs = self._read_log_file(log_file, query, start, end, log_level)
                logs.extend(file_logs)
            
            # Sort logs by timestamp. ISO format strings sort correctly lexicographically.
            # Use a sentinel for logs without timestamps (sort them last)
            logs.sort(key=lambda x: (x.timestamp or '0000-01-01T00:00:00+00:00'),
                     reverse=(direction == "backward"))
            
            # Apply limit
            if limit > 0:
                logs = logs[:limit]
            
            return logs
            
        except Exception as e:
            raise LogExtractorError(f"Failed to fetch logs: {str(e)}")
    
    def _get_log_files(self, file_pattern: Optional[str] = None) -> List[Path]:
        """Get all log files matching the pattern."""
        log_files = []
        patterns = [file_pattern] if file_pattern else self.file_patterns
        
        # If tenant_id is provided, only search in tenant directory
        if self.tenant_id:
            tenant_dir = self.log_directory / self.tenant_id
            if tenant_dir.exists():
                for pattern in patterns:
                    log_files.extend(tenant_dir.glob(pattern))
                    log_files.extend(tenant_dir.glob(f"**/{pattern}"))  # Recursive
        else:
            # No tenant_id, search in main directory
            for pattern in patterns:
                log_files.extend(self.log_directory.glob(pattern))
        
        # Remove duplicates by converting to set and back to list
        log_files = list(set(log_files))
        
        return sorted(log_files, key=lambda f: f.stat().st_mtime, reverse=True)
    
    def _read_log_file(self, log_file: Path, query: str, 
                      start: Optional[datetime] = None, 
                      end: Optional[datetime] = None,
                      log_level: Optional[str] = None) -> List[LogEntry]:
        """Read and filter logs from a single file."""
        logs = []
        
        try:
            with open(log_file, 'r', encoding='utf-8', errors='ignore') as f:
                for line_num, line in enumerate(f, 1):
                    line = line.strip()
                    if not line:
                        continue
                    
                    # Try to parse timestamp from line
                    timestamp_dt, timestamp_str = self._extract_timestamp(line)
                    
                    # Filter by time range
                    if start and timestamp_dt and timestamp_dt < start:
                        continue
                    if end and timestamp_dt and timestamp_dt > end:
                        continue
                    
                    # Extract implicit log level from the line (if present)
                    parsed_level = self._extract_log_level(line)

                    # Filter by log level if specified. Prefer parsed level when available.
                    if log_level:
                        req = log_level.upper()
                        if parsed_level:
                            # Normalize WARNING -> WARN for comparison
                            pl = 'WARN' if parsed_level == 'WARNING' else parsed_level
                            if pl != req:
                                continue
                        else:
                            if not self._matches_log_level(line, req):
                                continue
                    
                    # Filter by query pattern
                    if not self._matches_query(line, query):
                        continue
                    
                    # Parse log entry into standardized format
                    log_entry = self._parse_log_entry(
                        line=line,
                        timestamp_dt=timestamp_dt,
                        timestamp_str=timestamp_str,
                        parsed_level=parsed_level,
                        log_level=log_level,
                        log_file=log_file,
                        line_num=line_num
                    )
                    
                    logs.append(log_entry)
                    
        except Exception as e:
            raise LogExtractorError(f"Failed to read log file {log_file}: {str(e)}")
        
        return logs
    
    def _extract_timestamp(self, line: str) -> Tuple[Optional[datetime], Optional[str]]:
        """Extract timestamp from log line."""
        # Try to find timestamp patterns in the line
        for date_format in self.date_formats:
            # Create regex pattern from strftime format
            regex_pattern = self._strftime_to_regex(date_format)
            match = re.search(regex_pattern, line)
            
            if match:
                timestamp_str = match.group()
                try:
                    timestamp_dt = datetime.strptime(timestamp_str, date_format)
                    # Make timezone-aware (assume UTC) to avoid naive/aware comparison issues
                    if timestamp_dt.tzinfo is None:
                        timestamp_dt = timestamp_dt.replace(tzinfo=timezone.utc)
                    return timestamp_dt, timestamp_str
                except ValueError:
                    continue
        
        # Try ISO format
        iso_pattern = r'\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+-]\d{2}:\d{2})?'
        match = re.search(iso_pattern, line)
        if match:
            timestamp_str = match.group()
            # Handle various ISO formats
            for fmt in ['%Y-%m-%dT%H:%M:%S.%fZ', '%Y-%m-%dT%H:%M:%SZ', '%Y-%m-%dT%H:%M:%S.%f', '%Y-%m-%dT%H:%M:%S']:
                try:
                    timestamp_dt = datetime.strptime(timestamp_str.rstrip('Z'), fmt.rstrip('Z'))
                    if timestamp_dt.tzinfo is None:
                        timestamp_dt = timestamp_dt.replace(tzinfo=timezone.utc)
                    return timestamp_dt, timestamp_str
                except ValueError:
                    continue
        
        return None, None
    
    def _strftime_to_regex(self, strftime_format: str) -> str:
        """Convert strftime format to regex pattern."""
        format_to_regex = {
            '%Y': r'\d{4}',
            '%y': r'\d{2}',
            '%m': r'\d{2}',
            '%B': r'[A-Za-z]+',
            '%b': r'[A-Za-z]{3}',
            '%d': r'\d{2}',
            '%H': r'\d{2}',
            '%M': r'\d{2}',
            '%S': r'\d{2}',
            '%f': r'\d+',
            '%%': r'%'
        }
        
        pattern = strftime_format
        for fmt, regex in format_to_regex.items():
            pattern = pattern.replace(fmt, regex)
        
        return pattern

    def _extract_log_level(self, line: str) -> Optional[str]:
        """Extract a log level from a line if present.

        Returns canonical uppercase level (INFO, ERROR, WARN, DEBUG, TRACE, CRITICAL) or None.
        """
        # 1) JSON-like: "level":"INFO" or 'level':'INFO'
        m = re.search(r'["\']level["\']\s*[:=]\s*["\']?([A-Za-z]+)["\']?', line, re.IGNORECASE)
        if m:
            val = m.group(1).upper()
            return 'WARN' if val == 'WARNING' else val

        # 2) key=value: level=INFO or severity=error
        m = re.search(r'\b(level|severity)\s*=\s*([A-Za-z]+)\b', line, re.IGNORECASE)
        if m:
            val = m.group(2).upper()
            return 'WARN' if val == 'WARNING' else val

        # 3) [LEVEL]
        m = re.search(r'\[(DEBUG|INFO|WARN|WARNING|ERROR|TRACE|CRITICAL|FATAL)\]', line, re.IGNORECASE)
        if m:
            val = m.group(1).upper()
            return 'WARN' if val == 'WARNING' else ('ERROR' if val == 'FATAL' else val)

        # 4) LEVEL: or LEVEL <space>
        m = re.search(r'\b(DEBUG|INFO|WARN|WARNING|ERROR|TRACE|CRITICAL|FATAL)[:\s]', line, re.IGNORECASE)
        if m:
            val = m.group(1).upper()
            return 'WARN' if val == 'WARNING' else ('ERROR' if val == 'FATAL' else val)

        return None
    
    def _matches_query(self, line: str, query: str) -> bool:
        """Check if line matches the query pattern."""
        if not query:
            return True
        
        try:
            # Support Loki-style label queries like: {service_name="OrderHandling"}
            # If query looks like a single label selector, extract the value and
            # match it as a substring against the line. This makes LocalFileExtractor
            # compatible with queries generated elsewhere in the analyzer.
            label_query_match = re.fullmatch(r'\s*\{\s*([a-zA-Z0-9_\-]+)\s*=\s*"([^"]+)"\s*\}\s*', query)
            if label_query_match:
                label_name, label_value = label_query_match.groups()
                # Match label value as substring (case-insensitive)
                return label_value.lower() in line.lower()

            # Try regex match first
            return bool(re.search(query, line, re.IGNORECASE))
        except re.error:
            # Fall back to simple string contains
            return query.lower() in line.lower()
    
    def _matches_log_level(self, line: str, log_level: str) -> bool:
        """Check if line matches the specified log level."""
        log_level = log_level.upper()
        line_upper = line.upper()
        
        # Common log level patterns
        level_patterns = [
            rf'\b{log_level}\b',
            rf'\[{log_level}\]',
            rf'{log_level}:',
            rf'"{log_level}"',
        ]
        
        for pattern in level_patterns:
            if re.search(pattern, line_upper):
                return True
        
        return False
    
    def fetch_instant_logs(self, query: str, time: Optional[datetime] = None, 
                          limit: int = 100, direction: str = "backward") -> List[LogEntry]:
        """
        Fetch logs around a specific point in time.
        
        Args:
            query: Pattern to search for
            time: Query time (defaults to now)
            limit: Maximum number of entries to return
            direction: Sort order ('forward' or 'backward')
            
        Returns:
            List of log entries
        """
        if time is None:
            time = datetime.now()
        
        # Create a small time window around the target time
        window = timedelta(minutes=5)  # 5 minute window
        
        return self.fetch_logs(
            query=query,
            start=time - window,
            end=time + window,
            limit=limit,
            direction=direction
        )
    
    def get_available_files(self) -> List[Dict[str, Any]]:
        """Get list of available log files."""
        files = []
        log_files = self._get_log_files()
        
        for log_file in log_files:
            stat = log_file.stat()
            files.append({
                'name': log_file.name,
                'path': str(log_file),
                'size': stat.st_size,
                'modified': datetime.fromtimestamp(stat.st_mtime).isoformat(),
                'created': datetime.fromtimestamp(stat.st_ctime).isoformat()
            })
        
        return files
    
    def tail_logs(self, query: str = "", limit: int = 100, **kwargs) -> List[LogEntry]:
        """Get the most recent log entries (like tail command)."""
        return self.fetch_logs(
            query=query,
            limit=limit,
            direction="backward",
            **kwargs
        )
    
    def _parse_log_entry(self, line: str, timestamp_dt: Optional[datetime], 
                        timestamp_str: Optional[str], parsed_level: Optional[str],
                        log_level: Optional[str], log_file: Path, line_num: int) -> LogEntry:
        """
        Parse a raw log entry from local file into a standardized format.
        
        Args:
            line: Raw log line content
            timestamp_dt: Parsed datetime object (internal use)
            timestamp_str: Timestamp string representation
            parsed_level: Extracted log level from line parsing
            log_level: Requested log level filter
            log_file: Path object of the log file
            line_num: Line number in the file
            
        Returns:
            Standardized log entry dict with timestamp, message, level, labels, traceId, spanId
        """
        # Build labels dict
        labels = {
            'filename': log_file.name,
            'filepath': str(log_file),
            'line_number': line_num
        }
        
        if self.tenant_id:
            labels['tenant_id'] = self.tenant_id
        
        # Extract traceId/spanId
        trace_id = None
        span_id = None
        
        # Priority 1: Parse message field if it's structured (JSON/logfmt)
        if line and isinstance(line, str):
            format_type = detect_format(line)
            
            if format_type == 'json':
                parsed = parse_json(line)
                if parsed:
                    trace_id, span_id = extract_trace_id_span_id(parsed)
            
            elif format_type == 'logfmt':
                parsed = parse_logfmt(line)
                if parsed:
                    trace_id, span_id = extract_trace_id_span_id(parsed)
        
        # Priority 2: Check labels dict
        if trace_id is None or span_id is None:
            if trace_id is None:
                trace_id, _ = extract_trace_id_span_id(labels)
            if span_id is None:
                _, span_id = extract_trace_id_span_id(labels)
        
        # Standardize timestamp to ISO format string (UTC)
        timestamp_iso = timestamp_dt.isoformat() if timestamp_dt else (timestamp_str or None)
        
        # Create standardized log entry format (consistent across all extractors)
        log_entry = LogEntry(
            timestamp=timestamp_iso if timestamp_iso else 'unknown',
            message=line,
            level=parsed_level or (log_level.upper() if log_level else None),
            labels=labels,  # Consistent field name
            traceId=trace_id,
            spanId=span_id,
        )
        
        return log_entry

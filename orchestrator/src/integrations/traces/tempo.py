"""
Grafana Tempo integration for trace extraction and analysis.
"""
from datetime import datetime
import json
import logging
import base64
from typing import Dict, Any, List, Optional
from .base import TraceExtractor, TraceExtractorError

logger = logging.getLogger(__name__)


class GrafanaTempoExtractor(TraceExtractor):
    """Grafana Tempo trace extractor implementation"""
    
    def __init__(self, base_url: str, tenant_id: Optional[str] = None, **kwargs):
        """
        Initialize Grafana Tempo extractor.
        
        Args:
            base_url: Tempo base URL (e.g., 'http://localhost:3200')
            tenant_id: Optional tenant ID for multi-tenancy
            **kwargs: Additional headers
        """
        self.tenant_id = tenant_id
        
        headers = {}
        
        if tenant_id:
            headers['X-Scope-OrgID'] = tenant_id
        
        # Merge with any additional headers
        headers.update(kwargs.get('headers', {}))
        
        super().__init__(base_url, headers)
    
    @classmethod
    def from_bearer_token(cls, base_url: str, token: str, 
                         tenant_id: Optional[str] = None, 
                         headers: Optional[Dict[str, str]] = None) -> 'GrafanaTempoExtractor':
        """
        Create Grafana Tempo extractor with Bearer token authentication.
        
        Args:
            base_url: Tempo base URL (e.g., 'http://localhost:3200')
            token: Bearer token for authentication
            tenant_id: Optional tenant ID for multi-tenancy
            headers: Optional additional headers to merge with auth header
            
        Returns:
            GrafanaTempoExtractor instance configured with Bearer token authentication
        """
        auth_headers = {
            'Authorization': f'Bearer {token}'
        }
        
        # Merge with any additional headers
        if headers:
            auth_headers.update(headers)
        
        # Use standard __init__ with pre-built headers
        return cls(base_url, tenant_id=tenant_id, headers=auth_headers)
    
    @classmethod
    def from_basic_auth(cls, base_url: str, username: str, password: str,
                       tenant_id: Optional[str] = None, 
                       headers: Optional[Dict[str, str]] = None) -> 'GrafanaTempoExtractor':
        """
        Create Grafana Tempo extractor with Basic authentication.
        
        Args:
            base_url: Tempo base URL (e.g., 'http://localhost:3200')
            username: Username for Basic authentication
            password: Password for Basic authentication
            tenant_id: Optional tenant ID for multi-tenancy
            headers: Optional additional headers to merge with auth header
            
        Returns:
            GrafanaTempoExtractor instance configured with Basic authentication
        """
        # Encode credentials for Basic auth
        credentials = base64.b64encode(f"{username}:{password}".encode()).decode()
        
        auth_headers = {
            'Authorization': f'Basic {credentials}'
        }
        
        # Merge with any additional headers
        if headers:
            auth_headers.update(headers)
        
        # Use standard __init__ with pre-built headers
        return cls(base_url, tenant_id=tenant_id, headers=auth_headers)
    
    @classmethod
    def from_api_key(cls, base_url: str, api_key: str, header_name: str = 'X-API-Key',
                    tenant_id: Optional[str] = None, 
                    headers: Optional[Dict[str, str]] = None) -> 'GrafanaTempoExtractor':
        """
        Create Grafana Tempo extractor with API key authentication.
        
        Args:
            base_url: Tempo base URL (e.g., 'http://localhost:3200')
            api_key: API key for authentication
            header_name: Header name to use for API key (default: 'X-API-Key')
            tenant_id: Optional tenant ID for multi-tenancy
            headers: Optional additional headers to merge with API key header
            
        Returns:
            GrafanaTempoExtractor instance configured with API key authentication
        """
        auth_headers = {
            header_name: api_key
        }
        
        # Merge with any additional headers
        if headers:
            auth_headers.update(headers)
        
        # Use standard __init__ with pre-built headers
        return cls(base_url, tenant_id=tenant_id, headers=auth_headers)
    
    def fetch_trace(self, trace_id: str, **kwargs) -> Dict[str, Any]:
        """
        Fetch a trace by its ID from Tempo.
        
        Args:
            trace_id: The trace ID to fetch (hex string)
            **kwargs: Additional query parameters
            
        Returns:
            Trace data in OpenTelemetry format
            
        Raises:
            TraceExtractorError: If fetching trace fails
        """
        url = f"{self.base_url}/api/traces/{trace_id}"
        
        # Log the request
        logger.debug(f"Fetching trace: {trace_id}, URL: {url}")
        
        # Make the request
        response = self._make_request('GET', url, params=kwargs)
        
        try:
            data = response.json()
            
            # Tempo returns traces in different formats depending on the endpoint
            # /api/traces/{traceID} returns OTLP JSON format
            if 'batches' in data:
                # OTLP format
                return self._parse_otlp_trace(data, trace_id)
            elif 'traceID' in data:
                # Direct trace format
                return data
            else:
                raise TraceExtractorError(f"Unexpected trace format: {list(data.keys())}")
            
        except json.JSONDecodeError as e:
            raise TraceExtractorError(f"Failed to parse trace response: {str(e)}")
        except KeyError as e:
            raise TraceExtractorError(f"Unexpected trace format: missing key {str(e)}")
    
    def _parse_otlp_trace(self, data: Dict[str, Any], trace_id: str) -> Dict[str, Any]:
        """
        Parse OTLP format trace data into a structured format.
        
        Args:
            data: Raw OTLP trace data
            trace_id: The trace ID
            
        Returns:
            Parsed trace data with spans
        """
        spans = []
        
        # Parse batches (OTLP format)
        for batch in data.get('batches', []):
            resource = batch.get('resource', {})
            resource_attrs = self._parse_attributes(resource.get('attributes', []))
            
            scope_spans = batch.get('scopeSpans', []) or batch.get('instrumentationLibrarySpans', [])
            
            for scope_span in scope_spans:
                scope = scope_span.get('scope', {})
                scope_name = scope.get('name', 'unknown')
                
                for span in scope_span.get('spans', []):
                    parsed_span = self._parse_span(span, resource_attrs, scope_name)
                    spans.append(parsed_span)
        
        # Sort spans by start time (ensure integer comparison)
        spans.sort(key=lambda s: self._safe_int(s.get('startTime', 0), 0))
        
        return {
            'traceID': trace_id,
            'spans': spans,
            'spanCount': len(spans),
            'duration': self._calculate_trace_duration(spans),
            'rootSpan': self._find_root_span(spans),
            'services': list(set(s.get('serviceName', 'unknown') for s in spans))
        }
    
    def _parse_span(self, span: Dict[str, Any], resource_attrs: Dict[str, Any], scope_name: str) -> Dict[str, Any]:
        """Parse a single span from OTLP format"""
        span_id = span.get('spanId', '')
        parent_span_id = span.get('parentSpanId', '')
        
        # Parse attributes
        attributes = self._parse_attributes(span.get('attributes', []))
        
        # Parse events
        events = []
        for event in span.get('events', []):
            events.append({
                'name': event.get('name', ''),
                'timestamp': self._safe_int(event.get('timeUnixNano', 0)),
                'attributes': self._parse_attributes(event.get('attributes', []))
            })
        
        # Parse status
        status = span.get('status', {})
        status_code = status.get('code', 0)  # 0=UNSET, 1=OK, 2=ERROR
        
        # Determine span kind
        span_kind = span.get('kind', 0)
        kind_map = {
            0: 'UNSPECIFIED',
            1: 'INTERNAL',
            2: 'SERVER',
            3: 'CLIENT',
            4: 'PRODUCER',
            5: 'CONSUMER'
        }
        
        # Convert timestamps to integers (OTLP returns them as strings)
        start_time = self._safe_int(span.get('startTimeUnixNano', 0), 0)
        end_time = self._safe_int(span.get('endTimeUnixNano', 0), 0)
        duration = end_time - start_time
        
        return {
            'spanId': span_id,
            'traceId': span.get('traceId', ''),
            'parentSpanId': parent_span_id,
            'name': span.get('name', ''),
            'kind': kind_map.get(span_kind, 'UNSPECIFIED'),
            'startTime': start_time,
            'endTime': end_time,
            'duration': duration,
            'attributes': attributes,
            'events': events,
            'status': {
                'code': status_code,
                'message': status.get('message', ''),
                'ok': status_code != 2
            },
            'serviceName': resource_attrs.get('service.name', 'unknown'),
            'resource': resource_attrs,
            'scope': scope_name
        }
    
    def _parse_attributes(self, attributes: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Parse OTLP attributes into a dict"""
        result = {}
        
        for attr in attributes:
            key = attr.get('key', '')
            value = attr.get('value', {})
            
            # Extract value based on type
            if 'stringValue' in value:
                result[key] = value['stringValue']
            elif 'intValue' in value:
                result[key] = int(value['intValue'])
            elif 'doubleValue' in value:
                result[key] = float(value['doubleValue'])
            elif 'boolValue' in value:
                result[key] = value['boolValue']
            elif 'arrayValue' in value:
                result[key] = [v for v in value['arrayValue'].get('values', [])]
            elif 'kvlistValue' in value:
                result[key] = self._parse_attributes(value['kvlistValue'].get('values', []))
        
        return result
    
    def _calculate_trace_duration(self, spans: List[Dict[str, Any]]) -> int:
        """Calculate total trace duration from spans"""
        if not spans:
            return 0
        
        # Ensure startTime and endTime are integers for proper comparison
        start_times = [self._safe_int(s.get('startTime', 0), float('inf')) for s in spans]
        end_times = [self._safe_int(s.get('endTime', 0), 0) for s in spans]
        
        min_start = min(start_times) if start_times else 0
        max_end = max(end_times) if end_times else 0
        
        return max_end - min_start
    
    def _find_root_span(self, spans: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
        """Find the root span (span with no parent)"""
        for span in spans:
            if not span.get('parentSpanId'):
                return span
        
        # If no span without parent, return first span
        return spans[0] if spans else None
    
    def search_traces(self, query: str, start: Optional[datetime] = None,
                     end: Optional[datetime] = None, limit: int = 20,
                     **kwargs) -> List[Dict[str, Any]]:
        """
        Search for traces using TraceQL.
        
        Args:
            query: TraceQL query string (e.g., '{ service.name = "frontend" }')
            start: Start time for search
            end: End time for search
            limit: Maximum number of traces to return
            **kwargs: Additional query parameters
            
        Returns:
            List of trace metadata
            
        Raises:
            TraceExtractorError: If searching traces fails
        """
        url = f"{self.base_url}/api/search"
        
        params = {
            'q': query,
            'limit': limit,
            **kwargs
        }
        
        # Handle time parameters
        if start:
            params['start'] = int(start.timestamp())
        
        if end:
            params['end'] = int(end.timestamp())
        
        logger.debug(f"Searching traces with query: {query}, Params: {params}")
        
        # Make the request
        response = self._make_request('GET', url, params=params)
        
        try:
            data = response.json()
            
            traces = data.get('traces', [])
            
            result = []
            for trace in traces:
                result.append({
                    'traceID': trace.get('traceID', ''),
                    'rootServiceName': trace.get('rootServiceName', ''),
                    'rootTraceName': trace.get('rootTraceName', ''),
                    'startTimeUnixNano': trace.get('startTimeUnixNano', 0),
                    'durationMs': trace.get('durationMs', 0),
                    'spanCount': trace.get('spanSet', {}).get('matched', 0)
                })
            
            return result
            
        except json.JSONDecodeError as e:
            raise TraceExtractorError(f"Failed to parse search response: {str(e)}")
    
    def search_tags(self, scope: str = 'resource', **kwargs) -> List[str]:
        """
        Get available tags for searching.
        
        Args:
            scope: Scope of tags ('resource', 'span', 'intrinsic')
            **kwargs: Additional parameters
            
        Returns:
            List of tag names
        """
        url = f"{self.base_url}/api/search/tags"
        
        params = {'scope': scope, **kwargs}
        
        response = self._make_request('GET', url, params=params)
        
        try:
            data = response.json()
            return data.get('tagNames', [])
        except json.JSONDecodeError as e:
            raise TraceExtractorError(f"Failed to parse tags response: {str(e)}")
    
    def get_tag_values(self, tag_name: str, **kwargs) -> List[str]:
        """
        Get available values for a specific tag.
        
        Args:
            tag_name: Name of the tag
            **kwargs: Additional parameters
            
        Returns:
            List of tag values
        """
        url = f"{self.base_url}/api/search/tag/{tag_name}/values"
        
        response = self._make_request('GET', url, params=kwargs)
        
        try:
            data = response.json()
            return data.get('tagValues', [])
        except json.JSONDecodeError as e:
            raise TraceExtractorError(f"Failed to parse tag values response: {str(e)}")
    
    def get_trace_metrics(self, trace_id: str) -> Dict[str, Any]:
        """
        Calculate metrics from a trace.
        
        Args:
            trace_id: The trace ID
            
        Returns:
            Dictionary of trace metrics
        """
        trace = self.fetch_trace(trace_id)
        spans = trace.get('spans', [])
        
        if not spans:
            return {}
        
        # Calculate metrics
        error_spans = [s for s in spans if not s.get('status', {}).get('ok', True)]
        
        # Group by service
        service_spans = {}
        for span in spans:
            service = span.get('serviceName', 'unknown')
            if service not in service_spans:
                service_spans[service] = []
            service_spans[service].append(span)
        
        # Calculate service durations
        service_durations = {}
        for service, svc_spans in service_spans.items():
            total_duration = sum(s.get('duration', 0) for s in svc_spans)
            service_durations[service] = total_duration
        
        return {
            'traceID': trace_id,
            'totalSpans': len(spans),
            'errorSpans': len(error_spans),
            'services': list(service_spans.keys()),
            'serviceSpanCounts': {svc: len(svc_spans) for svc, svc_spans in service_spans.items()},
            'serviceDurations': service_durations,
            'totalDuration': trace.get('duration', 0),
            'rootSpan': trace.get('rootSpan', {}),
            'hasErrors': len(error_spans) > 0
        }

    def _safe_int(self, value: Any, default: int = 0) -> int:
        """
        Safely convert a value to integer, handling string representations.
        
        Args:
            value: Value to convert (can be int, str, or other)
            default: Default value if conversion fails
            
        Returns:
            Integer value
        """
        if value is None:
            return default
        if isinstance(value, int):
            return value
        if isinstance(value, str):
            try:
                return int(value)
            except (ValueError, TypeError):
                return default
        try:
            return int(value)
        except (ValueError, TypeError):
            return default
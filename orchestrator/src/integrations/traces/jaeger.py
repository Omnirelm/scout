"""
Jaeger integration for trace extraction and analysis.
Supports both Jaeger v1 and v2 Query API.
"""
from datetime import datetime
import json
import logging
import base64
from typing import Dict, Any, List, Optional
from .base import TraceExtractor, TraceExtractorError

logger = logging.getLogger(__name__)


class JaegerExtractor(TraceExtractor):
    """Jaeger trace extractor implementation"""
    
    def __init__(self, base_url: str, 
                 base_path: str = "/jaeger/ui",
                 tenant_id: Optional[str] = None, **kwargs):
        """
        Initialize Jaeger extractor.
        
        Args:
            base_url: Jaeger base URL (e.g., 'http://localhost:8080')
            base_path: Base path for API (default: '/jaeger/ui')
            tenant_id: Optional tenant ID for multi-tenancy
            **kwargs: Additional headers
        """
        self.base_path = base_path.rstrip('/')
        self.api_base = f"{base_url.rstrip('/')}{self.base_path}/api"
        self.tenant_id = tenant_id
        
        headers = {}
        
        if tenant_id:
            headers['X-Scope-OrgID'] = tenant_id
        
        # Merge with any additional headers
        headers.update(kwargs.get('headers', {}))
        
        super().__init__(base_url, headers)
    
    @classmethod
    def from_bearer_token(cls, base_url: str, token: str, 
                         base_path: str = "/jaeger/ui",
                         tenant_id: Optional[str] = None, 
                         headers: Optional[Dict[str, str]] = None) -> 'JaegerExtractor':
        """
        Create Jaeger extractor with Bearer token authentication.
        
        Args:
            base_url: Jaeger base URL (e.g., 'http://localhost:8080')
            token: Bearer token for authentication
            base_path: Base path for API (default: '/jaeger/ui')
            tenant_id: Optional tenant ID for multi-tenancy
            headers: Optional additional headers to merge with auth header
            
        Returns:
            JaegerExtractor instance configured with Bearer token authentication
        """
        auth_headers = {
            'Authorization': f'Bearer {token}'
        }
        
        # Merge with any additional headers
        if headers:
            auth_headers.update(headers)
        
        # Use standard __init__ with pre-built headers
        return cls(base_url, base_path=base_path, tenant_id=tenant_id, headers=auth_headers)
    
    @classmethod
    def from_basic_auth(cls, base_url: str, username: str, password: str,
                       base_path: str = "/jaeger/ui",
                       tenant_id: Optional[str] = None, 
                       headers: Optional[Dict[str, str]] = None) -> 'JaegerExtractor':
        """
        Create Jaeger extractor with Basic authentication.
        
        Args:
            base_url: Jaeger base URL (e.g., 'http://localhost:8080')
            username: Username for Basic authentication
            password: Password for Basic authentication
            base_path: Base path for API (default: '/jaeger/ui')
            tenant_id: Optional tenant ID for multi-tenancy
            headers: Optional additional headers to merge with auth header
            
        Returns:
            JaegerExtractor instance configured with Basic authentication
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
        return cls(base_url, base_path=base_path, tenant_id=tenant_id, headers=auth_headers)
    
    @classmethod
    def from_api_key(cls, base_url: str, api_key: str, header_name: str = 'X-API-Key',
                    base_path: str = "/jaeger/ui",
                    tenant_id: Optional[str] = None, 
                    headers: Optional[Dict[str, str]] = None) -> 'JaegerExtractor':
        """
        Create Jaeger extractor with API key authentication.
        
        Args:
            base_url: Jaeger base URL (e.g., 'http://localhost:8080')
            api_key: API key for authentication
            header_name: Header name to use for API key (default: 'X-API-Key')
            base_path: Base path for API (default: '/jaeger/ui')
            tenant_id: Optional tenant ID for multi-tenancy
            headers: Optional additional headers to merge with API key header
            
        Returns:
            JaegerExtractor instance configured with API key authentication
        """
        auth_headers = {
            header_name: api_key
        }
        
        # Merge with any additional headers
        if headers:
            auth_headers.update(headers)
        
        # Use standard __init__ with pre-built headers
        return cls(base_url, base_path=base_path, tenant_id=tenant_id, headers=auth_headers)
    
    def fetch_trace(self, trace_id: str, **kwargs) -> Dict[str, Any]:
        """
        Fetch a trace by its ID from Jaeger.
        
        Args:
            trace_id: The trace ID to fetch (hex string)
            **kwargs: Additional query parameters
            
        Returns:
            Trace data in Jaeger format
            
        Raises:
            TraceExtractorError: If fetching trace fails
        """
        url = f"{self.api_base}/traces/{trace_id}"
        
        # Log the request
        logger.debug(f"Fetching trace: {trace_id}, URL: {url}")
        
        # Make the request
        response = self._make_request('GET', url, params=kwargs)
        
        try:
            data = response.json()
            
            # Jaeger v2 returns data with 'data' key containing array of traces
            if 'data' in data:
                traces = data.get('data', [])
                if traces:
                    return self._parse_jaeger_trace(traces[0], trace_id)
                else:
                    raise TraceExtractorError(f"No trace found with ID: {trace_id}")
            # Handle OTLP format if returned
            elif 'resourceSpans' in data:
                return self._parse_otlp_trace(data, trace_id)
            else:
                raise TraceExtractorError(f"Unexpected trace format: {list(data.keys())}")
            
        except json.JSONDecodeError as e:
            raise TraceExtractorError(f"Failed to parse trace response: {str(e)}")
        except KeyError as e:
            raise TraceExtractorError(f"Unexpected trace format: missing key {str(e)}")
    
    def _parse_otlp_trace(self, data: Dict[str, Any], trace_id: str) -> Dict[str, Any]:
        """
        Parse OTLP format trace data (Jaeger v2).
        
        Args:
            data: Raw OTLP trace data
            trace_id: The trace ID
            
        Returns:
            Parsed trace data with spans
        """
        spans = []
        
        # Parse resourceSpans (OTLP format used by Jaeger v2)
        for resource_span in data.get('resourceSpans', []):
            resource = resource_span.get('resource', {})
            resource_attrs = self._parse_otlp_attributes(resource.get('attributes', []))
            
            scope_spans = resource_span.get('scopeSpans', [])
            
            for scope_span in scope_spans:
                scope = scope_span.get('scope', {})
                scope_name = scope.get('name', 'unknown')
                
                for span in scope_span.get('spans', []):
                    parsed_span = self._parse_otlp_span(span, resource_attrs, scope_name)
                    spans.append(parsed_span)
        
        # Sort spans by start time
        spans.sort(key=lambda s: s.get('startTime', 0))
        
        return {
            'traceID': trace_id,
            'spans': spans,
            'spanCount': len(spans),
            'duration': self._calculate_trace_duration(spans),
            'rootSpan': self._find_root_span(spans),
            'services': list(set(s.get('serviceName', 'unknown') for s in spans))
        }
    
    def _parse_jaeger_trace(self, trace_data: Dict[str, Any], trace_id: str) -> Dict[str, Any]:
        """
        Parse Jaeger native format trace data (Jaeger v1).
        
        Args:
            trace_data: Raw Jaeger trace data
            trace_id: The trace ID
            
        Returns:
            Parsed trace data with spans
        """
        spans = []
        
        # Parse Jaeger format spans
        for span in trace_data.get('spans', []):
            parsed_span = self._parse_jaeger_span(span)
            spans.append(parsed_span)
        
        # Sort spans by start time
        spans.sort(key=lambda s: s.get('startTime', 0))
        
        return {
            'traceID': trace_id,
            'spans': spans,
            'spanCount': len(spans),
            'duration': self._calculate_trace_duration(spans),
            'rootSpan': self._find_root_span(spans),
            'services': list(set(s.get('serviceName', 'unknown') for s in spans)),
            'processes': trace_data.get('processes', {})
        }
    
    def _parse_otlp_span(self, span: Dict[str, Any], resource_attrs: Dict[str, Any], 
                         scope_name: str) -> Dict[str, Any]:
        """Parse a single span from OTLP format"""
        span_id = span.get('spanId', '')
        parent_span_id = span.get('parentSpanId', '')
        
        # Parse attributes
        attributes = self._parse_otlp_attributes(span.get('attributes', []))
        
        # Parse events
        events = []
        for event in span.get('events', []):
            events.append({
                'name': event.get('name', ''),
                'timestamp': event.get('timeUnixNano', 0),
                'attributes': self._parse_otlp_attributes(event.get('attributes', []))
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
        
        return {
            'spanId': span_id,
            'traceId': span.get('traceId', ''),
            'parentSpanId': parent_span_id,
            'name': span.get('name', ''),
            'kind': kind_map.get(span_kind, 'UNSPECIFIED'),
            'startTime': span.get('startTimeUnixNano', 0),
            'endTime': span.get('endTimeUnixNano', 0),
            'duration': span.get('endTimeUnixNano', 0) - span.get('startTimeUnixNano', 0),
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
    
    def _parse_jaeger_span(self, span: Dict[str, Any]) -> Dict[str, Any]:
        """Parse a single span from Jaeger native format"""
        span_id = span.get('spanID', '')
        parent_span_id = self._get_parent_span_id(span)
        
        # Parse tags (Jaeger's version of attributes)
        tags = span.get('tags', [])
        attributes = {tag['key']: tag['value'] for tag in tags}
        
        # Parse logs (Jaeger's version of events)
        events = []
        for log in span.get('logs', []):
            event_attrs = {field['key']: field['value'] for field in log.get('fields', [])}
            events.append({
                'timestamp': log.get('timestamp', 0),
                'attributes': event_attrs
            })
        
        # Determine status from tags
        status_code = attributes.get('otel.status_code', 'UNSET')
        is_error = attributes.get('error', False)
        
        # Get process info for service name
        process_id = span.get('processID', '')
        service_name = span.get('process', {}).get('serviceName', 'unknown')
        
        return {
            'spanId': span_id,
            'traceId': span.get('traceID', ''),
            'parentSpanId': parent_span_id,
            'name': span.get('operationName', ''),
            'kind': attributes.get('span.kind', 'INTERNAL'),
            'startTime': span.get('startTime', 0),
            'endTime': span.get('startTime', 0) + span.get('duration', 0),
            'duration': span.get('duration', 0),
            'attributes': attributes,
            'events': events,
            'status': {
                'code': 2 if is_error else 1,
                'message': attributes.get('otel.status_description', ''),
                'ok': not is_error
            },
            'serviceName': service_name,
            'processID': process_id,
            'warnings': span.get('warnings', [])
        }
    
    def _get_parent_span_id(self, span: Dict[str, Any]) -> str:
        """Extract parent span ID from Jaeger span references"""
        references = span.get('references', [])
        for ref in references:
            if ref.get('refType') == 'CHILD_OF':
                return ref.get('spanID', '')
        return ''
    
    def _parse_otlp_attributes(self, attributes: List[Dict[str, Any]]) -> Dict[str, Any]:
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
                result[key] = self._parse_otlp_attributes(value['kvlistValue'].get('values', []))
        
        return result
    
    def _calculate_trace_duration(self, spans: List[Dict[str, Any]]) -> int:
        """Calculate total trace duration from spans"""
        if not spans:
            return 0
        
        min_start = min(s.get('startTime', float('inf')) for s in spans)
        max_end = max(s.get('endTime', 0) for s in spans)
        
        return max_end - min_start
    
    def _find_root_span(self, spans: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
        """Find the root span (span with no parent)"""
        for span in spans:
            if not span.get('parentSpanId'):
                return span
        
        # If no span without parent, return first span
        return spans[0] if spans else None
    
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

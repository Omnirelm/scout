"""
Base classes for tracing integrations.
"""
from abc import ABC, abstractmethod
from typing import Dict, Any, List, Optional
import requests


class TraceExtractorError(Exception):
    """Base exception for trace extractor errors"""
    def __init__(self, message: str, status_code: Optional[int] = None, response_text: Optional[str] = None):
        super().__init__(message)
        self.status_code = status_code
        self.response_text = response_text


class TraceExtractor(ABC):
    """Abstract base class for trace extractors"""
    
    def __init__(self, base_url: str, headers: Optional[Dict[str, str]] = None):
        """
        Initialize the trace extractor.
        
        Args:
            base_url: Base URL for the tracing service API
            headers: Optional default headers for requests
        """
        self.base_url = base_url.rstrip('/')
        self.headers = headers or {}
    
    @abstractmethod
    def fetch_trace(self, trace_id: str, **kwargs) -> Dict[str, Any]:
        """
        Fetch a trace by its ID.
        
        Args:
            trace_id: The trace ID to fetch
            **kwargs: Additional parameters specific to the implementation
            
        Returns:
            Trace data as a dictionary
            
        Raises:
            TraceExtractorError: If fetching trace fails
        """
        pass
    
    
    def _make_request(self, method: str, url: str, params: Optional[Dict] = None, 
                     headers: Optional[Dict] = None, **kwargs) -> requests.Response:
        """
        Make HTTP request with error handling.
        
        Args:
            method: HTTP method (GET, POST, etc.)
            url: Request URL
            params: Query parameters
            headers: Request headers
            **kwargs: Additional requests parameters
            
        Returns:
            requests.Response object
            
        Raises:
            TraceExtractorError: If request fails
        """
        request_headers = {**self.headers, **(headers or {})}
        
        try:
            response = requests.request(
                method=method,
                url=url,
                params=params,
                headers=request_headers,
                **kwargs
            )
            
            if not response.ok:
                raise TraceExtractorError(
                    f"HTTP {response.status_code}: {response.reason}",
                    status_code=response.status_code,
                    response_text=response.text
                )
            
            return response
            
        except requests.exceptions.RequestException as e:
            raise TraceExtractorError(f"Request failed: {str(e)}")

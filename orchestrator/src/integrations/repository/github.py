"""
GitHub repository validation: URL parsing and API connectivity check.
"""
import re
import logging
from typing import Tuple, Optional

import requests

logger = logging.getLogger(__name__)

# GitHub URL patterns: https://github.com/owner/repo or https://github.com/owner/repo.git
_GITHUB_REPO_PATTERN = re.compile(
    r"^https?://(?:www\.)?github\.com/([^/]+)/([^/]+?)(?:\.git)?/?$",
    re.IGNORECASE,
)
GITHUB_API_BASE = "https://api.github.com"


def parse_github_repo_url(url: str) -> Tuple[str, str]:
    """Parse GitHub repo URL into (owner, repo). Raises ValueError if not a supported GitHub URL."""
    url = (url or "").strip()
    match = _GITHUB_REPO_PATTERN.match(url)
    if not match:
        raise ValueError(
            "Unsupported repository URL format; only GitHub URLs are supported (e.g. https://github.com/owner/repo)"
        )
    owner, repo = match.group(1), match.group(2)
    return owner, repo


def validate_repository(
    url: str,
    ref: Optional[str],
    pat: Optional[str],
) -> Tuple[bool, str]:
    """
    Validate repository URL and optional ref using GitHub API.
    Requires a request-scoped PAT.

    Args:
        url: Repository URL (e.g. https://github.com/org/repo).
        ref: Optional branch/tag to verify (e.g. main).
        pat: PAT for GitHub API from the request.

    Returns:
        (valid, message) tuple.
    """
    if not (pat or "").strip():
        return False, "Repository validation requires a PAT in the request."

    try:
        owner, repo = parse_github_repo_url(url)
    except ValueError as e:
        return False, str(e)

    headers = {
        "Accept": "application/vnd.github.v3+json",
        "Authorization": f"Bearer {(pat or '').strip()}",
    }

    # Check repo access
    repo_url = f"{GITHUB_API_BASE}/repos/{owner}/{repo}"
    try:
        resp = requests.get(repo_url, headers=headers, timeout=10)
    except requests.exceptions.Timeout:
        return False, "GitHub API request timed out."
    except requests.exceptions.RequestException as e:
        return False, f"Failed to reach GitHub API: {e}"

    if resp.status_code == 401:
        return False, "Authentication failed (invalid or expired PAT)."
    if resp.status_code == 403:
        return False, "Access denied to repository (forbidden)."
    if resp.status_code == 404:
        return False, "Repository not found or no access."
    if not resp.ok:
        return False, f"GitHub API error: {resp.status_code} {resp.reason}"

    # Optionally verify ref exists
    if ref and (ref := (ref or "").strip()):
        ref_url = f"{GITHUB_API_BASE}/repos/{owner}/{repo}/git/ref/heads/{ref}"
        try:
            ref_resp = requests.get(ref_url, headers=headers, timeout=10)
        except requests.exceptions.Timeout:
            return False, "GitHub API request timed out while checking ref."
        except requests.exceptions.RequestException as e:
            return False, f"Failed to verify ref: {e}"
        if ref_resp.status_code == 404:
            return False, f"Ref '{ref}' not found."
        if not ref_resp.ok:
            return False, f"Failed to verify ref: {ref_resp.status_code} {ref_resp.reason}"

    return True, "Repository accessible"

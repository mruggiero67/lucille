#!/usr/bin/env python3
"""
Jira Utilities Module
Shared utilities for Jira API interactions, including pagination handling.
"""

import logging
import requests
from typing import Dict, List, Optional, Union
from requests.auth import HTTPBasicAuth


logger = logging.getLogger(__name__)


def fetch_all_issues(
    session: requests.Session,
    base_url: str,
    jql: str,
    fields: Union[str, List[str]],
    max_results: Optional[int] = None,
    expand: Optional[str] = None,
    fields_by_keys: bool = False
) -> List[Dict]:
    """
    Fetch all issues matching JQL query, handling pagination with nextPageToken.
    
    This function handles the updated Jira API pagination that uses nextPageToken
    instead of the older startAt/maxResults approach.
    
    Args:
        session: Authenticated requests session
        base_url: Jira base URL (e.g., 'https://your-domain.atlassian.net')
        jql: JQL query string
        fields: Fields to retrieve (string or list of strings)
        max_results: Maximum number of results to fetch (None for no limit)
        expand: Additional data to expand (e.g., 'changelog')
        fields_by_keys: Whether to use field keys instead of field names
        
    Returns:
        List of issue dictionaries
        
    Raises:
        requests.exceptions.RequestException: If API request fails
    """
    # Normalize fields parameter
    if isinstance(fields, str):
        if fields == "*all":
            fields_param = "*all"
        else:
            # Convert comma-separated string to list
            fields_param = [f.strip() for f in fields.split(",")]
    else:
        fields_param = fields
    
    all_issues = []
    next_page_token = None
    page_size = min(100, max_results) if max_results else 100  # Jira API limit is 100 per request
    
    logger.info(f"Fetching issues with pagination (max: {max_results or 'unlimited'})")
    
    while True:
        # Build request parameters
        params = {
            'jql': jql,
            'fields': fields_param,
            'maxResults': page_size,
            'fieldsByKeys': fields_by_keys
        }
        
        # Add optional parameters
        if expand:
            params['expand'] = expand
        
        # Add nextPageToken if we have one (not on first request)
        if next_page_token:
            params['nextPageToken'] = next_page_token
        
        logger.debug(f"Fetching page with maxResults={page_size}, nextPageToken={next_page_token}")
        
        try:
            # Make the API request
            url = f"{base_url.rstrip('/')}/rest/api/3/search/jql"
            response = session.get(url, params=params)
            response.raise_for_status()
            result = response.json()
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to fetch issues page with token={next_page_token}: {e}")
            if next_page_token is None:
                # If first page fails, re-raise the exception
                raise
            else:
                # If subsequent page fails, return what we have so far
                logger.warning(f"Returning {len(all_issues)} issues due to pagination error")
                break
        
        issues = result.get('issues', [])
        
        if not issues:
            logger.debug("No more issues found, stopping pagination")
            break
        
        all_issues.extend(issues)
        
        # Check pagination info from API response
        total = result.get('total', 0)
        next_page_token = result.get('nextPageToken')
        
        logger.debug(f"API Response: total={total}, received={len(issues)}, nextPageToken={next_page_token}")
        
        # If no nextPageToken, we've reached the end
        if not next_page_token:
            logger.debug(f"No more pages available (total fetched: {len(all_issues)})")
            break
        
        # Check if we've reached the max_results limit
        if max_results and len(all_issues) >= max_results:
            logger.debug(f"Reached max_results limit of {max_results}")
            break
        
        # Adjust page size for remaining items
        if max_results:
            remaining = max_results - len(all_issues)
            page_size = min(100, remaining)
        
        logger.debug(f"Fetched {len(all_issues)} issues so far, continuing...")
    
    # Trim to max_results if we got more than requested
    if max_results and len(all_issues) > max_results:
        all_issues = all_issues[:max_results]
    
    logger.info(f"Successfully fetched {len(all_issues)} total issues")
    return all_issues


def create_jira_session(base_url: str, username: str, api_token: str) -> requests.Session:
    """
    Create and configure a requests session for Jira API calls.
    
    Args:
        base_url: Jira base URL
        username: Jira username/email
        api_token: Jira API token
        
    Returns:
        Configured requests session
        
    Raises:
        requests.exceptions.RequestException: If authentication test fails
    """
    session = requests.Session()
    session.auth = HTTPBasicAuth(username, api_token)
    session.headers.update({
        "Accept": "application/json",
        "Content-Type": "application/json"
    })
    
    # Test the connection
    test_url = f"{base_url.rstrip('/')}/rest/api/3/myself"
    try:
        response = session.get(test_url)
        response.raise_for_status()
        user_info = response.json()
        logger.info(f"Successfully authenticated as: {user_info.get('displayName', username)}")
        return session
    except requests.exceptions.RequestException as e:
        logger.error(f"Failed to authenticate with Jira: {e}")
        raise


def make_jira_request(
    session: requests.Session,
    base_url: str,
    endpoint: str,
    params: Optional[Dict] = None
) -> Dict:
    """
    Make authenticated request to Jira API.
    
    Args:
        session: Authenticated requests session
        base_url: Jira base URL
        endpoint: API endpoint (e.g., 'search/jql', 'issue/KEY-123')
        params: Query parameters
        
    Returns:
        JSON response data
        
    Raises:
        requests.exceptions.RequestException: If API request fails
    """
    url = f"{base_url.rstrip('/')}/rest/api/3/{endpoint}"
    logger.debug(f"Making Jira request to: {url}")
    
    try:
        response = session.get(url, params=params)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        logger.error(f"Jira API request failed: {e}")
        raise

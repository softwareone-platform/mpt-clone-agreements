"""
Utility functions for microsoft_clone_agreement scripts.
"""
import json
import logging
import logging.config
import time
from pathlib import Path
from typing import Dict, Any, Optional, Union
import rich.console
import httpx
from urllib.parse import urljoin


def validate_agreement_id(agreement_id: str) -> None:
    """Validate that agreement ID starts with AGR- prefix."""
    if not agreement_id.startswith('AGR-'):
        raise ValueError(f"Invalid agreement ID format: '{agreement_id}'. Must start with 'AGR-' (e.g., AGR-1234-5678-9012)")


def validate_listing_id(listing_id: str) -> None:
    """Validate that listing ID starts with LST- prefix."""
    if not listing_id.startswith('LST-'):
        raise ValueError(f"Invalid listing ID format: '{listing_id}'. Must start with 'LST-' (e.g., LST-9279-6638)")


def validate_licensee_id(licensee_id: str) -> None:
    """Validate that licensee ID starts with LCE- prefix."""
    if not licensee_id.startswith('LCE-'):
        raise ValueError(f"Invalid licensee ID format: '{licensee_id}'. Must start with 'LCE-' (e.g., LCE-1234-5678-9012)")


def calculate_unit_sp(unit_pp: float, markup: float) -> float:
    """
    Calculate unit selling price (unitSP) from unit purchase price (unitPP) and markup.
    
    Formula: unitSP = unitPP * (1 + markup)
    
    Args:
        unit_pp: Unit purchase price
        markup: Markup as a decimal (e.g., 0.02 for 2% markup, 0.15 for 15% markup)
               Note: If markup is passed as percentage (e.g., 2 for 2%), it must be
               converted to decimal (2/100 = 0.02) before calling this function.
        
    Returns:
        Unit selling price rounded to 2 decimal places
    """
    unit_sp = unit_pp * (1 + markup)
    return round(unit_sp, 2)


def ensure_bearer(token: str) -> str:
    """Ensure token has Bearer prefix."""
    token = token.strip()
    return token if token.lower().startswith('bearer ') else f"Bearer {token}"


def has_more_pages(page: Optional[Dict[str, Any]]) -> bool:
    """
    Check if there are more pages of data to fetch.
    
    Args:
        page: API response page containing metadata
        
    Returns:
        bool: True if there are more pages, False if this is the last or only page
    """
    if page is None:
        return True
    
    meta = page.get('$meta', {}).get('pagination', {})
    offset = meta.get('offset', 0)
    limit = meta.get('limit', 0)
    total = meta.get('total', 0)
    
    return offset + limit < total


def setup_logging(
    script_name: str,
    debug: bool = False,
    agreement_id: Optional[str] = None
) -> logging.Logger:
    """
    Configure and return logger instance.
    
    Args:
        script_name: Name of the script (without .py extension) for log file naming
        debug: If True, sets console logging level to DEBUG, otherwise INFO. 
               Log file always uses DEBUG level.
        agreement_id: Optional agreement ID to store logs in agreement-specific folder
    """
    console_level = "DEBUG" if debug else "INFO"
    file_level = "DEBUG"
    
    console = rich.console.Console(width=178)
    
    if agreement_id:
        logs_dir = Path("output") / agreement_id / "logs"
    else:
        logs_dir = Path("output") / "unknown" / "logs"
    logs_dir.mkdir(parents=True, exist_ok=True)
    
    log_file_path = logs_dir / f"{script_name}.log"
    
    logging_config = {
        "version": 1,
        "disable_existing_loggers": False,
        "formatters": {
            "verbose": {
                "format": "{asctime} {name} {levelname} (pid: {process}) {message}",
                "style": "{",
            },
            "rich": {
                "format": "{message}",
                "style": "{",
            },
        },
        "handlers": {
            "file": {
                "class": "logging.handlers.RotatingFileHandler",
                "formatter": "verbose",
                "maxBytes": 1024 * 1024 * 50,
                "backupCount": 50,
                "filename": str(log_file_path),
                "level": file_level,
            },
            "rich": {
                "class": "rich.logging.RichHandler",
                "formatter": "rich",
                "log_time_format": lambda x: x.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3],
                "rich_tracebacks": True,
                "level": console_level,
                "console": console,
            },
        },
        "root": {
            "handlers": ["rich", "file"],
            "level": "DEBUG",
        },
        "loggers": {
            "mypackage": {
                "handlers": ["rich", "file"],
                "level": "DEBUG",
                "propagate": False,
            },
        },
    }

    logging.config.dictConfig(logging_config)
    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG)
    logger.info(f"Console logging level: {console_level}, File logging level: {file_level}")
    logger.info(f"Log file: {log_file_path}")
    logger.debug(f"Debug logs are always written to file regardless of --debug flag")
    return logger


def validate_agreement_and_tokens(
    agreement_id: str,
    api_url: str,
    ops_token: str,
    vendor_token: str,
    logger: logging.Logger
) -> None:
    """
    Validate agreement and tokens by fetching agreement with both tokens.
    
    Validates:
    1. Agreement can be retrieved with OPS_TOKEN
    2. Agreement can be retrieved with VENDOR_TOKEN
    3. Agreement status is "Active"
    4. OPS_TOKEN can see price.defaultMarkup (validates it's an ops token)
    5. VENDOR_TOKEN cannot see price.defaultMarkup (validates it's a vendor token)
    
    Args:
        agreement_id: Agreement ID to validate (must start with AGR-)
        api_url: Base API URL
        ops_token: Operations token
        vendor_token: Vendor token
        logger: Logger instance
        
    Raises:
        ValueError: If agreement_id format is invalid
        RuntimeError: If any validation fails
    """
    validate_agreement_id(agreement_id)
    
    logger.info(f"Validating agreement {agreement_id} and tokens...")
    
    logger.debug(f"Fetching agreement {agreement_id} with OPS_TOKEN")
    url = urljoin(api_url, f'/public/v1/commerce/agreements/{agreement_id}')
    
    with create_http_client(ops_token, 'Agreement Clone Validator') as client:
        ops_agreement = make_request_with_retry(
            client=client,
            method='GET',
            url=url,
            logger=logger,
            max_retries=DEFAULT_MAX_RETRIES,
            parse_json=True
        )
    
    if not ops_agreement:
        raise RuntimeError(
            f"Failed to fetch agreement {agreement_id} with OPS_TOKEN. "
            f"OPS_TOKEN may be invalid or insufficient permissions."
        )
    
    logger.debug(f"Successfully fetched agreement with OPS_TOKEN")
    
    logger.debug(f"Fetching agreement {agreement_id} with VENDOR_TOKEN")
    
    with create_http_client(vendor_token, 'Agreement Clone Validator') as client:
        vendor_agreement = make_request_with_retry(
            client=client,
            method='GET',
            url=url,
            logger=logger,
            max_retries=DEFAULT_MAX_RETRIES,
            parse_json=True
        )
    
    if not vendor_agreement:
        raise RuntimeError(
            f"Failed to fetch agreement {agreement_id} with VENDOR_TOKEN. "
            f"VENDOR_TOKEN may be invalid or insufficient permissions."
        )
    
    logger.debug(f"Successfully fetched agreement with VENDOR_TOKEN")
    
    agreement_status = ops_agreement.get('status', '').strip()
    if agreement_status not in ['Active', 'Terminated']:
        raise RuntimeError(
            f"Agreement {agreement_id} status is '{agreement_status}', expected 'Active' or 'Terminated'. "
            f"Cannot proceed with operations on agreement with status '{agreement_status}'."
        )
    logger.info(f"Agreement {agreement_id} status is {agreement_status} ✓")
    
    price_node = ops_agreement.get('price', {})
    default_markup = price_node.get('defaultMarkup') if isinstance(price_node, dict) else None
    
    if default_markup is None:
        # Try alternative paths in case structure is different
        default_markup = ops_agreement.get('defaultMarkup')
    
    if default_markup is None:
        raise RuntimeError(
            f"OPS_TOKEN cannot see price.defaultMarkup in agreement {agreement_id}. "
            f"This suggests OPS_TOKEN may be invalid or not have sufficient permissions. "
            f"Available keys in agreement: {list(ops_agreement.keys())}"
        )
    logger.info(f"OPS_TOKEN can see price.defaultMarkup: {default_markup} ✓")
    
    vendor_price_node = vendor_agreement.get('price', {})
    vendor_default_markup = vendor_price_node.get('defaultMarkup') if isinstance(vendor_price_node, dict) else None
    
    if vendor_default_markup is None:
        # Try alternative paths
        vendor_default_markup = vendor_agreement.get('defaultMarkup')
    
    if vendor_default_markup is not None:
        raise RuntimeError(
            f"VENDOR_TOKEN can see price.defaultMarkup in agreement {agreement_id} "
            f"(value: {vendor_default_markup}). This suggests VENDOR_TOKEN may actually be an OPS_TOKEN. "
            f"Vendor tokens should not have access to price.defaultMarkup."
        )
    logger.info(f"VENDOR_TOKEN cannot see price.defaultMarkup (as expected) ✓")
    
    logger.info(f"All validations passed for agreement {agreement_id} and tokens")


# HTTP Client Configuration
HTTP_TIMEOUT = 60.0  # 60 seconds timeout
DEFAULT_MAX_RETRIES = 3
BACKOFF_BASE_DELAY = 2  # Base delay for exponential backoff (seconds)


def create_http_client(
    token: str,
    user_agent: str = 'Microsoft Agreement Processor',
    http2: bool = True
) -> httpx.Client:
    """
    Create a configured httpx client with HTTP/2 support and common headers.
    
    Args:
        token: Bearer token for authentication
        user_agent: User agent string for requests
        http2: Whether to enable HTTP/2 (default: True)
        
    Returns:
        Configured httpx.Client instance
    """
    headers = {
        'Authorization': ensure_bearer(token),
        'Content-Type': 'application/json',
        'Accept-Encoding': 'gzip',
        'User-agent': user_agent
    }
    
    return httpx.Client(
        headers=headers,
        timeout=HTTP_TIMEOUT,
        follow_redirects=True,
        http2=http2,
    )


def make_request_with_retry(
    client: httpx.Client,
    method: str,
    url: str,
    logger: logging.Logger,
    max_retries: int = DEFAULT_MAX_RETRIES,
    parse_json: bool = True,
    **kwargs
) -> Union[Dict[str, Any], httpx.Response, None]:
    """
    Make an HTTP request with retry logic and exponential backoff.
    
    Args:
        client: httpx.Client instance
        method: HTTP method (GET, POST, PUT, etc.)
        url: Request URL
        logger: Logger instance
        max_retries: Maximum number of retry attempts (default: 3)
        parse_json: Whether to parse response as JSON (default: True)
        **kwargs: Additional arguments to pass to client.request()
        
    Returns:
        Parsed JSON dict if parse_json=True, httpx.Response if parse_json=False, or None on failure
    """
    last_error = None
    
    for attempt in range(max_retries):
        try:
            logger.debug(f"Making {method} request to {url} (attempt {attempt + 1}/{max_retries})")
            if kwargs.get('json'):
                logger.debug(f"Request payload: {json.dumps(kwargs['json'], indent=2) if isinstance(kwargs['json'], dict) else kwargs['json']}")
            
            response = client.request(
                method=method,
                url=url,
                **kwargs
            )
            
            logger.debug(f"Response status code: {response.status_code}")
            
            if response.status_code >= 400:
                error_text = response.text[:500] if response.text else "No response body"
                last_error = f"HTTP {response.status_code}: {error_text}"
                logger.error(f"Request failed with status {response.status_code}: {error_text}")
                
                if response.status_code >= 500 or response.status_code in [408, 429]:
                    if attempt < max_retries - 1:
                        wait_time = BACKOFF_BASE_DELAY * (2 ** attempt)
                        logger.warning(f"Attempt {attempt + 1} failed with retryable error, retrying in {wait_time} seconds...")
                        time.sleep(wait_time)
                        continue
                
                return None
            
            if parse_json:
                try:
                    return response.json()
                except ValueError as e:
                    logger.error(f"Failed to parse JSON response: {str(e)}")
                    logger.debug(f"Response text: {response.text[:500]}")
                    return None
            
            return response
            
        except (httpx.ReadTimeout, httpx.ConnectTimeout, httpx.TimeoutException) as e:
            last_error = f"Timeout error: {str(e)}"
            logger.warning(f"Request timeout on attempt {attempt + 1}/{max_retries}: {str(e)}")
            if attempt < max_retries - 1:
                wait_time = BACKOFF_BASE_DELAY * (2 ** attempt)
                logger.warning(f"Retrying in {wait_time} seconds...")
                time.sleep(wait_time)
                continue
            else:
                logger.error(f"Request failed after {max_retries} attempts due to timeout")
                return None
                
        except (httpx.RemoteProtocolError, httpx.ConnectError, httpx.NetworkError) as e:
            last_error = f"Connection error: {str(e)}"
            logger.warning(f"Connection error on attempt {attempt + 1}/{max_retries}: {str(e)}")
            if attempt < max_retries - 1:
                wait_time = BACKOFF_BASE_DELAY * (2 ** attempt)
                logger.warning(f"Retrying in {wait_time} seconds...")
                time.sleep(wait_time)
                continue
            else:
                logger.error(f"Request failed after {max_retries} attempts due to connection error")
                return None
                
        except Exception as e:
            last_error = f"Unexpected error: {str(e)}"
            logger.error(f"Unexpected error on attempt {attempt + 1}/{max_retries}: {str(e)}")
            error_str = str(e).lower()
            if any(keyword in error_str for keyword in ['timeout', 'connection', 'network', 'reset']):
                if attempt < max_retries - 1:
                    wait_time = BACKOFF_BASE_DELAY * (2 ** attempt)
                    logger.warning(f"Retrying in {wait_time} seconds...")
                    time.sleep(wait_time)
                    continue
            else:
                logger.error(f"Non-retryable error, stopping retries")
                return None
    
    logger.error(f"Request failed after {max_retries} attempts. Last error: {last_error}")
    return None


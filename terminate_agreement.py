import logging
import os
import sys
from pathlib import Path
from dotenv import load_dotenv
from urllib.parse import urljoin
from typing import Dict, List, Any, Optional
import argparse

# Add the script's directory to the path to allow importing utils
script_dir = Path(__file__).parent
if str(script_dir) not in sys.path:
    sys.path.insert(0, str(script_dir))

from utils import (
    validate_agreement_id,
    setup_logging,
    has_more_pages,
    validate_agreement_and_tokens,
    create_http_client,
    make_request_with_retry
)


# Load environment variables from ~/.mpt-clone-agreement (reuse same convention)
ENV_PATH = Path.home() / '.mpt-clone-agreement'
load_dotenv(ENV_PATH)


class ConfigurationManager:
    REQUIRED_VARS = {
        'OPS_TOKEN': 'API Operations Token',
        'VENDOR_TOKEN': 'Vendor API Token',
        'API_URL': 'API Base URL',
    }

    @classmethod
    def load_config(cls) -> Dict[str, str]:
        """Load and validate environment variables."""
        config = {key: os.getenv(key) for key in cls.REQUIRED_VARS}
        missing_vars = [f"{key} ({desc})" for key, desc in cls.REQUIRED_VARS.items()
                        if not config.get(key)]
        if missing_vars:
            raise EnvironmentError(
                f"Missing required environment variables:\n"
                f"{chr(10).join(missing_vars)}\n"
                f"Please ensure these are set in {ENV_PATH}"
            )
        return config


def parse_arguments() -> argparse.Namespace:
    """
    Parse command line arguments.
    """
    parser = argparse.ArgumentParser(description='Terminate all subscriptions for an agreement')
    parser.add_argument(
        '--agreement-id',
        type=str,
        required=True,
        dest='agreement_id',
        help='Agreement ID to filter subscriptions (e.g., AGR-0413-5979-0750)'
    )
    parser.add_argument(
        '--debug',
        action='store_true',
        help='Enable debug level logging (more verbose output)'
    )
    args = parser.parse_args()
    
    validate_agreement_id(args.agreement_id)
    
    return args


def fetch_agreement_subscriptions(
    base_url: str,
    token: str,
    agreement_id: str,
    logger: logging.Logger
) -> List[Dict[str, Any]]:
    """
    Retrieve all subscriptions for the given agreement from the vendor portal.
    """
    items: List[Dict[str, Any]] = []
    page: Optional[Dict[str, Any]] = None
    limit = 1000
    offset = 0

    select = "agreement,agreement.listing.priceList,audit.created,audit.updated,seller.address"
    query = (
        f"/public/v1/commerce/subscriptions"
        f"?select={select}"
        f"&eq(status,active)"
        f"&eq(agreement.id,{agreement_id})"
        f"&order=-audit.created.at"
        f"&offset={{offset}}&limit={{limit}}"
    )

    with create_http_client(token, 'Agreement Termination Utility') as client:
        while has_more_pages(page):
            url = urljoin(base_url, query.format(offset=offset, limit=limit))
            logger.debug(f"GET {url}")
            page = make_request_with_retry(
                client=client,
                method='GET',
                url=url,
                logger=logger,
                parse_json=True
            )
            
            if not page:
                logger.error(f"Failed to fetch subscriptions page at offset {offset}")
                break
            
            data = page.get('data', [])
            items.extend(data)
            logger.info(page.get('$meta', {}))
            offset += limit

    logger.info(f"Collected {len(items)} subscriptions for agreement {agreement_id}")
    return items


def terminate_subscription(
    base_url: str,
    token: str,
    subscription_id: str,
    logger: logging.Logger
) -> bool:
    """
    POST to /commerce/subscriptions/{id}/terminate with body { "id": "<id>" }.
    """
    url = urljoin(base_url, f"/public/v1/commerce/subscriptions/{subscription_id}/terminate")
    body = {"id": subscription_id}
    logger.debug(f"POST {url} body={body}")
    
    with create_http_client(token, 'Agreement Termination Utility') as client:
        resp = make_request_with_retry(
            client=client,
            method='POST',
            url=url,
            logger=logger,
            parse_json=False,
            json=body
        )
    
    if resp and resp.status_code in (200, 202, 204):
        logger.info(f"Terminated subscription {subscription_id}")
        return True
    else:
        status_code = resp.status_code if resp else 0
        error_text = resp.text[:200] if resp and resp.text else "No response"
        logger.error(f"Failed to terminate {subscription_id}: {status_code} {error_text}")
        return False


def main():
    args = parse_arguments()
    script_name = Path(__file__).stem
    logger = setup_logging(script_name, args.debug, args.agreement_id)

    try:
        # Validate that dump_agreement.py has been run first
        output_dir = Path("output") / args.agreement_id
        agreement_object_path = output_dir / 'agreement_object.json'
        
        if not output_dir.exists():
            logger.error(
                f"Output directory for agreement {args.agreement_id} does not exist: {output_dir}\n"
                f"Please run dump_agreement.py first to create the agreement dump."
            )
            return
        
        if not agreement_object_path.exists():
            logger.error(
                f"Agreement object file not found: {agreement_object_path}\n"
                f"Please run dump_agreement.py first to create the agreement dump."
            )
            return
        
        logger.info(f"Verified agreement dump exists at {output_dir}")
        
        config = ConfigurationManager.load_config()
        
        validate_agreement_and_tokens(
            args.agreement_id,
            config['API_URL'],
            config['OPS_TOKEN'],
            config['VENDOR_TOKEN'],
            logger
        )
        
        base_url = config['API_URL']
        token = config['VENDOR_TOKEN']

        logger.info(f"Starting termination for agreement {args.agreement_id}")
        subs = fetch_agreement_subscriptions(base_url, token, args.agreement_id, logger)
        if not subs:
            logger.warning(f"No subscriptions found for agreement {args.agreement_id}")
            return

        successes = 0
        failures = 0
        for sub in subs:
            sub_id = sub.get('id')
            if not sub_id:
                logger.error(f"Skipping subscription without id: {sub}")
                failures += 1
                continue
            ok = terminate_subscription(base_url, token, sub_id, logger)
            successes += 1 if ok else 0
            failures += 0 if ok else 1

        logger.info(f"Termination completed. Success: {successes}, Failed: {failures}, Total: {len(subs)}")

    except Exception as e:
        logger.error(f"Error in main execution: {str(e)}")
        raise


if __name__ == "__main__":
    main()



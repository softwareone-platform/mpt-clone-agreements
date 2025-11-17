import logging
import os
import sys
import json
from pathlib import Path
from dotenv import load_dotenv
from urllib.parse import urljoin
from typing import Dict, Any
import argparse

# Add the script's directory to the path to allow importing utils
script_dir = Path(__file__).parent
if str(script_dir) not in sys.path:
    sys.path.insert(0, str(script_dir))

from utils import (
    validate_agreement_id,
    setup_logging,
    create_http_client,
    make_request_with_retry
)


# Load environment variables from ~/.mpt-clone-agreement
ENV_PATH = Path.home() / '.mpt-clone-agreement'
load_dotenv(ENV_PATH)


class ConfigurationManager:
    REQUIRED_VARS = {
        'OPS_TOKEN': 'API Operations Token',
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
    parser = argparse.ArgumentParser(description='Add audit records for cloned agreements')
    parser.add_argument(
        '--agreement-id',
        type=str,
        required=True,
        dest='agreement_id',
        help='Original/old Agreement ID (e.g., AGR-0413-5979-0750)'
    )
    parser.add_argument(
        '--debug',
        action='store_true',
        help='Enable debug level logging (more verbose output)'
    )
    args = parser.parse_args()
    
    validate_agreement_id(args.agreement_id)
    
    return args


def load_agreement_json(
    file_path: Path,
    logger: logging.Logger
) -> Dict[str, Any]:
    """
    Load and parse an agreement JSON file.
    """
    logger.debug(f"Loading agreement from {file_path}")
    
    if not file_path.exists():
        raise FileNotFoundError(f"Agreement file not found: {file_path}")
    
    with open(file_path, 'r', encoding='utf-8') as f:
        data = json.load(f)
    
    logger.debug(f"Successfully loaded agreement from {file_path}")
    return data


def create_audit_record(
    base_url: str,
    token: str,
    event: str,
    summary: str,
    details: str,
    object_id: str,
    documents: Dict[str, Any],
    logger: logging.Logger
) -> bool:
    """
    Create an audit record via POST to /public/v1/audit/records.
    
    Args:
        base_url: Base API URL
        token: Authentication token
        event: Event type (e.g., "extensions.cline.agreement")
        summary: Short summary of the audit event
        details: Detailed description of the audit event
        object_id: ID of the object being audited (agreement ID)
        documents: Dictionary of documents to attach to the audit record
        logger: Logger instance
        
    Returns:
        bool: True if successful, False otherwise
    """
    url = urljoin(base_url, "/public/v1/audit/records")
    
    body = {
        "event": event,
        "summary": summary,
        "details": details,
        "type": "Private",
        "object": {
            "id": object_id
        },
        "documents": documents
    }
    
    logger.debug(f"Creating audit record for {object_id}")
    logger.debug(f"POST {url}")
    
    with create_http_client(token, 'Agreement Clone Audit') as client:
        resp = make_request_with_retry(
            client=client,
            method='POST',
            url=url,
            logger=logger,
            parse_json=False,
            json=body
        )
    
    if resp and resp.status_code in (200, 201, 202, 204):
        logger.info(f"Successfully created audit record for agreement {object_id}")
        return True
    else:
        status_code = resp.status_code if resp else 0
        error_text = resp.text[:500] if resp and resp.text else "No response"
        logger.error(f"Failed to create audit record for {object_id}: {status_code} {error_text}")
        return False


def main():
    args = parse_arguments()
    script_name = Path(__file__).stem
    logger = setup_logging(script_name, args.debug, args.agreement_id)

    try:
        # Validate that dump_agreement.py and create_new_agreement.py have been run first
        output_dir = Path("output") / args.agreement_id
        agreement_object_path = output_dir / 'agreement_object.json'
        final_agreement_path = output_dir / 'final_agreement.json'
        
        if not output_dir.exists():
            logger.error(
                f"Output directory for agreement {args.agreement_id} does not exist: {output_dir}\n"
                f"Please run dump_agreement.py first to create the agreement dump."
            )
            return
        
        if not agreement_object_path.exists():
            logger.error(
                f"Old agreement file not found: {agreement_object_path}\n"
                f"Please run dump_agreement.py first to create the agreement dump."
            )
            return
        
        if not final_agreement_path.exists():
            logger.error(
                f"New agreement file not found: {final_agreement_path}\n"
                f"Please run create_new_agreement.py first to create the new agreement."
            )
            return
        
        logger.info(f"Verified agreement files exist at {output_dir}")
        
        # Load agreement files
        old_agreement = load_agreement_json(agreement_object_path, logger)
        new_agreement = load_agreement_json(final_agreement_path, logger)
        
        old_agreement_id = old_agreement.get('id')
        new_agreement_id = new_agreement.get('id')
        
        if not old_agreement_id:
            logger.error(f"Could not extract ID from old agreement file: {agreement_object_path}")
            return
        
        if not new_agreement_id:
            logger.error(f"Could not extract ID from new agreement file: {final_agreement_path}")
            return
        
        logger.info(f"Old Agreement ID: {old_agreement_id}")
        logger.info(f"New Agreement ID: {new_agreement_id}")
        
        config = ConfigurationManager.load_config()
        base_url = config['API_URL']
        token = config['OPS_TOKEN']
        
        # Prepare documents for audit records
        documents = {
            "Old Agreement": old_agreement,
            "New Agreement": new_agreement
        }
        
        # Create audit record for old agreement
        logger.info(f"Creating audit record for old agreement {old_agreement_id}")
        success_old = create_audit_record(
            base_url=base_url,
            token=token,
            event="extensions.clone.agreement",
            summary=f"Agreement has been cloned to {new_agreement_id}",
            details=f"The agreement has been cloned to new one with id {new_agreement_id}",
            object_id=old_agreement_id,
            documents=documents,
            logger=logger
        )
        
        # Create audit record for new agreement
        logger.info(f"Creating audit record for new agreement {new_agreement_id}")
        success_new = create_audit_record(
            base_url=base_url,
            token=token,
            event="extensions.clone.agreement",
            summary=f"Agreement has been cloned from {old_agreement_id}",
            details=f"The agreement has been cloned from the one with id {old_agreement_id}",
            object_id=new_agreement_id,
            documents=documents,
            logger=logger
        )
        
        # Summary
        if success_old and success_new:
            logger.info("✓ Successfully created audit records for both agreements")
        elif success_old:
            logger.warning("⚠ Created audit record for old agreement only (new agreement failed)")
        elif success_new:
            logger.warning("⚠ Created audit record for new agreement only (old agreement failed)")
        else:
            logger.error("✗ Failed to create audit records for both agreements")

    except Exception as e:
        logger.error(f"Error in main execution: {str(e)}")
        raise


if __name__ == "__main__":
    main()


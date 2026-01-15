# Reveal Markets API
import requests
from typing import List, Dict, Optional
from datetime import datetime, timedelta
from integrations.skybox_client import SkyboxClient
import logging

logger = logging.getLogger(__name__)
class RevealMarketsClient:
    """Client for Reveal Markets API operations"""
    
    BASE_URL = "https://portal.revealmarkets.com/public/api/v1"
    
    def __init__(self, api_token: str):
        self.headers = {
            "Authorization": f"Token {api_token}",
            "Content-Type": "application/json"
        }
        self.session = requests.Session()
        self.session.headers.update(self.headers)
    
    def get_unmatched_transactions(
        self,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
        pending_only: bool = False
    ) -> List[Dict]:
        """
        Fetch all unmatched credit card transactions
        
        Args:
            start_date: Filter by transaction date
            end_date: Filter by transaction date
            pending_only: Only get pending transactions
        """
        # Default to last 30 days
        if not end_date:
            end_date = datetime.now()
        if not start_date:
            start_date = end_date - timedelta(days=30)
        
        params = {
            "has_match": "false",  # Only unmatched
            "is_pending": "no" if not pending_only else "yes",
            "date_from": start_date.strftime("%Y-%m-%d"),
            "date_to": end_date.strftime("%Y-%m-%d"),
        }
        
        all_transactions = []
        url = f"{self.BASE_URL}/purchasing/banking-transactions/"
        
        try:
            # Handle pagination
            while url:
                response = self.session.get(url, params=params)
                response.raise_for_status()
                data = response.json()
                
                results = data.get('results', [])
                all_transactions.extend(results)
                
                # Check for next page
                url = data.get('next')
                params = {}  # Next URL already has params
            
            logger.info(f"Retrieved {len(all_transactions)} unmatched transactions")
            return all_transactions
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to fetch Reveal transactions: {e}")
            raise
    
    def mark_transaction_matched(
        self,
        transaction_id: int,
        skybox_purchase_id: int
    ) -> bool:
        """
        Mark a transaction as matched to a Skybox purchase
        
        Note: Exact endpoint TBD - may need to create matching_group
        """
        # TODO: Confirm exact endpoint with Michael
        # This is a placeholder based on the API structure
        url = f"{self.BASE_URL}/purchasing/banking-transactions/{transaction_id}/"
        
        payload = {
            "matching_group": {
                "purchase_id": skybox_purchase_id,
                "matched": True
            }
        }
        
        try:
            response = self.session.patch(url, json=payload)
            response.raise_for_status()
            logger.info(f"Marked transaction {transaction_id} as matched to {skybox_purchase_id}")
            return True
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to mark transaction {transaction_id} as matched: {e}")
            return False


# Helper Functions

def load_credit_card_mapping(mapping_file: str = "config/credit_card_mapping.json") -> Dict:
    """
    Load credit card mapping from file
    
    Format:
    {
        "Venture X": {
            "account_name": "Venture X",
            "last_four": "3969",
            "credit_card_group_id": 123,
            "credit_card_id": 456
        },
        ...
    }
    """
    import json
    try:
        with open(mapping_file, 'r') as f:
            return json.load(f)
    except FileNotFoundError:
        logger.warning(f"Credit card mapping file not found: {mapping_file}")
        return {}


def get_cc_info_by_account_name(
    account_name: str, 
    mapping: Dict
) -> Optional[Dict]:
    """Get CC group/card IDs by Reveal account name"""
    return mapping.get(account_name)


def get_cc_info_by_last_four(
    last_four: str,
    mapping: Dict
) -> Optional[Dict]:
    """Get CC info by last 4 digits"""
    for name, info in mapping.items():
        if info.get('last_four') == last_four:
            return info
    return None


#  Testing Utilities

def test_skybox_connection(api_token: str, account_id: int, app_token: str) -> bool:
    """Test Skybox API connectivity"""
    client = SkyboxClient(api_token, account_id, app_token)
    try:
        # Try to fetch a small set of purchases
        purchases = client.get_unreconciled_purchases(
            start_date=datetime.now() - timedelta(days=1),
            end_date=datetime.now()
        )
        logger.info(f"✓ Skybox connection successful - found {len(purchases)} purchases")
        return True
    except Exception as e:
        logger.error(f"✗ Skybox connection failed: {e}")
        return False


def test_reveal_connection(api_token: str) -> bool:
    """Test Reveal Markets API connectivity"""
    client = RevealMarketsClient(api_token)
    try:
        # Try to fetch transactions
        transactions = client.get_unmatched_transactions(
            start_date=datetime.now() - timedelta(days=1),
            end_date=datetime.now()
        )
        logger.info(f"✓ Reveal Markets connection successful - found {len(transactions)} transactions")
        return True
    except Exception as e:
        logger.error(f"✗ Reveal Markets connection failed: {e}")
        return False
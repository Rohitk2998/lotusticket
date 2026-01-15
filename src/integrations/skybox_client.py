# Skybox API wrapper
"""
API Clients for Skybox and Reveal Markets
Production-ready with error handling and pagination
"""
import requests
from typing import List, Dict, Optional
from datetime import datetime, timedelta
import logging

logger = logging.getLogger(__name__)


class SkyboxClient:
    """Client for Skybox API operations"""
    
    BASE_URL = "https://skybox.vividseats.com/services"
    
    def __init__(self, api_token: str, account_id: int, app_token: str):
        self.headers = {
            "X-Api-Token": api_token,
            "X-Account": str(account_id),
            "X-Application-Token": app_token,
            "Content-Type": "application/json"
        }
        self.session = requests.Session()
        self.session.headers.update(self.headers)
    
    def get_unreconciled_purchases(
        self, 
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None
    ) -> List[Dict]:
        """
        Fetch all purchases with outstanding balance > 0
        
        Args:
            start_date: Filter purchases created after this date
            end_date: Filter purchases created before this date
        """
        # Default to last 30 days if not specified
        if not end_date:
            end_date = datetime.now()
        if not start_date:
            start_date = end_date - timedelta(days=30)
        
        params = {
            "minOutstandingBalance": 0.01,  # Has unpaid balance
            "createdDateFrom": start_date.strftime("%Y-%m-%d"),
            "createdDateTo": end_date.strftime("%Y-%m-%d"),
            "pageSize": 20000  # Max allowed
        }
        
        url = f"{self.BASE_URL}/purchases"
        
        try:
            response = self.session.get(url, params=params)
            response.raise_for_status()
            data = response.json()
            
            purchases = data.get('rows', [])
            logger.info(f"Retrieved {len(purchases)} unreconciled purchases")
            
            # Check for pagination 
            if len(purchases) == 20000:
                logger.warning("Hit pagination limit - may need multiple requests")
            
            return purchases
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to fetch Skybox purchases: {e}")
            raise
    
    def get_purchase_by_id(self, purchase_id: int) -> Optional[Dict]:
        """Fetch detailed purchase information"""
        url = f"{self.BASE_URL}/purchases/{purchase_id}"
        
        try:
            response = self.session.get(url)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to fetch purchase {purchase_id}: {e}")
            return None
    
    def update_purchase_payment(
        self,
        purchase_id: int,
        credit_card_group_id: int,
        credit_card_id: int,
        mark_as_paid: bool = True
    ) -> bool:
        """
        Update purchase with payment information
        
        Args:
            purchase_id: Skybox purchase ID
            credit_card_group_id: CC group ID from mapping
            credit_card_id: CC ID from mapping
            mark_as_paid: Whether to mark as PAID
        """
        url = f"{self.BASE_URL}/purchases/{purchase_id}"
        
        payload = {
            "creditCardGroupId": credit_card_group_id,
            "creditCardId": credit_card_id,
        }
        
        if mark_as_paid:
            payload["paymentStatus"] = "PAID"
            payload["paymentMethod"] = "CREDITCARD"
        
        try:
            response = self.session.put(url, json=payload)
            response.raise_for_status()
            logger.info(f"Updated purchase {purchase_id} - paid status: {mark_as_paid}")
            return True
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to update purchase {purchase_id}: {e}")
            return False

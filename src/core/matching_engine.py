"""
Core Matching Engine for Transaction Reconciliation
Handles multi-criteria matching with confidence scoring
"""
from typing import List, Dict, Optional, Tuple
from datetime import datetime, timedelta
from dataclasses import dataclass
import re


@dataclass
class MatchResult:
    """Result of a matching attempt"""
    skybox_id: int
    reveal_id: int
    confidence_score: float
    match_criteria: List[str]
    metadata: Dict


class MatchingEngine:
    """
    Multi-criteria transaction matching engine
    Implements waterfall matching logic with confidence scoring
    """
    
    def __init__(self, credit_card_mapping: Dict[str, Dict] = None):
        """
        Args:
            credit_card_mapping: Dict mapping account names to CC group/card IDs
        """
        self.cc_mapping = credit_card_mapping or {}
        
    def match_transactions(
        self, 
        skybox_purchase: Dict, 
        reveal_transactions: List[Dict]
    ) -> Optional[MatchResult]:
        """Find best match for a Skybox purchase from Reveal transactions"""
        purchase_meta = self._extract_skybox_metadata(skybox_purchase)
        candidates = []
        
        for reveal_txn in reveal_transactions:
            # Strategy 1: Order Number Match (Highest confidence)
            result = self._match_by_order_number(purchase_meta, reveal_txn)
            if result:
                candidates.append(result)
                continue
            
            # Strategy 2: Multi-criteria match (automated purchases)
            result = self._match_by_multiple_criteria(purchase_meta, reveal_txn)
            if result:
                candidates.append(result)
                continue
                
            # Strategy 3: Email-based match (requires email data)
            result = self._match_by_email_metadata(purchase_meta, reveal_txn)
            if result:
                candidates.append(result)
        
        if candidates:
            best_match = max(candidates, key=lambda x: x.confidence_score)
            if best_match.confidence_score >= 0.75:
                return best_match
        
        return None
    
    def _extract_skybox_metadata(self, purchase: Dict) -> Dict:
        """Extract and parse metadata from Skybox purchase"""
        meta = {
            'id': purchase['id'],
            'amount': purchase['total'],
            'created_date': datetime.fromisoformat(purchase['createdDate'].replace('Z', '+00:00')).replace(tzinfo=None),
            'is_automated': purchase['createdBy'] in ['SeatScouts', 'Lotus Tickets Sale Tracking'],
            'external_ref': purchase.get('externalRef'),
            'internal_notes': purchase.get('internalNotes', ''),
            'event_name': purchase.get('eventName', ''),
        }
        
        # Safe extraction of email and last 4
        meta['email'] = self._extract_email(meta['internal_notes'])
        meta['last_four'] = self._extract_last_four(meta['internal_notes'])
        
        return meta
    
    def _match_by_order_number(self, purchase_meta: Dict, reveal_txn: Dict) -> Optional[MatchResult]:
        order_num = purchase_meta['external_ref']
        if not order_num: return None
        
        desc = reveal_txn.get('description', '')
        ext_desc = reveal_txn.get('extended_description', '') or ''
        
        if str(order_num) in str(desc) or str(order_num) in str(ext_desc):
            reveal_amount = abs(reveal_txn['amount']) / 100
            if abs(reveal_amount - purchase_meta['amount']) < 0.01:
                return MatchResult( 
                    skybox_id=purchase_meta['id'],
                    reveal_id=reveal_txn['id'],
                    confidence_score=1.0,
                    match_criteria=['order_number', 'amount'],
                    metadata={'order_number': order_num, 'amount_diff': 0}
                )
        return None
    
    def _match_by_multiple_criteria(self, purchase_meta: Dict, reveal_txn: Dict) -> Optional[MatchResult]:
        criteria_met = []
        score = 0.0
        
        reveal_amount = abs(reveal_txn['amount']) / 100
        amount_diff = abs(reveal_amount - purchase_meta['amount'])
        if amount_diff < 0.01:
            criteria_met.append('amount_exact')
            score += 0.40
        elif amount_diff < 1.0:
            criteria_met.append('amount_close')
            score += 0.20
        else:
            return None
        
        reveal_date = datetime.fromisoformat(reveal_txn['date'])
        date_diff = abs((reveal_date - purchase_meta['created_date']).days)
        if date_diff <= 0:
            criteria_met.append('date_same_day')
            score += 0.30
        elif date_diff <= 3:
            criteria_met.append('date_within_3_days')
            score += 0.20
        
        if purchase_meta['last_four']:
            reveal_last4 = reveal_txn.get('last_four') or reveal_txn.get('sub_account', '')
            if purchase_meta['last_four'] in str(reveal_last4):
                criteria_met.append('last_four')
                score += 0.20
        
        if score >= 0.75:
            return MatchResult(
                skybox_id=purchase_meta['id'],
                reveal_id=reveal_txn['id'],
                confidence_score=score,
                match_criteria=criteria_met,
                metadata={'amount_diff': amount_diff, 'is_automated': purchase_meta['is_automated']}
            )
        return None
    
    def _match_by_email_metadata(self, purchase_meta: Dict, reveal_txn: Dict) -> Optional[MatchResult]:
        return None # Placeholder for Gmail API logic
    
    #  Helper Methods with Safety Guards 
    
    def _extract_email(self, text: str) -> Optional[str]:
        """Extract email from internal notes - Added safety for NoneType"""
        if not text or not isinstance(text, str):
            return None
        email_pattern = r'[\w\.-]+@[\w\.-]+\.\w+'
        match = re.search(email_pattern, text)
        return match.group(0) if match else None
    
    def _extract_last_four(self, text: str) -> Optional[str]:
        """Extract last 4 digits from notes - Added safety for NoneType"""
        if not text or not isinstance(text, str):
            return None
        patterns = [
            r'CC#\s*(\d{4})',
            r'CC:\s*(\d{4})',
            r'CC\s+(\d{4})',
            r'#(\d{4})',
            r'cc\s?(\d{4})'
        ]
        for pattern in patterns:
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                return match.group(1)
        return None
    
    def _extract_keywords(self, event_name: str) -> List[str]:
        if not event_name: return []
        stop_words = {'at', 'vs', 'parking', 'the', 'a', 'an', 'and', 'or'}
        words = str(event_name).lower().split()
        return [w for w in words if w not in stop_words and len(w) > 2][:3]


# Batch Processing 

def batch_match_transactions(
    skybox_purchases: List[Dict],
    reveal_transactions: List[Dict],
    credit_card_mapping: Dict = None
) -> Tuple[List[MatchResult], List[Dict]]:
    engine = MatchingEngine(credit_card_mapping)
    matched = []
    unmatched = []
    
    sorted_purchases = sorted(skybox_purchases, key=lambda x: x['createdDate'])
    
    for purchase in sorted_purchases:
        candidates = _filter_candidate_transactions(purchase, reveal_transactions)
        match_result = engine.match_transactions(purchase, candidates)
        if match_result:
            matched.append(match_result)
        else:
            unmatched.append(purchase)
    
    return matched, unmatched


def _filter_candidate_transactions(purchase: Dict, all_transactions: List[Dict]) -> List[Dict]:
    amount = purchase['total']
    # Force timezone removal to allow subtraction
    created = datetime.fromisoformat(purchase['createdDate'].replace('Z', '+00:00')).replace(tzinfo=None)
    
    candidates = []
    for txn in all_transactions:
        txn_amount = abs(txn['amount']) / 100
        txn_date = datetime.fromisoformat(txn['date'])
        
        amount_match = abs(txn_amount - amount) < 5.0
        date_diff = abs((txn_date - created).days)
        date_match = date_diff <= 7
        
        if amount_match and date_match:
            candidates.append(txn)
    
    return candidates
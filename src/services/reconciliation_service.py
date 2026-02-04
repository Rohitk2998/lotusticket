# Main workflow orchestrator
"""
Reconciliation Service - Main orchestration logic
Coordinates matching engine + API clients
"""
from typing import List, Dict, Tuple
from datetime import datetime, timedelta
from dataclasses import dataclass, asdict
import logging
import json

logger = logging.getLogger(__name__)


@dataclass
class ReconciliationResult:
    """Result of a reconciliation run"""
    timestamp: str
    total_purchases: int
    total_transactions: int
    matches_found: int
    matches_updated: int
    unmatched_purchases: int
    errors: List[str]
    matches: List[Dict]
    
    def to_dict(self):
        return asdict(self)


class ReconciliationService:
    """
    Main service for automated transaction reconciliation
    """
    
    def __init__(
        self,
        skybox_client,
        reveal_client,
        matching_engine,
        credit_card_mapping: Dict,
        dry_run: bool = False
    ):
        """
        Args:
            skybox_client: SkyboxClient instance
            reveal_client: RevealMarketsClient instance
            matching_engine: MatchingEngine instance
            credit_card_mapping: CC mapping dict
            dry_run: If True, don't update systems (testing mode)
        """
        self.skybox = skybox_client
        self.reveal = reveal_client
        self.engine = matching_engine
        self.cc_mapping = credit_card_mapping
        self.dry_run = dry_run
    
    def reconcile_transactions(
        self,
        start_date: datetime = None,
        end_date: datetime = None
    ) -> ReconciliationResult:
        """
        Main reconciliation workflow
        
        Steps:
        1. Fetch unreconciled purchases from Skybox
        2. Fetch unmatched transactions from Reveal Markets
        3. Run matching engine
        4. Update both systems
        5. Generate report
        """
        logger.info("=" * 60)
        logger.info("Starting Reconciliation Run")
        logger.info(f"Mode: {'DRY RUN' if self.dry_run else 'LIVE'}")
        logger.info("=" * 60)
        
        errors = []
        matches = []
        
        try:
            import ipdb; ipdb.set_trace()
            # Step 1: Fetch unreconciled purchases
            logger.info("Fetching unreconciled purchases from Skybox...")
            purchases = self.skybox.get_unreconciled_purchases(start_date, end_date)
            logger.info(f"✓ Found {len(purchases)} unreconciled purchases")
            
            # Step 2: Fetch unmatched transactions
            logger.info("Fetching unmatched transactions from Reveal Markets...")
            transactions = self.reveal.get_unmatched_transactions(start_date, end_date)
            logger.info(f"✓ Found {len(transactions)} unmatched transactions")
            
            # Step 3: Run matching
            logger.info("Running matching engine...")
            matched, unmatched = self._batch_match(purchases, transactions)
            logger.info(f"✓ Matched {len(matched)} purchases")
            logger.info(f"  Unmatched: {len(unmatched)} purchases")
            
            # Step 4: Update systems
            updated_count = 0
            # if not self.dry_run and matched:
            #     logger.info("Updating systems with matched transactions...")
            #     updated_count = self._update_systems(matched)
            #     logger.info(f"✓ Successfully updated {updated_count} matches")
            # elif self.dry_run:
            #     logger.info("DRY RUN - Skipping system updates")
            
            # Convert matches to serializable format
            matches = [
                {
                    'skybox_id': m.skybox_id,
                    'reveal_id': m.reveal_id,
                    'confidence': m.confidence_score,
                    'criteria': m.match_criteria,
                    'metadata': m.metadata
                }
                for m in matched
            ]
            
            # Step 5: Generate result
            result = ReconciliationResult(
                timestamp=datetime.now().isoformat(),
                total_purchases=len(purchases),
                total_transactions=len(transactions),
                matches_found=len(matched),
                matches_updated=updated_count,
                unmatched_purchases=len(unmatched),
                errors=errors,
                matches=matches
            )
            
            logger.info("=" * 60)
            logger.info("Reconciliation Complete!")
            logger.info(f"Matched: {result.matches_found}/{result.total_purchases}")
            logger.info(f"Success Rate: {(result.matches_found/result.total_purchases*100):.1f}%")
            logger.info("=" * 60)
            
            return result
            
        except Exception as e:
            logger.error(f"Reconciliation failed: {e}", exc_info=True)
            errors.append(str(e))
            
            return ReconciliationResult(
                timestamp=datetime.now().isoformat(),
                total_purchases=len(purchases) if 'purchases' in locals() else 0,
                total_transactions=len(transactions) if 'transactions' in locals() else 0,
                matches_found=len(matches),
                matches_updated=0,
                unmatched_purchases=0,
                errors=errors,
                matches=matches
            )
    
    def _batch_match(
        self, 
        purchases: List[Dict], 
        transactions: List[Dict]
    ) -> Tuple[List, List]:
        """
        Run matching engine on all purchases
        
        Returns:
            (matched_results, unmatched_purchases)
        """
        from core.matching_engine import _filter_candidate_transactions
        
        matched = []
        unmatched = []
        
        # Sort purchases (oldest first)
        sorted_purchases = sorted(purchases, key=lambda x: x['createdDate'])
        
        for purchase in sorted_purchases:
            # Filter to likely candidates
            candidates = _filter_candidate_transactions(purchase, transactions)
            
            # Try to match
            match_result = self.engine.match_transactions(purchase, candidates)
            
            if match_result:
                matched.append(match_result)
                logger.debug(f"Matched purchase {purchase['id']} (confidence: {match_result.confidence_score:.2f})")
            else:
                unmatched.append(purchase)
                logger.debug(f"No match for purchase {purchase['id']}")
        
        return matched, unmatched
    
    def _update_systems(self, matches: List) -> int:
        """
        Update both Skybox and Reveal Markets with matches
        
        Returns:
            Number of successfully updated matches
        """
        success_count = 0
        
        for match in matches:
            try:
                # Get CC info from mapping
                # We need to find which account this transaction belongs to
                # This requires looking up the reveal transaction again
                reveal_txn = self._get_reveal_transaction(match.reveal_id)
                if not reveal_txn:
                    logger.error(f"Could not find Reveal transaction {match.reveal_id}")
                    continue
                
                account_name = reveal_txn.get('account__name')
                cc_info = self.cc_mapping.get(account_name)
                
                if not cc_info:
                    logger.error(f"No CC mapping for account: {account_name}")
                    continue
                
                # Update Skybox
                skybox_success = self.skybox.update_purchase_payment(
                    purchase_id=match.skybox_id,
                    credit_card_group_id=cc_info['credit_card_group_id'],
                    credit_card_id=cc_info['credit_card_id'],
                    mark_as_paid=True
                )
                
                # Update Reveal Markets
                reveal_success = self.reveal.mark_transaction_matched(
                    transaction_id=match.reveal_id,
                    skybox_purchase_id=match.skybox_id
                )
                
                if skybox_success and reveal_success:
                    success_count += 1
                    logger.info(f"✓ Updated match: Purchase {match.skybox_id} ↔ Transaction {match.reveal_id}")
                
            except Exception as e:
                logger.error(f"Failed to update match {match.skybox_id}: {e}")
        
        return success_count
    
    def _get_reveal_transaction(self, transaction_id: int) -> Dict:
        """Helper to get transaction details - implementation depends on API"""
        # This would need to fetch from stored transactions or make another API call
        pass


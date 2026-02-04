# Daily reports

# === Reporting Service ===
from typing import List, Dict, Tuple
from datetime import datetime, timedelta
from dataclasses import dataclass, asdict
from models.schemas import ReconciliationResult
import logging
import json

logger = logging.getLogger(__name__)

class ReportingService:
    """Generate reconciliation reports"""
    
    def __init__(self, output_dir: str = "reports"):
        self.output_dir = output_dir
        import os
        os.makedirs(output_dir, exist_ok=True)
    
    def generate_daily_report(self, result: ReconciliationResult) -> str:
        """
        Generate daily reconciliation report
        
        Returns:
            Path to generated report file
        """
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"reconciliation_report_{timestamp}.json"
        filepath = f"{self.output_dir}/{filename}"
        
        # Write JSON report
        with open(filepath, 'w') as f:
            json.dump(result.to_dict(), f, indent=2)
        
        logger.info(f"Report saved to: {filepath}")
        
        # Generate summary text
        summary = self._generate_summary(result)
        summary_file = filepath.replace('.json', '_summary.txt')
        with open(summary_file, 'w', encoding='utf-8') as f:
            f.write(summary)
        
        return filepath
    
    def _generate_summary(self, result: ReconciliationResult) -> str:
        """Generate human-readable summary"""
        success_rate = (result.matches_found / result.total_purchases * 100) if result.total_purchases > 0 else 0
        
        summary = f"""TRANSACTION RECONCILIATION REPORT                 

Timestamp: {result.timestamp}

SUMMARY
-------
Total Purchases:        {result.total_purchases}
Total Transactions:     {result.total_transactions}
Matches Found:          {result.matches_found}
Successfully Updated:   {result.matches_updated}
Unmatched Purchases:    {result.unmatched_purchases}

Success Rate:           {success_rate:.1f}%

MATCHED TRANSACTIONS
-------------------
"""
        
        for match in result.matches[:10]:  
            summary += f"\n- Purchase {match['skybox_id']} ↔ Transaction {match['reveal_id']}"
            summary += f"\n  Confidence: {match['confidence']:.1%}"
            summary += f"\n  Criteria: {', '.join(match['criteria'])}\n"
        
        if len(result.matches) > 10:
            summary += f"\n... and {len(result.matches) - 10} more matches\n"
        
        if result.errors:
            summary += "\nERRORS\n------\n"
            for error in result.errors:
                summary += f"- {error}\n"
        
        return summary
    
    def send_email_report(self, result: ReconciliationResult, recipients: List[str]):
        """
        Send report via email
        TODO: Implement email sending
        """
        pass


# Exception Handling 

class ReconciliationException(Exception):
    """Base exception for reconciliation errors"""
    pass


class MatchingException(ReconciliationException):
    """Error during matching process"""
    pass


class UpdateException(ReconciliationException):
    """Error updating systems"""
    pass
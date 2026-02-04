# Pydantic models
# src/models/schemas.py
from dataclasses import dataclass, asdict
from typing import List, Dict

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
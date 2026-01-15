# FastAPI endpoints
"""
FastAPI Service for Transaction Reconciliation System
Production-ready API with monitoring and health checks
"""


from fastapi import FastAPI, HTTPException, BackgroundTasks
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field
from typing import Optional, List
from datetime import datetime, timedelta
import logging
import os

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

import sys
from pathlib import Path
# Adds the 'src' directory to the python path
sys.path.append(str(Path(__file__).parent.parent / "src"))
# Import our services (assuming they're in the same package)
# from matching_engine import MatchingEngine
# from api_clients import SkyboxClient, RevealMarketsClient
# from reconciliation_service import ReconciliationService, ReportingService

# FastAPI app
app = FastAPI(
    title="Transaction Reconciliation API",
    description="Automated reconciliation system for Skybox purchases and credit card transactions",
    version="1.0.0"
)


# === Request/Response Models ===

class ReconcileRequest(BaseModel):
    """Request to trigger reconciliation"""
    start_date: Optional[str] = Field(None, description="Start date (YYYY-MM-DD)")
    end_date: Optional[str] = Field(None, description="End date (YYYY-MM-DD)")
    dry_run: bool = Field(False, description="Test mode - don't update systems")


class ReconcileResponse(BaseModel):
    """Reconciliation result"""
    job_id: str
    status: str
    message: str
    result: Optional[dict] = None


class HealthResponse(BaseModel):
    """Health check response"""
    status: str
    timestamp: str
    services: dict


class StatsResponse(BaseModel):
    """System statistics"""
    total_reconciliations: int
    success_rate: float
    last_run: Optional[str]
    avg_matches_per_run: float


# === Configuration ===

class Config:
    """Application configuration"""
    
    # Skybox API
    SKYBOX_API_TOKEN = os.getenv("SKYBOX_API_TOKEN", "8293a10f-6546-457c-8644-2b58a753617a")
    SKYBOX_ACCOUNT_ID = int(os.getenv("SKYBOX_ACCOUNT_ID", "5052"))
    SKYBOX_APP_TOKEN = os.getenv("SKYBOX_APP_TOKEN", "2140c962-2c86-4826-899a-20e6ae8fad31")
    
    # Reveal Markets API
    REVEAL_API_TOKEN = os.getenv("REVEAL_API_TOKEN", "8915365073157a5e061cc9174ef262419b1220e9")
    
    # CC Mapping
    CC_MAPPING_FILE = os.getenv("CC_MAPPING_FILE", "config/credit_card_mapping.json")
    
    # Reports
    REPORT_OUTPUT_DIR = os.getenv("REPORT_OUTPUT_DIR", "reports")


config = Config()


# === Dependency Injection ===

def get_services():
    """
    Initialize and return service instances with corrected syntax
    """
    from integrations.skybox_client import SkyboxClient
    from integrations.reveal_client import RevealMarketsClient
    from integrations.reveal_client import load_credit_card_mapping # Assuming you moved this to utils.py
    from core.matching_engine import MatchingEngine
    from services.reconciliation_service import ReconciliationService
    from services.reporting_service import ReportingService
    
    # Initialize clients
    skybox_client = SkyboxClient(
        api_token=config.SKYBOX_API_TOKEN,
        account_id=config.SKYBOX_ACCOUNT_ID,
        app_token=config.SKYBOX_APP_TOKEN
    )
    
    reveal_client = RevealMarketsClient(
        api_token=config.REVEAL_API_TOKEN
    )
    
    # Load CC mapping
    cc_mapping = load_credit_card_mapping(config.CC_MAPPING_FILE)
    
    # Initialize matching engine
    matching_engine = MatchingEngine(cc_mapping)
    
    # Initialize reconciliation service
    recon_service = ReconciliationService(
        skybox_client=skybox_client,
        reveal_client=reveal_client,
        matching_engine=matching_engine,
        credit_card_mapping=cc_mapping,
        dry_run=False
    )
    
    reporting_service = ReportingService(output_dir=config.REPORT_OUTPUT_DIR)
    
    return recon_service, reporting_service


# API Endpoints

@app.get("/", tags=["Info"])
async def root():
    """API information"""
    return {
        "name": "Transaction Reconciliation API",
        "version": "1.0.0",
        "status": "operational",
        "endpoints": {
            "health": "/health",
            "reconcile": "/api/v1/reconcile",
            "stats": "/api/v1/stats"
        }
    }


@app.get("/health", response_model=HealthResponse, tags=["Health"])
async def health_check():
    """
    Health check endpoint
    Tests connectivity to Skybox and Reveal Markets APIs
    """
    from integrations.reveal_client import test_skybox_connection, test_reveal_connection
    
    services = {
        "skybox": "unknown",
        "reveal_markets": "unknown",
        "database": "ok"  #  don't have a DB 
    }
    
    # Test Skybox
    try:
        if test_skybox_connection(
            config.SKYBOX_API_TOKEN,
            config.SKYBOX_ACCOUNT_ID,
            config.SKYBOX_APP_TOKEN
        ):
            services["skybox"] = "ok"
        else:
            services["skybox"] = "error"
    except Exception as e:
        services["skybox"] = f"error: {str(e)}"
    
    # Test Reveal Markets
    try:
        if test_reveal_connection(config.REVEAL_API_TOKEN):
            services["reveal_markets"] = "ok"
        else:
            services["reveal_markets"] = "error"
    except Exception as e:
        services["reveal_markets"] = f"error: {str(e)}"
    
    # Overall status
    status = "healthy" if all(s == "ok" for s in services.values()) else "degraded"
    
    return HealthResponse(
        status=status,
        timestamp=datetime.now().isoformat(),
        services=services
    )


@app.post("/api/v1/reconcile", response_model=ReconcileResponse, tags=["Reconciliation"])
async def reconcile_transactions(
    request: ReconcileRequest,
    background_tasks: BackgroundTasks
):
    """
    Trigger transaction reconciliation
    
    This endpoint starts a reconciliation job that:
    1. Fetches unreconciled purchases from Skybox
    2. Fetches unmatched transactions from Reveal Markets
    3. Runs the matching engine
    4. Updates both systems (unless dry_run=true)
    5. Generates a report
    """
    import uuid
    
    job_id = str(uuid.uuid4())
    
    # Parse dates
    start_date = None
    end_date = None
    
    if request.start_date:
        try:
            start_date = datetime.fromisoformat(request.start_date)
        except ValueError:
            raise HTTPException(400, "Invalid start_date format. Use YYYY-MM-DD")
    
    if request.end_date:
        try:
            end_date = datetime.fromisoformat(request.end_date)
        except ValueError:
            raise HTTPException(400, "Invalid end_date format. Use YYYY-MM-DD")
    
    # Default to last 30 days
    if not end_date:
        end_date = datetime.now()
    if not start_date:
        start_date = end_date - timedelta(days=30)
    
    logger.info(f"Starting reconciliation job {job_id}")
    logger.info(f"Date range: {start_date.date()} to {end_date.date()}")
    logger.info(f"Dry run: {request.dry_run}")
    
    # Run reconciliation in background
    try:
        recon_service, reporting_service = get_services()
        
        # Override dry_run setting
        recon_service.dry_run = request.dry_run
        
        # Run reconciliation
        result = recon_service.reconcile_transactions(start_date, end_date)
        
        # Generate report
        report_path = reporting_service.generate_daily_report(result)
        
        return ReconcileResponse(
            job_id=job_id,
            status="completed",
            message=f"Reconciliation complete. Matched {result.matches_found}/{result.total_purchases} purchases.",
            result=result.to_dict()
        )
        
    except Exception as e:
        logger.error(f"Reconciliation job {job_id} failed: {e}", exc_info=True)
        raise HTTPException(500, f"Reconciliation failed: {str(e)}")


@app.get("/api/v1/stats", response_model=StatsResponse, tags=["Statistics"])
async def get_stats():
    """
    Get reconciliation statistics
    TODO: Implement stats tracking (requires database)
    """
    return StatsResponse(
        total_reconciliations=0,
        success_rate=0.0,
        last_run=None,
        avg_matches_per_run=0.0
    )


@app.post("/api/v1/test-match", tags=["Testing"])
async def test_single_match(skybox_id: int, reveal_id: int):
    """
    Test matching logic for a single purchase/transaction pair
    Useful for debugging
    """
    try:
        recon_service, _ = get_services()
        
        # Fetch specific purchase and transaction
        purchase = recon_service.skybox.get_purchase_by_id(skybox_id)
        if not purchase:
            raise HTTPException(404, f"Purchase {skybox_id} not found")
        
        # We'd need to implement a get_transaction_by_id method
        # For now, return a placeholder
        return {
            "message": "Test endpoint - implementation pending",
            "purchase": purchase
        }
        
    except Exception as e:
        raise HTTPException(500, str(e))


#  Event Handlers

@app.on_event("startup")
async def startup_event():
    """Initialize services on startup"""
    logger.info("=" * 60)
    logger.info("Transaction Reconciliation API Starting...")
    logger.info("=" * 60)
    
    # Test connections
    logger.info("Testing API connections...")
    
    try:
        from integrations.reveal_client import test_skybox_connection, test_reveal_connection
        
        skybox_ok = test_skybox_connection(
            config.SKYBOX_API_TOKEN,
            config.SKYBOX_ACCOUNT_ID,
            config.SKYBOX_APP_TOKEN
        )
        
        reveal_ok = test_reveal_connection(config.REVEAL_API_TOKEN)
        
        if skybox_ok and reveal_ok:
            logger.info("✓ All API connections successful")
        else:
            logger.warning("⚠ Some API connections failed")
            
    except Exception as e:
        logger.error(f"✗ Failed to test API connections: {e}")
    
    logger.info("API Ready!")


@app.on_event("shutdown")
async def shutdown_event():
    """Cleanup on shutdown"""
    logger.info("Shutting down API...")


# Error Handlers 

@app.exception_handler(Exception)
async def global_exception_handler(request, exc):
    """Global exception handler"""
    logger.error(f"Unhandled exception: {exc}", exc_info=True)
    return JSONResponse(
        status_code=500,
        content={
            "error": "Internal Server Error",
            "message": str(exc)
        }
    )


#  Run Server 

if __name__ == "__main__":
    import uvicorn
    
    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=8000,
        reload=True,  # Development only
        log_level="info"
    )
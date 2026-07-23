"""FastAPI application for RAIDAR."""

import logging
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from src.api.routes import router

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(
    title="RAIDAR",
    description="High-quality AI research signal without the noise",
    version="0.1.0",
)

# CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Configure for specific origins in production
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Include routes
app.include_router(router)


@app.get("/")
def root():
    """API root."""
    return {
        "name": "RAIDAR",
        "description": "AI research signal without the noise",
        "docs": "/docs",
        "health": "/api/health",
    }


@app.on_event("startup")
async def startup():
    """Initialize on startup."""
    logger.info("RAIDAR API starting up")


@app.on_event("shutdown")
async def shutdown():
    """Cleanup on shutdown."""
    logger.info("RAIDAR API shutting down")


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)

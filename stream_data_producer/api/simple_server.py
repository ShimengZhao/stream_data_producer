"""Simplified RESTful API server for single producer mode"""

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import Optional, Dict, Any
import uvicorn
import threading
import time


class RateUpdateRequest(BaseModel):
    """Request model for rate updates"""
    rate: Optional[int] = None
    interval: Optional[str] = None


class ApiResponse(BaseModel):
    """Generic API response model"""
    success: bool
    message: str
    data: Optional[Dict[str, Any]] = None


class SimpleAPIServer:
    """Simplified FastAPI-based server for single producer management"""
    
    def __init__(self, producer_manager, host: str = "127.0.0.1", port: int = 8000):
        self.producer_manager = producer_manager
        self.host = host
        self.port = port
        self.app = FastAPI(
            title="Stream Data Producer API (Single Producer)",
            description="API for monitoring and controlling single data producer",
            version="0.1.0"
        )
        self._setup_routes()
        self._setup_middleware()
    
    def _setup_middleware(self):
        """Setup CORS middleware"""
        self.app.add_middleware(
            CORSMiddleware,
            allow_origins=["*"],
            allow_credentials=True,
            allow_methods=["*"],
            allow_headers=["*"],
        )
    
    def _setup_routes(self):
        """Setup API routes"""
        
        @self.app.get("/", response_model=ApiResponse)
        async def root():
            """Root endpoint"""
            return ApiResponse(
                success=True,
                message="Stream Data Producer API (Single Producer) is running",
                data={"version": "0.1.0"}
            )
        
        @self.app.get("/status", response_model=Dict[str, Any])
        async def get_status():
            """Get status of the single producer"""
            try:
                status = self.producer_manager.get_status()
                return status
            except Exception as e:
                raise HTTPException(status_code=500, detail=str(e))
        
        @self.app.post("/rate", response_model=ApiResponse)
        async def update_rate(request: RateUpdateRequest):
            """Update rate for the single producer"""
            try:
                success = self.producer_manager.update_rate(request.rate, request.interval)
                if success:
                    return ApiResponse(
                        success=True,
                        message="Rate updated successfully",
                        data={"rate": request.rate, "interval": request.interval}
                    )
                else:
                    raise HTTPException(status_code=400, detail="Failed to update rate")
            except HTTPException:
                raise
            except Exception as e:
                raise HTTPException(status_code=500, detail=str(e))
        
        @self.app.post("/start", response_model=ApiResponse)
        async def start_producer():
            """Start the single producer"""
            try:
                if not self.producer_manager.is_running():
                    success = self.producer_manager.restart()
                    if success:
                        return ApiResponse(
                            success=True,
                            message="Producer started successfully"
                        )
                    else:
                        raise HTTPException(status_code=500, detail="Failed to restart producer")
                else:
                    return ApiResponse(
                        success=True,
                        message="Producer is already running"
                    )
            except Exception as e:
                raise HTTPException(status_code=500, detail=str(e))
        
        @self.app.post("/stop", response_model=ApiResponse)
        async def stop_producer():
            """Stop the single producer"""
            try:
                if self.producer_manager.is_running():
                    self.producer_manager.stop()
                    return ApiResponse(
                        success=True,
                        message="Producer stopped successfully"
                    )
                else:
                    return ApiResponse(
                        success=True,
                        message="Producer is already stopped"
                    )
            except Exception as e:
                raise HTTPException(status_code=500, detail=str(e))
        
        @self.app.get("/health", response_model=ApiResponse)
        async def health_check():
            """Health check endpoint"""
            return ApiResponse(
                success=True,
                message="Service is healthy",
                data={
                    "status": "healthy",
                    "timestamp": time.time()
                }
            )
    
    def start(self, background: bool = True):
        """Start the API server"""
        if background:
            # Run in background thread
            def run_server():
                import asyncio
                # Create new event loop for this thread
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)
                try:
                    uvicorn.run(
                        self.app,
                        host=self.host,
                        port=self.port,
                        log_level="info"
                    )
                finally:
                    loop.close()
            
            server_thread = threading.Thread(target=run_server, daemon=True)
            server_thread.start()
            print(f"🚀 API server started on http://{self.host}:{self.port}")
        else:
            # Run in foreground
            uvicorn.run(
                self.app,
                host=self.host,
                port=self.port,
                log_level="info"
            )
    
    def stop(self):
        """Stop the API server (placeholder)"""
        pass

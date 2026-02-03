"""
Pipeline manager for running multiple pipelines with multiprocessing.

Provides process isolation and lifecycle management for CDC pipelines.
"""

import logging
import signal
import sys
import time
from multiprocessing import Process, Event
from typing import Optional
from dataclasses import dataclass, field

from compute.core.engine import PipelineEngine
from compute.core.repository import PipelineRepository, PipelineMetadataRepository
from compute.core.database import init_connection_pool, close_connection_pool
from compute.core.exceptions import PipelineException

logger = logging.getLogger(__name__)


@dataclass
class PipelineProcess:
    """Container for pipeline process information."""
    pipeline_id: int
    pipeline_name: str
    process: Optional[Process] = None
    stop_event: Event = field(default_factory=Event)
    
    @property
    def is_alive(self) -> bool:
        """Check if process is running."""
        return self.process is not None and self.process.is_alive()


def _run_pipeline_process(pipeline_id: int, stop_event: Event) -> None:
    """
    Worker function for running a pipeline in a separate process.
    
    This function is called in a child process.
    
    Args:
        pipeline_id: Pipeline ID to run
        stop_event: Event to signal stop
    """
    # Initialize database connection pool for this process
    init_connection_pool(min_conn=1, max_conn=5)
    
    engine = None
    try:
        engine = PipelineEngine(pipeline_id)
        engine.initialize()
        
        # Run until stop event is set
        # Note: The Debezium engine.run() is blocking
        # We'll need to implement proper shutdown handling
        engine.run()
        
    except Exception as e:
        logger.error(f"Pipeline {pipeline_id} crashed: {e}")
        PipelineMetadataRepository.upsert(pipeline_id, "ERROR", str(e))
    finally:
        if engine:
            engine.stop()
        close_connection_pool()


class PipelineManager:
    """
    Manager for running multiple CDC pipelines with process isolation.
    
    Each pipeline runs in its own process for:
    - Fault isolation (one pipeline crash doesn't affect others)
    - Better resource utilization
    - Independent memory management
    """
    
    def __init__(self):
        """Initialize pipeline manager."""
        self._processes: dict[int, PipelineProcess] = {}
        self._shutdown_event = Event()
        self._logger = logging.getLogger(__name__)
        
        # Register signal handlers
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)
    
    def _signal_handler(self, signum, frame):
        """Handle shutdown signals."""
        self._logger.info(f"Received signal {signum}, initiating shutdown")
        self.shutdown()
    
    def start_pipeline(self, pipeline_id: int) -> bool:
        """
        Start a pipeline in a new process.
        
        Args:
            pipeline_id: Pipeline ID to start
            
        Returns:
            True if started successfully
        """
        if pipeline_id in self._processes and self._processes[pipeline_id].is_alive:
            self._logger.warning(f"Pipeline {pipeline_id} is already running")
            return False
        
        # Get pipeline info
        pipeline = PipelineRepository.get_by_id(pipeline_id)
        if pipeline is None:
            self._logger.error(f"Pipeline {pipeline_id} not found")
            return False
        
        # Create process wrapper
        stop_event = Event()
        proc = Process(
            target=_run_pipeline_process,
            args=(pipeline_id, stop_event),
            name=f"Pipeline_{pipeline.name}",
            daemon=True,
        )
        
        pipeline_proc = PipelineProcess(
            pipeline_id=pipeline_id,
            pipeline_name=pipeline.name,
            process=proc,
            stop_event=stop_event,
        )
        
        self._processes[pipeline_id] = pipeline_proc
        
        # Start process
        proc.start()
        self._logger.info(f"Started pipeline {pipeline.name} (PID: {proc.pid})")
        
        # Update pipeline status
        PipelineRepository.update_status(pipeline_id, "START")
        
        return True
    
    def stop_pipeline(self, pipeline_id: int, timeout: float = 10.0) -> bool:
        """
        Stop a running pipeline.
        
        Args:
            pipeline_id: Pipeline ID to stop
            timeout: Seconds to wait for graceful shutdown
            
        Returns:
            True if stopped successfully
        """
        if pipeline_id not in self._processes:
            self._logger.warning(f"Pipeline {pipeline_id} not tracked")
            return False
        
        pipeline_proc = self._processes[pipeline_id]
        
        if not pipeline_proc.is_alive:
            self._logger.info(f"Pipeline {pipeline_id} is not running")
            del self._processes[pipeline_id]
            return True
        
        # Signal stop
        pipeline_proc.stop_event.set()
        
        # Wait for graceful shutdown
        pipeline_proc.process.join(timeout=timeout)
        
        if pipeline_proc.is_alive:
            # Force terminate
            self._logger.warning(f"Force terminating pipeline {pipeline_id}")
            pipeline_proc.process.terminate()
            pipeline_proc.process.join(timeout=5.0)
        
        # Update status
        PipelineRepository.update_status(pipeline_id, "PAUSE")
        PipelineMetadataRepository.upsert(pipeline_id, "PAUSED")
        
        del self._processes[pipeline_id]
        self._logger.info(f"Stopped pipeline {pipeline_id}")
        
        return True
    
    def restart_pipeline(self, pipeline_id: int) -> bool:
        """
        Restart a pipeline (stop if running, then start).
        
        Args:
            pipeline_id: Pipeline ID to restart
            
        Returns:
            True if restarted successfully
        """
        self.stop_pipeline(pipeline_id)
        time.sleep(1)  # Brief pause between stop and start
        return self.start_pipeline(pipeline_id)
    
    def start_all_active(self) -> int:
        """
        Start all pipelines with START status.
        
        Returns:
            Number of pipelines started
        """
        active_pipelines = PipelineRepository.get_active_pipelines()
        started = 0
        
        for pipeline in active_pipelines:
            if self.start_pipeline(pipeline.id):
                started += 1
        
        self._logger.info(f"Started {started} active pipelines")
        return started
    
    def get_status(self) -> dict[int, dict]:
        """
        Get status of all tracked pipelines.
        
        Returns:
            Dict mapping pipeline_id to status info
        """
        status = {}
        for pipeline_id, proc in self._processes.items():
            status[pipeline_id] = {
                "pipeline_name": proc.pipeline_name,
                "is_alive": proc.is_alive,
                "pid": proc.process.pid if proc.process else None,
            }
        return status
    
    def monitor(self, check_interval: float = 30.0) -> None:
        """
        Run monitoring loop to restart crashed pipelines.
        
        Args:
            check_interval: Seconds between checks
        """
        self._logger.info("Starting pipeline monitor")
        
        while not self._shutdown_event.is_set():
            # Check each pipeline
            for pipeline_id, proc in list(self._processes.items()):
                if not proc.is_alive:
                    self._logger.warning(
                        f"Pipeline {proc.pipeline_name} crashed, restarting"
                    )
                    self.start_pipeline(pipeline_id)
            
            # Wait before next check
            self._shutdown_event.wait(check_interval)
        
        self._logger.info("Pipeline monitor stopped")
    
    def shutdown(self, timeout: float = 30.0) -> None:
        """
        Gracefully shutdown all pipelines.
        
        Args:
            timeout: Seconds to wait for all pipelines to stop
        """
        self._logger.info("Shutting down pipeline manager")
        self._shutdown_event.set()
        
        # Stop all pipelines
        per_pipeline_timeout = timeout / max(len(self._processes), 1)
        
        for pipeline_id in list(self._processes.keys()):
            self.stop_pipeline(pipeline_id, timeout=per_pipeline_timeout)
        
        self._logger.info("Pipeline manager shutdown complete")
    
    def run(self) -> None:
        """
        Main entry point: start all active pipelines and monitor.
        """
        try:
            self.start_all_active()
            self.monitor()
        except KeyboardInterrupt:
            self._logger.info("Received keyboard interrupt")
        finally:
            self.shutdown()


def main():
    """Main entry point for running the pipeline manager."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )
    
    # Initialize database
    init_connection_pool()
    
    try:
        manager = PipelineManager()
        manager.run()
    finally:
        close_connection_pool()


if __name__ == "__main__":
    main()

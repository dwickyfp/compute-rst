"""
Pipeline manager for running multiple pipelines with multiprocessing.

Provides process isolation and lifecycle management for CDC pipelines.
"""

import logging
import signal
import sys
import time
from multiprocessing import Process, Event
from multiprocessing.synchronize import Event as EventClass
from typing import Optional
from dataclasses import dataclass, field
from datetime import datetime

from compute.core.engine import PipelineEngine
from compute.core.repository import PipelineRepository, PipelineMetadataRepository
from compute.core.database import init_connection_pool, close_connection_pool

logger = logging.getLogger(__name__)


@dataclass
class PipelineProcess:
    """Container for pipeline process information."""

    pipeline_id: int
    pipeline_name: str
    process: Optional[Process] = None
    stop_event: EventClass = field(default_factory=Event)
    last_updated_at: datetime = field(default_factory=datetime.now)

    @property
    def is_alive(self) -> bool:
        """Check if process is running."""
        return self.process is not None and self.process.is_alive()


def _run_pipeline_process(pipeline_id: int, stop_event: EventClass) -> None:
    """
    Worker function for running a pipeline in a separate process.

    This function is called in a child process.

    Args:
        pipeline_id: Pipeline ID to run
        stop_event: Event to signal stop
    """
    # Configure logging for subprocess - CRITICAL: parent logging is not inherited
    import sys
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        handlers=[logging.StreamHandler(sys.stdout)],
        force=True,  # Override any existing config
    )
    # Reduce noise from some libraries
    logging.getLogger("jpype").setLevel(logging.WARNING)
    logging.getLogger("urllib3").setLevel(logging.WARNING)
    logging.getLogger("httpx").setLevel(logging.WARNING)
    
    subprocess_logger = logging.getLogger(f"Pipeline_{pipeline_id}")
    subprocess_logger.info(f"Subprocess started for pipeline {pipeline_id}")
    
    # Initialize database connection pool for this process
    init_connection_pool(min_conn=1, max_conn=5)

    engine = None
    try:
        engine = PipelineEngine(pipeline_id)
        # engine.initialize() # Moved inside run or handled by engine
        # Verify engine initialization

        # Run until stop event is set
        # We need to run the engine in a non-blocking way or handle stop signals
        # Since Debezium engine blocks, we rely on the process termination for now
        # OR if PipelineEngine supports a stop check.
        # Assuming PipelineEngine.run() is blocking but we can stop it via terminate

        engine.initialize()
        engine.run()

    except Exception as e:
        logger.error(f"Pipeline {pipeline_id} crashed: {e}")
        PipelineMetadataRepository.upsert(pipeline_id, "ERROR", str(e))
    finally:
        if engine:
            try:
                engine.stop()
            except:
                pass
        close_connection_pool()


class PipelineManager:
    """
    Manager for running multiple CDC pipelines with process isolation.

    Each pipeline runs in its own process for:
    - Fault isolation (one pipeline crash doesn't affect others)
    - Better resource utilization
    - Independent memory management

    Real-time capabilities:
    - Polls database for status changes (START/PAUSE)
    - Detects configuration updates and restarts pipelines
    - Handles pipeline deletions
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
        signal_name = "SIGINT" if signum == signal.SIGINT else "SIGTERM"
        self._logger.info(f"\n{'='*60}")
        self._logger.info(f"Received {signal_name} - Initiating graceful shutdown...")
        self._logger.info(f"{'='*60}")
        self._shutdown_event.set()

    def start_pipeline(
        self, pipeline_id: int, updated_at: Optional[datetime] = None
    ) -> bool:
        """
        Start a pipeline in a new process.

        Args:
            pipeline_id: Pipeline ID to start
            updated_at: Timestamp of pipeline configuration

        Returns:
            True if started successfully
        """
        if pipeline_id in self._processes:
            proc_info = self._processes[pipeline_id]
            if proc_info.is_alive:
                self._logger.warning(f"Pipeline {pipeline_id} is already running")
                return False
            else:
                # Cleanup dead process info before restarting
                self._cleanup_process(pipeline_id)

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
            last_updated_at=updated_at or datetime.now(),
        )

        self._processes[pipeline_id] = pipeline_proc

        # Start process
        proc.start()
        self._logger.info(f"Started pipeline {pipeline.name} (PID: {proc.pid})")

        # Update pipeline status in DB to ensure it matches specific running state if needed
        # But usually we trust the START status from DB.
        # We might want to update metadata status to RUNNING
        PipelineMetadataRepository.upsert(pipeline_id, "RUNNING")

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
            return True

        pipeline_proc = self._processes[pipeline_id]

        if not pipeline_proc.is_alive:
            self._logger.info(f"Pipeline {pipeline_id} is not running")
            self._cleanup_process(pipeline_id)
            return True

        self._logger.info(f"Stopping pipeline {pipeline_proc.pipeline_name}...")

        # Signal stop
        pipeline_proc.stop_event.set()

        # Wait for graceful shutdown
        pipeline_proc.process.join(timeout=timeout)

        if pipeline_proc.is_alive:
            # Force terminate
            self._logger.warning(f"Force terminating pipeline {pipeline_id}")
            pipeline_proc.process.terminate()
            pipeline_proc.process.join(timeout=5.0)

        # Update metadata status
        PipelineMetadataRepository.upsert(pipeline_id, "PAUSED")

        self._cleanup_process(pipeline_id)
        self._logger.info(f"Stopped pipeline {pipeline_id}")

        return True

    def _cleanup_process(self, pipeline_id: int) -> None:
        """Remove pipeline from tracking."""
        if pipeline_id in self._processes:
            del self._processes[pipeline_id]

    def restart_pipeline(
        self, pipeline_id: int, updated_at: Optional[datetime] = None
    ) -> bool:
        """
        Restart a pipeline (stop if running, then start).

        Args:
            pipeline_id: Pipeline ID to restart
            updated_at: New configuration timestamp

        Returns:
            True if restarted successfully
        """
        self.stop_pipeline(pipeline_id)
        time.sleep(1)  # Brief pause between stop and start
        return self.start_pipeline(pipeline_id, updated_at)

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
                "last_updated_at": (
                    proc.last_updated_at.isoformat() if proc.last_updated_at else None
                ),
            }
        return status

    def _sync_pipelines_state(self) -> None:
        """
        Sync running pipelines with database state.

        Handles:
        1. Starting new pipelines (START)
        2. Stopping paused pipelines (PAUSE)
        3. Restarting updated pipelines (updated_at changed)
        4. Removing deleted pipelines
        """
        try:
            # Get all pipelines from DB
            db_pipelines = PipelineRepository.get_all()
            db_pipeline_map = {p.id: p for p in db_pipelines}

            # Check for pipelines to STOP or DELETE
            # Use list() to avoid runtime error during dictionary modification
            for pipeline_id, proc in list(self._processes.items()):
                # Case 1: Pipeline deleted from DB
                if pipeline_id not in db_pipeline_map:
                    self._logger.warning(
                        f"Pipeline {proc.pipeline_name} (ID: {pipeline_id}) deleted from DB. Stopping..."
                    )
                    self.stop_pipeline(pipeline_id)
                    continue

                pipeline = db_pipeline_map[pipeline_id]

                # Case 2: Pipeline status changed to PAUSE
                if pipeline.status == "PAUSE":
                    self._logger.info(
                        f"Pipeline {pipeline.name} status is PAUSE. Stopping..."
                    )
                    self.stop_pipeline(pipeline_id)
                    continue

                # Case 3: Pipeline configuration updated
                # We interpret 'REFRESH' as an immediate restart signal too
                if pipeline.status == "REFRESH":
                    self._logger.info(
                        f"Pipeline {pipeline.name} requested REFRESH. Restarting..."
                    )
                    self.restart_pipeline(pipeline_id, pipeline.updated_at)
                    # Reset status to START after refresh signal
                    PipelineRepository.update_status(pipeline_id, "START")
                    continue

                # Check timestamps for config changes
                # Ensure we handle timezone interactions carefully or assume naive/aware consistency
                # Best to compare equality directly if both are same type
                if pipeline.updated_at and proc.last_updated_at:
                    if pipeline.updated_at > proc.last_updated_at:
                        self._logger.info(
                            f"Pipeline {pipeline.name} configuration updated ({pipeline.updated_at} > {proc.last_updated_at}). Restarting..."
                        )
                        self.restart_pipeline(pipeline_id, pipeline.updated_at)

            # Check for pipelines to START
            for pipeline in db_pipelines:
                if pipeline.status == "START":
                    if pipeline.id not in self._processes:
                        self._logger.info(
                            f"Found new pipeline to start: {pipeline.name}"
                        )
                        self.start_pipeline(pipeline.id, pipeline.updated_at)
                    elif not self._processes[pipeline.id].is_alive:
                        self._logger.warning(
                            f"Pipeline {pipeline.name} is START but process died. Restarting..."
                        )
                        self.restart_pipeline(pipeline.id, pipeline.updated_at)

        except Exception as e:
            self._logger.error(f"Error syncing pipeline states: {e}", exc_info=True)

    def monitor(self, check_interval: float = 5.0) -> None:
        """
        Run monitoring loop to manage pipeline lifecycle.

        Args:
            check_interval: Seconds between checks
        """
        self._logger.info(f"Starting pipeline monitor (interval: {check_interval}s)")

        while not self._shutdown_event.is_set():
            self._sync_pipelines_state()

            # Wait before next check, but check shutdown event more frequently
            # This makes Ctrl+C more responsive
            for _ in range(int(check_interval * 2)):
                if self._shutdown_event.is_set():
                    break
                time.sleep(0.5)

        self._logger.info("Monitor loop exited, cleaning up...")

    def shutdown(self, timeout: float = 30.0) -> None:
        """
        Gracefully shutdown all pipelines.

        Args:
            timeout: Seconds to wait for all pipelines to stop
        """
        self._shutdown_event.set()

        # Stop all pipelines
        pk_list = list(self._processes.keys())
        if not pk_list:
            self._logger.info("No active pipelines to shutdown")
            return

        self._logger.info(f"Stopping {len(pk_list)} active pipeline(s)...")
        per_pipeline_timeout = timeout / max(len(pk_list), 1)

        for pipeline_id in pk_list:
            self.stop_pipeline(pipeline_id, timeout=per_pipeline_timeout)

        self._logger.info("All pipelines stopped successfully")

    def run(self) -> None:
        """
        Main entry point: start monitor loop.
        """
        try:
            self.monitor()
        finally:
            # Only shutdown if not already triggered by signal handler
            if not self._shutdown_event.is_set():
                self._shutdown_event.set()
            self.shutdown()


def main():
    """Main entry point for running the pipeline manager."""
    try:
        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        )

        # Initialize database
        init_connection_pool()

        manager = PipelineManager()
        manager.run()
    except Exception as e:
        print(f"Bailed out: {e}")
    finally:
        close_connection_pool()


if __name__ == "__main__":
    main()

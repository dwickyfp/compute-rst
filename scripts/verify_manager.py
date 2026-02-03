
import logging
import time
import unittest
import sys
from unittest.mock import MagicMock, patch
from datetime import datetime, timedelta

# Add current directory to path
import os
sys.path.append(os.getcwd())

print("STARTING VERIFICATION SCRIPT")

from compute.core.manager import PipelineManager, PipelineProcess
from compute.core.models import Pipeline, PipelineStatus

# Configure logging
logging.basicConfig(level=logging.INFO)

class TestPipelineManager(unittest.TestCase):
    
    @patch('compute.core.manager.signal')
    @patch('compute.core.manager.PipelineRepository')
    @patch('compute.core.manager.PipelineMetadataRepository')
    @patch('compute.core.manager.Process')
    @patch('compute.core.manager.init_connection_pool')
    @patch('compute.core.manager.close_connection_pool')
    def test_pipeline_lifecycle(self, mock_close_pool, mock_init_pool, mock_process, mock_metadata_repo, mock_pipeline_repo, mock_signal):
        """Test complete lifecycle: Start -> Update -> Pause -> Delete"""
        
        print("Initializing Manager...")

        
        manager = PipelineManager()
        
        # Setup mock pipeline
        pipeline_id = 1
        pipeline = Pipeline(
            id=pipeline_id,
            name="TestPipeline",
            source_id=1,
            status=PipelineStatus.START.value,
            updated_at=datetime.now()
        )
        
        # Mock Repository to return our pipeline
        mock_pipeline_repo.get_all.return_value = [pipeline]
        mock_pipeline_repo.get_by_id.return_value = pipeline
        
        # Mock Process
        mock_proc_instance = MagicMock()
        mock_proc_instance.is_alive.return_value = True
        mock_proc_instance.pid = 12345
        mock_process.return_value = mock_proc_instance
        
        print("\n=== 1. Testing START detection ===")
        # Run sync once
        manager._sync_pipelines_state()
        
        # Verify start was called
        self.assertIn(pipeline_id, manager._processes)
        self.assertTrue(manager._processes[pipeline_id].is_alive)
        mock_process.assert_called()
        print(f"Pipeline {pipeline_id} started successfully.")
        
        print("\n=== 2. Testing UPDATE detection ===")
        # Update pipeline timestamp in DB mock
        old_updated_at = pipeline.updated_at
        new_updated_at = old_updated_at + timedelta(minutes=1)
        pipeline.updated_at = new_updated_at
        
        # Reset process mock for restart
        mock_proc_instance.start.reset_mock()
        
        # Run sync
        manager._sync_pipelines_state()
        
        # Verify restart (updated_at should update)
        self.assertEqual(manager._processes[pipeline_id].last_updated_at, new_updated_at)
        # Should have called start again (restart = stop + start)
        self.assertTrue(mock_process.called)
        print(f"Pipeline {pipeline_id} restarted successfully on update.")
        
        print("\n=== 3. Testing PAUSE detection ===")
        # Set status to PAUSE
        pipeline.status = PipelineStatus.PAUSE.value
        
        # Run sync
        manager._sync_pipelines_state()
        
        # Verify stopped
        self.assertNotIn(pipeline_id, manager._processes)
        print(f"Pipeline {pipeline_id} paused successfully.")
        
        print("\n=== 4. Testing DELETE detection ===")
        # First restart it manually to test delete
        pipeline.status = PipelineStatus.START.value
        manager.start_pipeline(pipeline_id)
        self.assertIn(pipeline_id, manager._processes)
        
        # Remove from DB return list
        mock_pipeline_repo.get_all.return_value = []
        
        # Run sync
        manager._sync_pipelines_state()
        
        # Verify stopped (deleted)
        self.assertNotIn(pipeline_id, manager._processes)
        print(f"Pipeline {pipeline_id} deletion handled successfully.")
        
if __name__ == '__main__':
    unittest.main()

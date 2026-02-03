
print("Start script")
import sys
import os
sys.path.append(os.getcwd())
print("Path added")

try:
    from compute.core.manager import PipelineManager
    print("Import success")
except Exception as e:
    print(f"Import failed: {e}")

import sys
import os
from pathlib import Path

# Add scripts directory to path so checks can import modules
# Assuming this conftest is in tests/ and scripts is in ../scripts
ROOT_DIR = Path(__file__).parent.parent
SCRIPTS_DIR = ROOT_DIR / "scripts"

sys.path.append(str(SCRIPTS_DIR))

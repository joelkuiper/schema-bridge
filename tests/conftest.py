from __future__ import annotations

import sys
from pathlib import Path

# Ensure tests use the local src package instead of any globally installed one.
PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_PATH = PROJECT_ROOT / "src"
sys.path.insert(0, str(SRC_PATH))

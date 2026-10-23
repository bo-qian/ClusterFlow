"""Run the checkout without installing it. All behavior lives in src/clusterflow."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from clusterflow.cli import main

if __name__ == "__main__":
    raise SystemExit(main())

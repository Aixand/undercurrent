"""Record actual process identity and progress for post-recording verification."""
import json
from pathlib import Path
import subprocess
import sys

phase = sys.argv[1]
session = "review" if phase == "requested" else "demo"
state = ({"import_exists": Path("imported.json").exists()} if phase == "requested"
         else json.loads(Path("worker-evidence.json").read_text()))
result = subprocess.run(["uc", "inspect", session, "--json"], check=True, capture_output=True, text=True)
state["inspection"] = json.loads(result.stdout)
Path(f"checkpoint-{phase}.json").write_text(json.dumps(state, indent=2) + "\n")

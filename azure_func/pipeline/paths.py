import os, tempfile
from pathlib import Path

def base_out() -> Path:
    # Local: ./out  |  Azure Functions: <temp>/out
    if os.getenv("FUNCTIONS_WORKER_RUNTIME"):
        root = Path(os.getenv("BASE_OUT", tempfile.gettempdir())) / "out"
    else:
        root = Path(os.getenv("BASE_OUT", "out"))
    root.mkdir(parents=True, exist_ok=True)
    return root

def dataset_out(dataset: str, *subparts: str) -> Path:
    """
    Returns an ensured directory like:
      out/<dataset>/[subparts...]
    e.g., dataset_out("t100","year=2024") -> out/t100/year=2024
    """
    p = base_out() / dataset
    for part in subparts:
        p /= part
    p.mkdir(parents=True, exist_ok=True)
    return p

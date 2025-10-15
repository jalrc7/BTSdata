import io, os, logging
level = os.environ.get("PYTHON_LOGGING_LEVEL", "INFO").upper()
logging.basicConfig(
    level=getattr(logging, level, logging.INFO),
    format="%(asctime)s %(levelname)s %(name)s: %(message)s"
)
# Reduce SDK noise if you want
logging.getLogger("azure").setLevel(logging.WARNING)
logging.getLogger("urllib3").setLevel(logging.WARNING)
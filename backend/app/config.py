import os
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

BASE_DIR = Path(__file__).resolve().parent.parent

AI_PROVIDER = os.getenv("AI_PROVIDER", "openai")  # openai, anthropic, etc.
AI_MODEL = os.getenv("AI_MODEL", "gpt-4o")
AI_API_KEY = os.getenv("AI_API_KEY", "")
AI_BASE_URL = os.getenv("AI_BASE_URL", None)

DATABASE_URL = os.getenv("DATABASE_URL", f"sqlite:///{BASE_DIR / 'etl_testing.db'}")
UPLOAD_DIR = Path(os.getenv("UPLOAD_DIR", str(BASE_DIR / "uploads")))
UPLOAD_DIR.mkdir(parents=True, exist_ok=True)

def setup_logging():
    import logging
    # Root logger config
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        handlers=[logging.StreamHandler()]
    )
    # Set levels for our specific loggers
    logging.getLogger("etl_ai").setLevel(logging.INFO)
    logging.getLogger("etl_executor").setLevel(logging.INFO)

"""gunicorn server configuration."""
import os

threads = 2  # pylint: disable=invalid-name
workers = 2  # pylint: disable=invalid-name
timeout = 0  # pylint: disable=invalid-name
bind = f":{os.environ.get('PORT', '8000')}"  # pylint: disable=invalid-name
worker_class = "uvicorn.workers.UvicornWorker"  # pylint: disable=invalid-name

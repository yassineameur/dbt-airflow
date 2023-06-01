import os

import pytest
import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from fastapi.testclient import TestClient

from main import app
from setup_db import _get_main_directory


db_url = os.environ.get("DATABASE_URL", "postgres://postgres:postgres@localhost:5432/postgres")
assert "localhost" in db_url

os.environ["DBT_PROFILES_DIR"] = str(_get_main_directory())


@pytest.fixture()
def db_connection():

    connection = psycopg2.connect(
        database="postgres",
        user="postgres",
        password="postgres",
        host="localhost",
        port=5432
    )
    connection.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)

    return connection


@pytest.fixture
def client() -> TestClient:
    test_app = TestClient(app)
    return test_app

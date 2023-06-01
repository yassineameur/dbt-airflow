# make sure dbt init is working
from setup_db import run_setup_db, run_dbt


def test_db_setup():
    # just execute operations without errors

    run_setup_db()
    run_dbt()

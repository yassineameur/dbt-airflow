from api.dbt_routes import _start_job, mark_job_as_success, mark_job_as_failed


def test_default_route(client):
    with client:
        response = client.get("/")
        assert response.status_code == 200
        assert response.json() == {"message": "App is active"}


def test_get_job_when_unfound(client):
    # we set up db at this level before other tests
    # to make sure that db is available
    from setup_db import run_setup_db
    run_setup_db()
    with client:
        response = client.get("/job", params={"job_id": "bad-job"})
        assert response.status_code == 404


def test_get_job_when_started(client, db_connection):

    cursor = db_connection.cursor()
    job_id = _start_job(cursor=cursor, model_name="some-model", run_or_test="test")
    with client:
        response = client.get("/job", params={"job_id": job_id})
        assert response.status_code == 200
        assert response.json()["job_status"] == "started"


def test_get_job_when_succeeded(client, db_connection):

    cursor = db_connection.cursor()
    job_id = _start_job(cursor=cursor, model_name="some-model", run_or_test="test")
    mark_job_as_success(cursor=cursor, job_id=job_id)
    with client:
        response = client.get("/job", params={"job_id": job_id})
        assert response.status_code == 200
        assert response.json()["job_status"] == "success"


def test_get_job_when_failed(client, db_connection):

    cursor = db_connection.cursor()
    job_id = _start_job(cursor=cursor, model_name="some-model", run_or_test="test")
    mark_job_as_failed(cursor=cursor, job_id=job_id)
    with client:
        response = client.get("/job", params={"job_id": job_id})
        assert response.status_code == 200
        assert response.json()["job_status"] == "failed"
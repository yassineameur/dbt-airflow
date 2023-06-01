from datetime import datetime, timedelta
import os
import subprocess
import uuid
from typing import List, Optional
from requests import request
from psycopg2.pool import ThreadedConnectionPool
from psycopg2.extras import DictCursor
from fastapi.responses import JSONResponse
from fastapi import APIRouter, HTTPException, status, Depends, BackgroundTasks, Request
import google
import google.auth.transport.requests
from google.oauth2 import id_token

from lib.logger import get_logger

logger = get_logger()

router = APIRouter()

DBT_JOBS_TABLE = "dbt_jobs"

START_STATUS = "started"

SUCCESS_STATUS = "success"

FAILED_STATUS = "failed"


def run_dbt_command(arguments_list: List[str]):
    command_arguments = ["dbt"] + arguments_list
    res = subprocess.run(command_arguments, check=True)
    if res.returncode == 1:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="The server could not execute your request"
        )


async def get_db_cursor():
    db_name = os.environ.get("PG_DBNAME", "postgres")
    db_host = os.environ.get("PG_HOST", "localhost")
    db_user = os.environ.get("PG_USER", "postgres")
    db_password = os.environ.get("PG_PASSWORD", "postgres")
    db_port = 5432
    dsn = f"dbname='{db_name}' host='{db_host}' user='{db_user}' password='{db_password}' port={db_port}"
    connection_pool = ThreadedConnectionPool(1, 10, dsn=dsn)
    con = connection_pool.getconn()
    con.autocommit = True
    try:
        yield con.cursor(cursor_factory=DictCursor)
    finally:
        connection_pool.putconn(con)


def _start_job(cursor, model_name: str, run_or_test: str):
    job_id = str(uuid.uuid4())
    query = f"""
    INSERT INTO {DBT_JOBS_TABLE} 
    (job_id, model_name, test_or_run, status, c_date)
    VALUES 
    (%s, %s, %s, %s, %s)
    """
    cursor.execute(query, (job_id, model_name, run_or_test, START_STATUS, datetime.utcnow()))
    return job_id


def _update_job_status(cursor, job_id, job_status):
    query = f"""
    UPDATE {DBT_JOBS_TABLE}
    SET status = %s
    WHERE job_id = %s
    """
    cursor.execute(query, (job_status, job_id))


def mark_job_as_success(cursor, job_id):
    _update_job_status(cursor, job_id=job_id, job_status=SUCCESS_STATUS)


def mark_job_as_failed(cursor, job_id):
    _update_job_status(cursor, job_id=job_id, job_status=FAILED_STATUS)


from pydantic import BaseModel


class DBTJob(BaseModel):
    job_id: str
    c_date: datetime
    status: str


def get_latest_job(cursor, model_name: str, run_or_test: str) -> Optional[DBTJob]:

    query = f"""
    SELECT 
        job_id, 
        c_date, 
        status
    FROM {DBT_JOBS_TABLE} 
    WHERE model_name = %s and test_or_run = %s AND c_date >= %s
    ORDER BY c_date DESC
    LIMIT 1
    """
    cursor.execute(query, (model_name, run_or_test, datetime.utcnow() - timedelta(hours=1)))
    res = cursor.fetchone()
    if res:
        return DBTJob(**dict(res))

    return None


@router.get("/")
def root():
    return {"message": "App is active"}


@router.post("/command", status_code=status.HTTP_201_CREATED)
def run_dbt_command_api(command: str):

    run_dbt_command(command.split(" "))


def run_dbt_job(cursor, job_id: str, model_name: str, run_or_test: str, current_url: str):
    command = [run_or_test, "--select", model_name]
    try:

        logger.info(f"Running job: {job_id}, command {command}")
        current_url = current_url.replace("http://", "https://")

        audiance = current_url
        if not current_url.endswith("/"):
            audiance += "/"

        auth_req = google.auth.transport.requests.Request()
        id_token = google.oauth2.id_token.fetch_id_token(auth_req, audiance)

        command_endpoint = os.path.join(current_url, "command")
        res = request(
            method="POST",
            url=command_endpoint,
            params={"command": " ".join(command)},
            headers={"Authorization": f"Bearer {id_token}"}
        )
        res.raise_for_status()
    except Exception as e:
        logger.error(f"Encountered error for job {job_id}: {str(e)}")
        mark_job_as_failed(cursor, job_id=job_id)
        raise Exception(str(e)) from e
    logger.info(f"Job {job_id} is successfull")
    mark_job_as_success(cursor, job_id=job_id)


def run_on_test_one_model(
        cursor,
        request: Request,
        background_tasks: BackgroundTasks,
        model_name: str,
        run_or_test: str
):
    latest_job = get_latest_job(
        cursor=cursor,
        model_name=model_name,
        run_or_test=run_or_test
    )

    skip_job = False
    if latest_job is None or latest_job.status == FAILED_STATUS:
        # In this case we launch a new job
        message = f"Model {run_or_test} {model_name} is launched"
        job_id = _start_job(cursor, model_name=model_name, run_or_test=run_or_test)
        background_tasks.add_task(
            run_dbt_job,
            cursor=cursor,
            job_id=job_id,
            model_name=model_name,
            run_or_test=run_or_test,
            current_url=os.path.dirname(request.url._url)
        )
    else:
        message = f"Model {run_or_test} {model_name} is already launched during a previous call"
        job_id = latest_job.job_id
        if latest_job.status == SUCCESS_STATUS:
            skip_job = True

    return {"message": message, "job_id": job_id, "skip_job": skip_job}


@router.post("/run_model", status_code=status.HTTP_201_CREATED)
def run_one_model(model_name: str, request: Request, background_tasks: BackgroundTasks, cursor=Depends(get_db_cursor)):

    return run_on_test_one_model(
        cursor=cursor,
        request=request,
        background_tasks=background_tasks,
        run_or_test="run",
        model_name=model_name
    )


@router.post("/test_model", status_code=status.HTTP_202_ACCEPTED)
def test_one_model(model_name: str, request: Request, background_tasks: BackgroundTasks, cursor=Depends(get_db_cursor)):
    return run_on_test_one_model(
        cursor=cursor,
        request=request,
        background_tasks=background_tasks,
        run_or_test="test",
        model_name=model_name
    )


@router.get("/job")
def get_job_status(job_id: str, cursor=Depends(get_db_cursor)):
    query = """SELECT status FROM dbt_jobs WHERE job_id=%s"""
    cursor.execute(query, (job_id,))
    res = cursor.fetchone()
    if res is None:
        return JSONResponse(content={"message": f"job {job_id} not found"}, status_code=status.HTTP_404_NOT_FOUND)
    return {"job_status": res[0]}
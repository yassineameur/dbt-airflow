from api.dbt_routes import router as dbt_router

from fastapi import FastAPI

app = FastAPI()

app.include_router(dbt_router)

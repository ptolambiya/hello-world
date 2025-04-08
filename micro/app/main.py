from fastapi import FastAPI
from routers import config_router

app = FastAPI(title="Configuration Service")
app.include_router(config_router.router, prefix="/api/v1/config")

@app.get("/")
def read_root():
    return {"message": "Configuration Service Running"}

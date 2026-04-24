import os
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from fastapi import FastAPI

from src.api.router import api_router
from src.bootstrap import wire_application
from src.config.settings import get_config


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    wire_application(app)
    yield


def create_app() -> FastAPI:
    config = get_config()
    app = FastAPI(title=config.app_name, lifespan=lifespan)
    app.include_router(api_router)
    return app


app = create_app()


def run() -> None:
    import uvicorn

    config = get_config()
    uvicorn.run(
        "src.main:app",
        host="0.0.0.0",
        port=int(os.environ.get("REST_PORT", "9999")),
        reload=config.debug,
    )


if __name__ == "__main__":
    run()

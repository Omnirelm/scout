from fastapi import APIRouter

from src.api.routes import health, skills, tasks

api_router = APIRouter()
api_router.include_router(health.router, tags=["health"])
api_router.include_router(skills.router)
api_router.include_router(tasks.router)

from fastapi import APIRouter, Depends

from src.config.settings import OrchestratorConfig, get_config

router = APIRouter()


@router.get("/health")
def health(config: OrchestratorConfig = Depends(get_config)) -> dict[str, str]:
    return {"status": "ok", "service": config.app_name}

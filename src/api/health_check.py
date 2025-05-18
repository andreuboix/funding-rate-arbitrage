"""
API para health-check y monitoreo de la estrategia de arbitraje.
"""
import asyncio
import json
from datetime import datetime
from typing import Dict, Optional

import uvicorn
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

from src.execution.execution_engine import ExecutionEngine
from src.risk.risk_manager import RiskManager


# Modelos para la API
class HealthResponse(BaseModel):
    """Respuesta del endpoint de health-check."""
    status: str
    uptime: str
    active_positions: int
    funding_differentials: Dict[str, float]
    risk_metrics: Dict
    timestamp: str


app = FastAPI(title="Funding Rate Arbitrage API", version="0.1.0")

# Variables globales para almacenar referencias a los componentes
_execution_engine: Optional[ExecutionEngine] = None
_risk_manager: Optional[RiskManager] = None
_start_time: Optional[datetime] = None


@app.get("/health", response_model=HealthResponse)
async def health_check():
    """Endpoint de health-check que proporciona información sobre el estado del sistema."""
    global _execution_engine, _risk_manager, _start_time
    
    if not _execution_engine or not _risk_manager or not _start_time:
        raise HTTPException(status_code=503, detail="Sistema no inicializado completamente")
    
    # Calcular tiempo de actividad
    uptime = datetime.now() - _start_time
    uptime_str = str(uptime).split('.')[0]  # Formato HH:MM:SS
    
    # Obtener diferenciales de funding rate actuales
    funding_differentials = {}
    
    for position_id, position in _execution_engine.active_positions.items():
        pair_key = f"{position.long_position.symbol}_{position.short_position.symbol}"
        funding_differentials[pair_key] = position.current_funding_rate_diff
    
    # Obtener métricas de riesgo
    risk_metrics = _risk_manager.get_risk_metrics()
    
    return HealthResponse(
        status="running",
        uptime=uptime_str,
        active_positions=len(_execution_engine.active_positions),
        funding_differentials=funding_differentials,
        risk_metrics=risk_metrics,
        timestamp=datetime.now().isoformat()
    )


@app.get("/positions")
async def get_positions():
    """Endpoint para obtener información sobre las posiciones activas."""
    global _execution_engine
    
    if not _execution_engine:
        raise HTTPException(status_code=503, detail="Sistema no inicializado completamente")
    
    positions = {}
    
    for position_id, position in _execution_engine.active_positions.items():
        positions[position_id] = {
            "long_exchange": position.long_position.exchange,
            "long_symbol": position.long_position.symbol,
            "long_amount": position.long_position.amount,
            "long_entry_price": position.long_position.entry_price,
            "short_exchange": position.short_position.exchange,
            "short_symbol": position.short_position.symbol,
            "short_amount": position.short_position.amount,
            "short_entry_price": position.short_position.entry_price,
            "funding_rate_diff_at_entry": position.funding_rate_diff_at_entry,
            "current_funding_rate_diff": position.current_funding_rate_diff,
            "total_pnl": position.total_pnl,
            "open_time": position.open_time.isoformat(),
            "holding_time_hours": (datetime.now() - position.open_time).total_seconds() / 3600
        }
    
    return {"positions": positions}


@app.get("/funding_rates")
async def get_funding_rates():
    """Endpoint para obtener las tasas de funding actuales."""
    global _execution_engine
    
    if not _execution_engine:
        raise HTTPException(status_code=503, detail="Sistema no inicializado completamente")
    
    funding_rates = {}
    
    for identifier, rate_info in _execution_engine.funding_rates.items():
        funding_rates[identifier] = {
            "exchange": rate_info.exchange,
            "symbol": rate_info.symbol,
            "funding_rate": rate_info.funding_rate,
            "next_funding_time": rate_info.next_funding_time.isoformat(),
            "mark_price": rate_info.mark_price,
            "index_price": rate_info.index_price,
            "timestamp": rate_info.timestamp.isoformat()
        }
    
    return {"funding_rates": funding_rates}


@app.get("/metrics")
async def get_metrics():
    """Endpoint para obtener métricas en formato compatible con Prometheus."""
    global _execution_engine, _risk_manager, _start_time
    
    if not _execution_engine or not _risk_manager or not _start_time:
        raise HTTPException(status_code=503, detail="Sistema no inicializado completamente")
    
    metrics = []
    
    # Métrica de tiempo de actividad
    uptime_seconds = (datetime.now() - _start_time).total_seconds()
    metrics.append(f"# HELP arbitrage_uptime_seconds Tiempo de actividad en segundos")
    metrics.append(f"# TYPE arbitrage_uptime_seconds gauge")
    metrics.append(f"arbitrage_uptime_seconds {uptime_seconds}")
    
    # Métrica de posiciones activas
    metrics.append(f"# HELP arbitrage_active_positions Número de posiciones de arbitraje activas")
    metrics.append(f"# TYPE arbitrage_active_positions gauge")
    metrics.append(f"arbitrage_active_positions {len(_execution_engine.active_positions)}")
    
    # Métricas de P&L
    metrics.append(f"# HELP arbitrage_daily_pnl P&L diario acumulado")
    metrics.append(f"# TYPE arbitrage_daily_pnl gauge")
    metrics.append(f"arbitrage_daily_pnl {_risk_manager.daily_pnl}")
    
    # Métricas de funding rates
    metrics.append(f"# HELP arbitrage_funding_rate Tasa de funding por exchange y símbolo")
    metrics.append(f"# TYPE arbitrage_funding_rate gauge")
    
    for identifier, rate_info in _execution_engine.funding_rates.items():
        metrics.append(f'arbitrage_funding_rate{{exchange="{rate_info.exchange}",symbol="{rate_info.symbol}"}} {rate_info.funding_rate}')
    
    # Métricas de diferenciales de funding
    metrics.append(f"# HELP arbitrage_funding_diff Diferencial de funding rate para posiciones activas")
    metrics.append(f"# TYPE arbitrage_funding_diff gauge")
    
    for position_id, position in _execution_engine.active_positions.items():
        metrics.append(f'arbitrage_funding_diff{{position_id="{position_id}",long_exchange="{position.long_position.exchange}",long_symbol="{position.long_position.symbol}",short_exchange="{position.short_position.exchange}",short_symbol="{position.short_position.symbol}"}} {position.current_funding_rate_diff}')
    
    return "\n".join(metrics)


async def start_api_server(port: int, execution_engine: ExecutionEngine, risk_manager: RiskManager):
    """
    Inicia el servidor API para health-check y monitoreo.
    
    Args:
        port: Puerto en el que escuchará el servidor.
        execution_engine: Instancia del motor de ejecución.
        risk_manager: Instancia del gestor de riesgos.
    """
    global _execution_engine, _risk_manager, _start_time
    
    _execution_engine = execution_engine
    _risk_manager = risk_manager
    _start_time = datetime.now()
    
    config = uvicorn.Config(app, host="0.0.0.0", port=port, log_level="info")
    server = uvicorn.Server(config)
    
    await server.serve()

"""
Módulo de configuración para cargar variables de entorno y parámetros de trading.
"""
import os
from typing import Dict, List, Optional, Tuple

from dotenv import load_dotenv
from pydantic import BaseModel, Field


class ExchangeConfig(BaseModel):
    """Configuración para un exchange específico."""
    api_key: str
    api_secret: str
    passphrase: Optional[str] = None


class TradingPairConfig(BaseModel):
    """Configuración para un par de trading."""
    exchange: str
    symbol: str
    
    @property
    def identifier(self) -> str:
        """Devuelve el identificador completo del par de trading."""
        return f"{self.exchange}:{self.symbol}"


class RiskConfig(BaseModel):
    """Configuración para la gestión de riesgos."""
    max_daily_drawdown: float = Field(..., description="Máxima pérdida diaria permitida (USD)")
    max_position_holding_time: int = Field(..., description="Tiempo máximo de mantenimiento de posición (horas)")
    exit_funding_rate_diff: float = Field(..., description="Umbral de diferencial para cerrar posición (% por 8h)")


class Config(BaseModel):
    """Configuración global de la aplicación."""
    exchanges: Dict[str, ExchangeConfig]
    trading_pairs: List[TradingPairConfig]
    min_funding_rate_diff: float
    max_position_size: float
    risk: RiskConfig
    api_port: int
    log_level: str
    log_dir: str


def load_config() -> Config:
    """
    Carga la configuración desde variables de entorno.
    
    Returns:
        Config: Objeto de configuración con todos los parámetros.
    """
    load_dotenv()
    
    # Cargar configuración de exchanges
    exchanges = {}
    
    # Binance
    if os.getenv("BINANCE_API_KEY") and os.getenv("BINANCE_API_SECRET"):
        exchanges["BINANCE"] = ExchangeConfig(
            api_key=os.getenv("BINANCE_API_KEY", ""),
            api_secret=os.getenv("BINANCE_API_SECRET", "")
        )
    
    # Bybit
    if os.getenv("BYBIT_API_KEY") and os.getenv("BYBIT_API_SECRET"):
        exchanges["BYBIT"] = ExchangeConfig(
            api_key=os.getenv("BYBIT_API_KEY", ""),
            api_secret=os.getenv("BYBIT_API_SECRET", "")
        )
    
    # OKX
    if os.getenv("OKX_API_KEY") and os.getenv("OKX_API_SECRET"):
        exchanges["OKX"] = ExchangeConfig(
            api_key=os.getenv("OKX_API_KEY", ""),
            api_secret=os.getenv("OKX_API_SECRET", ""),
            passphrase=os.getenv("OKX_PASSPHRASE", "")
        )
    
    # Cargar pares de trading
    trading_pairs_str = os.getenv("TRADING_PAIRS", "")
    trading_pairs = []
    
    for pair_str in trading_pairs_str.split(","):
        if ":" in pair_str:
            exchange, symbol = pair_str.strip().split(":", 1)
            trading_pairs.append(TradingPairConfig(exchange=exchange, symbol=symbol))
    
    # Cargar configuración de riesgo
    risk_config = RiskConfig(
        max_daily_drawdown=float(os.getenv("MAX_DAILY_DRAWDOWN", "500")),
        max_position_holding_time=int(os.getenv("MAX_POSITION_HOLDING_TIME", "24")),
        exit_funding_rate_diff=float(os.getenv("EXIT_FUNDING_RATE_DIFF", "0.005"))
    )
    
    # Crear configuración global
    config = Config(
        exchanges=exchanges,
        trading_pairs=trading_pairs,
        min_funding_rate_diff=float(os.getenv("MIN_FUNDING_RATE_DIFF", "0.01")),
        max_position_size=float(os.getenv("MAX_POSITION_SIZE", "10000")),
        risk=risk_config,
        api_port=int(os.getenv("API_PORT", "8000")),
        log_level=os.getenv("LOG_LEVEL", "INFO"),
        log_dir=os.getenv("LOG_DIR", "./logs")
    )
    
    return config

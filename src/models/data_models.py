"""
Modelos de datos para la estrategia de arbitraje de funding rate.
"""
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from typing import Dict, List, Optional, Union

from pydantic import BaseModel, Field


class OrderSide(str, Enum):
    """Lado de la orden: compra o venta."""
    BUY = "buy"
    SELL = "sell"


class OrderType(str, Enum):
    """Tipo de orden: mercado o límite."""
    MARKET = "market"
    LIMIT = "limit"


class OrderStatus(str, Enum):
    """Estado de la orden."""
    OPEN = "open"
    FILLED = "filled"
    PARTIALLY_FILLED = "partially_filled"
    CANCELED = "canceled"
    REJECTED = "rejected"


class FundingRateInfo(BaseModel):
    """Información de funding rate para un contrato perpetuo."""
    exchange: str
    symbol: str
    funding_rate: float  # Tasa actual en porcentaje (por 8h)
    next_funding_time: datetime
    mark_price: float
    index_price: float
    timestamp: datetime
    
    @property
    def identifier(self) -> str:
        """Devuelve el identificador completo del par de trading."""
        return f"{self.exchange}:{self.symbol}"


class ArbitrageOpportunity(BaseModel):
    """Oportunidad de arbitraje identificada entre dos exchanges."""
    long_exchange: str
    long_symbol: str
    short_exchange: str
    short_symbol: str
    funding_rate_diff: float  # Diferencial en porcentaje (por 8h)
    theoretical_profit: float  # Beneficio teórico en porcentaje (por 8h)
    timestamp: datetime
    
    @property
    def long_identifier(self) -> str:
        """Devuelve el identificador completo del par long."""
        return f"{self.long_exchange}:{self.long_symbol}"
    
    @property
    def short_identifier(self) -> str:
        """Devuelve el identificador completo del par short."""
        return f"{self.short_exchange}:{self.short_symbol}"


class Order(BaseModel):
    """Orden enviada a un exchange."""
    exchange: str
    symbol: str
    order_id: str
    client_order_id: Optional[str] = None
    side: OrderSide
    type: OrderType
    price: Optional[float] = None  # Requerido para órdenes límite
    amount: float
    status: OrderStatus = OrderStatus.OPEN
    filled_amount: float = 0
    average_fill_price: Optional[float] = None
    timestamp: datetime
    
    @property
    def identifier(self) -> str:
        """Devuelve el identificador completo del par de trading."""
        return f"{self.exchange}:{self.symbol}"


class Position(BaseModel):
    """Posición abierta en un exchange."""
    exchange: str
    symbol: str
    side: OrderSide
    amount: float
    entry_price: float
    current_price: float
    unrealized_pnl: float = 0
    realized_pnl: float = 0
    open_time: datetime
    last_update_time: datetime
    
    @property
    def identifier(self) -> str:
        """Devuelve el identificador completo del par de trading."""
        return f"{self.exchange}:{self.symbol}"
    
    @property
    def position_value(self) -> float:
        """Devuelve el valor actual de la posición en USD."""
        return self.amount * self.current_price


class ArbitragePosition(BaseModel):
    """Par de posiciones de arbitraje (long en un exchange, short en otro)."""
    id: str
    long_position: Position
    short_position: Position
    funding_rate_diff_at_entry: float
    current_funding_rate_diff: float = 0
    open_time: datetime
    last_update_time: datetime
    
    @property
    def total_pnl(self) -> float:
        """Devuelve el P&L total de la posición de arbitraje."""
        return self.long_position.unrealized_pnl + self.long_position.realized_pnl + \
               self.short_position.unrealized_pnl + self.short_position.realized_pnl
    
    @property
    def total_position_value(self) -> float:
        """Devuelve el valor total de la posición de arbitraje en USD."""
        return self.long_position.position_value + self.short_position.position_value

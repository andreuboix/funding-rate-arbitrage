"""
Módulo base para interactuar con exchanges de criptomonedas.
"""
import asyncio
import logging
from abc import ABC, abstractmethod
from datetime import datetime
from typing import Dict, List, Optional, Tuple, Union

import ccxt.async_support as ccxt
from ccxt.base.errors import ExchangeError, NetworkError

from src.models.data_models import FundingRateInfo, Order, OrderSide, OrderStatus, OrderType, Position


class BaseExchange(ABC):
    """Clase base abstracta para interactuar con exchanges de criptomonedas."""
    
    def __init__(self, exchange_id: str, api_key: str, api_secret: str, passphrase: Optional[str] = None):
        """
        Inicializa la conexión con el exchange.
        
        Args:
            exchange_id: Identificador del exchange (ej. 'binance', 'bybit').
            api_key: Clave API para autenticación.
            api_secret: Secreto API para autenticación.
            passphrase: Contraseña adicional (requerida para algunos exchanges como OKX).
        """
        self.exchange_id = exchange_id.lower()
        self.logger = logging.getLogger(f"exchange.{self.exchange_id}")
        
        # Configuración de la conexión
        exchange_config = {
            'apiKey': api_key,
            'secret': api_secret,
            'enableRateLimit': True,
            'options': {
                'defaultType': 'future',  # Usar futuros/perpetuos por defecto
            }
        }
        
        if passphrase:
            exchange_config['password'] = passphrase
        
        # Inicializar cliente CCXT
        self.client = getattr(ccxt, self.exchange_id)(exchange_config)
        self.markets = {}
        self.initialized = False
    
    async def initialize(self) -> None:
        """Inicializa el exchange cargando mercados y otra información necesaria."""
        try:
            self.logger.info(f"Inicializando exchange {self.exchange_id}...")
            self.markets = await self.client.load_markets()
            self.initialized = True
            self.logger.info(f"Exchange {self.exchange_id} inicializado correctamente.")
        except (ExchangeError, NetworkError) as e:
            self.logger.error(f"Error al inicializar exchange {self.exchange_id}: {str(e)}")
            raise
    
    async def close(self) -> None:
        """Cierra la conexión con el exchange."""
        await self.client.close()
    
    @abstractmethod
    async def get_funding_rate(self, symbol: str) -> FundingRateInfo:
        """
        Obtiene la tasa de financiamiento actual para un símbolo.
        
        Args:
            symbol: Símbolo del contrato perpetuo.
            
        Returns:
            FundingRateInfo: Información de la tasa de financiamiento.
        """
        pass
    
    @abstractmethod
    async def get_mark_price(self, symbol: str) -> float:
        """
        Obtiene el precio mark actual para un símbolo.
        
        Args:
            symbol: Símbolo del contrato perpetuo.
            
        Returns:
            float: Precio mark actual.
        """
        pass
    
    @abstractmethod
    async def get_index_price(self, symbol: str) -> float:
        """
        Obtiene el precio index actual para un símbolo.
        
        Args:
            symbol: Símbolo del contrato perpetuo.
            
        Returns:
            float: Precio index actual.
        """
        pass
    
    @abstractmethod
    async def get_orderbook(self, symbol: str, limit: int = 20) -> Dict:
        """
        Obtiene el libro de órdenes para un símbolo.
        
        Args:
            symbol: Símbolo del contrato perpetuo.
            limit: Número de niveles a obtener.
            
        Returns:
            Dict: Libro de órdenes con bids y asks.
        """
        pass
    
    @abstractmethod
    async def create_order(self, symbol: str, order_type: OrderType, side: OrderSide, 
                          amount: float, price: Optional[float] = None) -> Order:
        """
        Crea una orden en el exchange.
        
        Args:
            symbol: Símbolo del contrato perpetuo.
            order_type: Tipo de orden (market, limit).
            side: Lado de la orden (buy, sell).
            amount: Cantidad a comprar/vender.
            price: Precio para órdenes límite.
            
        Returns:
            Order: Información de la orden creada.
        """
        pass
    
    @abstractmethod
    async def get_order(self, symbol: str, order_id: str) -> Order:
        """
        Obtiene información de una orden específica.
        
        Args:
            symbol: Símbolo del contrato perpetuo.
            order_id: ID de la orden.
            
        Returns:
            Order: Información actualizada de la orden.
        """
        pass
    
    @abstractmethod
    async def cancel_order(self, symbol: str, order_id: str) -> bool:
        """
        Cancela una orden específica.
        
        Args:
            symbol: Símbolo del contrato perpetuo.
            order_id: ID de la orden.
            
        Returns:
            bool: True si se canceló correctamente, False en caso contrario.
        """
        pass
    
    @abstractmethod
    async def get_position(self, symbol: str) -> Optional[Position]:
        """
        Obtiene la posición actual para un símbolo.
        
        Args:
            symbol: Símbolo del contrato perpetuo.
            
        Returns:
            Optional[Position]: Información de la posición o None si no hay posición.
        """
        pass
    
    @abstractmethod
    async def get_balance(self) -> Dict[str, float]:
        """
        Obtiene el balance de la cuenta.
        
        Returns:
            Dict[str, float]: Balance por moneda.
        """
        pass
    
    @abstractmethod
    async def estimate_slippage(self, symbol: str, side: OrderSide, amount: float) -> float:
        """
        Estima el slippage para una operación de mercado.
        
        Args:
            symbol: Símbolo del contrato perpetuo.
            side: Lado de la orden (buy, sell).
            amount: Cantidad a comprar/vender.
            
        Returns:
            float: Slippage estimado en porcentaje.
        """
        pass

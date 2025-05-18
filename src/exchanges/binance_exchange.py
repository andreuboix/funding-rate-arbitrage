"""
Implementación específica para el exchange Binance.
"""
import asyncio
import logging
from datetime import datetime
from typing import Dict, List, Optional, Tuple

import ccxt.async_support as ccxt
from ccxt.base.errors import ExchangeError, NetworkError

from src.exchanges.base_exchange import BaseExchange
from src.models.data_models import FundingRateInfo, Order, OrderSide, OrderStatus, OrderType, Position


class BinanceExchange(BaseExchange):
    """Clase para interactuar con el exchange Binance."""
    
    def __init__(self, api_key: str, api_secret: str):
        """
        Inicializa la conexión con Binance.
        
        Args:
            api_key: Clave API para autenticación.
            api_secret: Secreto API para autenticación.
        """
        super().__init__('binance', api_key, api_secret)
        
        # Configuraciones específicas para Binance
        self.client.options['defaultType'] = 'future'
    
    async def get_funding_rate(self, symbol: str) -> FundingRateInfo:
        """
        Obtiene la tasa de financiamiento actual para un símbolo en Binance.
        
        Args:
            symbol: Símbolo del contrato perpetuo (ej. 'BTC/USDT').
            
        Returns:
            FundingRateInfo: Información de la tasa de financiamiento.
        """
        try:
            # Obtener información de financiamiento
            funding_info = await self.client.fapiPublic_get_premiumindex({'symbol': self.client.market_id(symbol)})
            
            # Obtener precios mark e index
            mark_price = float(funding_info['markPrice'])
            index_price = float(funding_info['indexPrice'])
            
            # Obtener próximo tiempo de financiamiento
            funding_time = await self.client.fapiPublic_get_fundinginfo({'symbol': self.client.market_id(symbol)})
            next_funding_time = datetime.fromtimestamp(int(funding_time['nextFundingTime']) / 1000)
            
            # Crear objeto de respuesta
            return FundingRateInfo(
                exchange="BINANCE",
                symbol=symbol,
                funding_rate=float(funding_info['lastFundingRate']) * 100,  # Convertir a porcentaje
                next_funding_time=next_funding_time,
                mark_price=mark_price,
                index_price=index_price,
                timestamp=datetime.now()
            )
        except (ExchangeError, NetworkError) as e:
            self.logger.error(f"Error al obtener funding rate para {symbol} en Binance: {str(e)}")
            raise
    
    async def get_mark_price(self, symbol: str) -> float:
        """
        Obtiene el precio mark actual para un símbolo en Binance.
        
        Args:
            symbol: Símbolo del contrato perpetuo.
            
        Returns:
            float: Precio mark actual.
        """
        try:
            mark_price_info = await self.client.fapiPublic_get_premiumindex({'symbol': self.client.market_id(symbol)})
            return float(mark_price_info['markPrice'])
        except (ExchangeError, NetworkError) as e:
            self.logger.error(f"Error al obtener mark price para {symbol} en Binance: {str(e)}")
            raise
    
    async def get_index_price(self, symbol: str) -> float:
        """
        Obtiene el precio index actual para un símbolo en Binance.
        
        Args:
            symbol: Símbolo del contrato perpetuo.
            
        Returns:
            float: Precio index actual.
        """
        try:
            index_price_info = await self.client.fapiPublic_get_premiumindex({'symbol': self.client.market_id(symbol)})
            return float(index_price_info['indexPrice'])
        except (ExchangeError, NetworkError) as e:
            self.logger.error(f"Error al obtener index price para {symbol} en Binance: {str(e)}")
            raise
    
    async def get_orderbook(self, symbol: str, limit: int = 20) -> Dict:
        """
        Obtiene el libro de órdenes para un símbolo en Binance.
        
        Args:
            symbol: Símbolo del contrato perpetuo.
            limit: Número de niveles a obtener.
            
        Returns:
            Dict: Libro de órdenes con bids y asks.
        """
        try:
            return await self.client.fetch_order_book(symbol, limit)
        except (ExchangeError, NetworkError) as e:
            self.logger.error(f"Error al obtener orderbook para {symbol} en Binance: {str(e)}")
            raise
    
    async def create_order(self, symbol: str, order_type: OrderType, side: OrderSide, 
                          amount: float, price: Optional[float] = None) -> Order:
        """
        Crea una orden en Binance.
        
        Args:
            symbol: Símbolo del contrato perpetuo.
            order_type: Tipo de orden (market, limit).
            side: Lado de la orden (buy, sell).
            amount: Cantidad a comprar/vender.
            price: Precio para órdenes límite.
            
        Returns:
            Order: Información de la orden creada.
        """
        try:
            # Preparar parámetros de la orden
            params = {}
            
            # Crear orden en el exchange
            ccxt_order = await self.client.create_order(
                symbol=symbol,
                type=order_type.value,
                side=side.value,
                amount=amount,
                price=price if order_type == OrderType.LIMIT else None,
                params=params
            )
            
            # Convertir a nuestro modelo de orden
            return Order(
                exchange="BINANCE",
                symbol=symbol,
                order_id=ccxt_order['id'],
                client_order_id=ccxt_order.get('clientOrderId'),
                side=OrderSide(ccxt_order['side']),
                type=OrderType(ccxt_order['type']),
                price=float(ccxt_order['price']) if ccxt_order.get('price') else None,
                amount=float(ccxt_order['amount']),
                status=self._convert_order_status(ccxt_order['status']),
                filled_amount=float(ccxt_order.get('filled', 0)),
                average_fill_price=float(ccxt_order.get('average')) if ccxt_order.get('average') else None,
                timestamp=datetime.fromtimestamp(ccxt_order['timestamp'] / 1000)
            )
        except (ExchangeError, NetworkError) as e:
            self.logger.error(f"Error al crear orden para {symbol} en Binance: {str(e)}")
            raise
    
    async def get_order(self, symbol: str, order_id: str) -> Order:
        """
        Obtiene información de una orden específica en Binance.
        
        Args:
            symbol: Símbolo del contrato perpetuo.
            order_id: ID de la orden.
            
        Returns:
            Order: Información actualizada de la orden.
        """
        try:
            ccxt_order = await self.client.fetch_order(order_id, symbol)
            
            return Order(
                exchange="BINANCE",
                symbol=symbol,
                order_id=ccxt_order['id'],
                client_order_id=ccxt_order.get('clientOrderId'),
                side=OrderSide(ccxt_order['side']),
                type=OrderType(ccxt_order['type']),
                price=float(ccxt_order['price']) if ccxt_order.get('price') else None,
                amount=float(ccxt_order['amount']),
                status=self._convert_order_status(ccxt_order['status']),
                filled_amount=float(ccxt_order.get('filled', 0)),
                average_fill_price=float(ccxt_order.get('average')) if ccxt_order.get('average') else None,
                timestamp=datetime.fromtimestamp(ccxt_order['timestamp'] / 1000)
            )
        except (ExchangeError, NetworkError) as e:
            self.logger.error(f"Error al obtener orden {order_id} para {symbol} en Binance: {str(e)}")
            raise
    
    async def cancel_order(self, symbol: str, order_id: str) -> bool:
        """
        Cancela una orden específica en Binance.
        
        Args:
            symbol: Símbolo del contrato perpetuo.
            order_id: ID de la orden.
            
        Returns:
            bool: True si se canceló correctamente, False en caso contrario.
        """
        try:
            await self.client.cancel_order(order_id, symbol)
            return True
        except (ExchangeError, NetworkError) as e:
            self.logger.error(f"Error al cancelar orden {order_id} para {symbol} en Binance: {str(e)}")
            return False
    
    async def get_position(self, symbol: str) -> Optional[Position]:
        """
        Obtiene la posición actual para un símbolo en Binance.
        
        Args:
            symbol: Símbolo del contrato perpetuo.
            
        Returns:
            Optional[Position]: Información de la posición o None si no hay posición.
        """
        try:
            positions = await self.client.fapiPrivate_get_positionrisk({'symbol': self.client.market_id(symbol)})
            
            if not positions or float(positions[0]['positionAmt']) == 0:
                return None
            
            position_data = positions[0]
            side = OrderSide.BUY if float(position_data['positionAmt']) > 0 else OrderSide.SELL
            amount = abs(float(position_data['positionAmt']))
            entry_price = float(position_data['entryPrice'])
            current_price = float(position_data['markPrice'])
            unrealized_pnl = float(position_data['unRealizedProfit'])
            
            return Position(
                exchange="BINANCE",
                symbol=symbol,
                side=side,
                amount=amount,
                entry_price=entry_price,
                current_price=current_price,
                unrealized_pnl=unrealized_pnl,
                realized_pnl=0,  # Binance no proporciona PnL realizado directamente
                open_time=datetime.now(),  # Binance no proporciona tiempo de apertura directamente
                last_update_time=datetime.now()
            )
        except (ExchangeError, NetworkError) as e:
            self.logger.error(f"Error al obtener posición para {symbol} en Binance: {str(e)}")
            raise
    
    async def get_balance(self) -> Dict[str, float]:
        """
        Obtiene el balance de la cuenta en Binance.
        
        Returns:
            Dict[str, float]: Balance por moneda.
        """
        try:
            balance = await self.client.fetch_balance()
            return {currency: float(data['total']) for currency, data in balance['total'].items() if float(data['total']) > 0}
        except (ExchangeError, NetworkError) as e:
            self.logger.error(f"Error al obtener balance en Binance: {str(e)}")
            raise
    
    async def estimate_slippage(self, symbol: str, side: OrderSide, amount: float) -> float:
        """
        Estima el slippage para una operación de mercado en Binance.
        
        Args:
            symbol: Símbolo del contrato perpetuo.
            side: Lado de la orden (buy, sell).
            amount: Cantidad a comprar/vender.
            
        Returns:
            float: Slippage estimado en porcentaje.
        """
        try:
            orderbook = await self.get_orderbook(symbol)
            
            if side == OrderSide.BUY:
                # Para compras, usamos los asks (ventas)
                asks = orderbook['asks']
                total_amount = 0
                weighted_price = 0
                
                for price, size in asks:
                    if total_amount >= amount:
                        break
                    
                    usable_amount = min(size, amount - total_amount)
                    weighted_price += price * usable_amount
                    total_amount += usable_amount
                
                if total_amount > 0:
                    average_price = weighted_price / total_amount
                    mark_price = await self.get_mark_price(symbol)
                    return ((average_price / mark_price) - 1) * 100  # Slippage en porcentaje
                
                return 0
            else:
                # Para ventas, usamos los bids (compras)
                bids = orderbook['bids']
                total_amount = 0
                weighted_price = 0
                
                for price, size in bids:
                    if total_amount >= amount:
                        break
                    
                    usable_amount = min(size, amount - total_amount)
                    weighted_price += price * usable_amount
                    total_amount += usable_amount
                
                if total_amount > 0:
                    average_price = weighted_price / total_amount
                    mark_price = await self.get_mark_price(symbol)
                    return ((1 - (average_price / mark_price))) * 100  # Slippage en porcentaje
                
                return 0
        except (ExchangeError, NetworkError) as e:
            self.logger.error(f"Error al estimar slippage para {symbol} en Binance: {str(e)}")
            raise
    
    def _convert_order_status(self, ccxt_status: str) -> OrderStatus:
        """
        Convierte el estado de orden de CCXT a nuestro modelo.
        
        Args:
            ccxt_status: Estado de orden según CCXT.
            
        Returns:
            OrderStatus: Estado de orden según nuestro modelo.
        """
        status_map = {
            'open': OrderStatus.OPEN,
            'closed': OrderStatus.FILLED,
            'canceled': OrderStatus.CANCELED,
            'expired': OrderStatus.CANCELED,
            'rejected': OrderStatus.REJECTED,
            'partial': OrderStatus.PARTIALLY_FILLED
        }
        
        return status_map.get(ccxt_status, OrderStatus.OPEN)

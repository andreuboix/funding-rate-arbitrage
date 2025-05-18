"""
Implementación específica para el exchange Bybit.
"""
import asyncio
import logging
from datetime import datetime
from typing import Dict, List, Optional, Tuple

import ccxt.async_support as ccxt
from ccxt.base.errors import ExchangeError, NetworkError

from src.exchanges.base_exchange import BaseExchange
from src.models.data_models import FundingRateInfo, Order, OrderSide, OrderStatus, OrderType, Position


class BybitExchange(BaseExchange):
    """Clase para interactuar con el exchange Bybit."""
    
    def __init__(self, api_key: str, api_secret: str):
        """
        Inicializa la conexión con Bybit.
        
        Args:
            api_key: Clave API para autenticación.
            api_secret: Secreto API para autenticación.
        """
        super().__init__('bybit', api_key, api_secret)
        
        # Configuraciones específicas para Bybit
        self.client.options['defaultType'] = 'swap'
    
    async def get_funding_rate(self, symbol: str) -> FundingRateInfo:
        """
        Obtiene la tasa de financiamiento actual para un símbolo en Bybit.
        
        Args:
            symbol: Símbolo del contrato perpetuo (ej. 'BTC-PERP').
            
        Returns:
            FundingRateInfo: Información de la tasa de financiamiento.
        """
        try:
            # Obtener información de financiamiento
            funding_info = await self.client.public_get_derivatives_v3_public_tickers_funding_rate({
                'category': 'linear',
                'symbol': self.client.market_id(symbol)
            })
            
            result = funding_info['result']['list'][0]
            
            # Obtener precios mark e index
            mark_price = await self.get_mark_price(symbol)
            index_price = await self.get_index_price(symbol)
            
            # Calcular próximo tiempo de financiamiento
            next_funding_time = datetime.fromtimestamp(int(result['nextFundingTime']) / 1000)
            
            # Crear objeto de respuesta
            return FundingRateInfo(
                exchange="BYBIT",
                symbol=symbol,
                funding_rate=float(result['fundingRate']) * 100,  # Convertir a porcentaje
                next_funding_time=next_funding_time,
                mark_price=mark_price,
                index_price=index_price,
                timestamp=datetime.now()
            )
        except (ExchangeError, NetworkError) as e:
            self.logger.error(f"Error al obtener funding rate para {symbol} en Bybit: {str(e)}")
            raise
    
    async def get_mark_price(self, symbol: str) -> float:
        """
        Obtiene el precio mark actual para un símbolo en Bybit.
        
        Args:
            symbol: Símbolo del contrato perpetuo.
            
        Returns:
            float: Precio mark actual.
        """
        try:
            tickers = await self.client.public_get_derivatives_v3_public_tickers({
                'category': 'linear',
                'symbol': self.client.market_id(symbol)
            })
            
            return float(tickers['result']['list'][0]['markPrice'])
        except (ExchangeError, NetworkError) as e:
            self.logger.error(f"Error al obtener mark price para {symbol} en Bybit: {str(e)}")
            raise
    
    async def get_index_price(self, symbol: str) -> float:
        """
        Obtiene el precio index actual para un símbolo en Bybit.
        
        Args:
            symbol: Símbolo del contrato perpetuo.
            
        Returns:
            float: Precio index actual.
        """
        try:
            tickers = await self.client.public_get_derivatives_v3_public_tickers({
                'category': 'linear',
                'symbol': self.client.market_id(symbol)
            })
            
            return float(tickers['result']['list'][0]['indexPrice'])
        except (ExchangeError, NetworkError) as e:
            self.logger.error(f"Error al obtener index price para {symbol} en Bybit: {str(e)}")
            raise
    
    async def get_orderbook(self, symbol: str, limit: int = 20) -> Dict:
        """
        Obtiene el libro de órdenes para un símbolo en Bybit.
        
        Args:
            symbol: Símbolo del contrato perpetuo.
            limit: Número de niveles a obtener.
            
        Returns:
            Dict: Libro de órdenes con bids y asks.
        """
        try:
            return await self.client.fetch_order_book(symbol, limit)
        except (ExchangeError, NetworkError) as e:
            self.logger.error(f"Error al obtener orderbook para {symbol} en Bybit: {str(e)}")
            raise
    
    async def create_order(self, symbol: str, order_type: OrderType, side: OrderSide, 
                          amount: float, price: Optional[float] = None) -> Order:
        """
        Crea una orden en Bybit.
        
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
            params = {
                'category': 'linear',
            }
            
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
                exchange="BYBIT",
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
            self.logger.error(f"Error al crear orden para {symbol} en Bybit: {str(e)}")
            raise
    
    async def get_order(self, symbol: str, order_id: str) -> Order:
        """
        Obtiene información de una orden específica en Bybit.
        
        Args:
            symbol: Símbolo del contrato perpetuo.
            order_id: ID de la orden.
            
        Returns:
            Order: Información actualizada de la orden.
        """
        try:
            params = {'category': 'linear'}
            ccxt_order = await self.client.fetch_order(order_id, symbol, params)
            
            return Order(
                exchange="BYBIT",
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
            self.logger.error(f"Error al obtener orden {order_id} para {symbol} en Bybit: {str(e)}")
            raise
    
    async def cancel_order(self, symbol: str, order_id: str) -> bool:
        """
        Cancela una orden específica en Bybit.
        
        Args:
            symbol: Símbolo del contrato perpetuo.
            order_id: ID de la orden.
            
        Returns:
            bool: True si se canceló correctamente, False en caso contrario.
        """
        try:
            params = {'category': 'linear'}
            await self.client.cancel_order(order_id, symbol, params)
            return True
        except (ExchangeError, NetworkError) as e:
            self.logger.error(f"Error al cancelar orden {order_id} para {symbol} en Bybit: {str(e)}")
            return False
    
    async def get_position(self, symbol: str) -> Optional[Position]:
        """
        Obtiene la posición actual para un símbolo en Bybit.
        
        Args:
            symbol: Símbolo del contrato perpetuo.
            
        Returns:
            Optional[Position]: Información de la posición o None si no hay posición.
        """
        try:
            params = {
                'category': 'linear',
                'symbol': self.client.market_id(symbol)
            }
            
            positions = await self.client.private_get_position_v5_list(params)
            position_list = positions['result']['list']
            
            if not position_list or float(position_list[0]['size']) == 0:
                return None
            
            position_data = position_list[0]
            side = OrderSide.BUY if position_data['side'] == 'Buy' else OrderSide.SELL
            amount = float(position_data['size'])
            entry_price = float(position_data['entryPrice'])
            current_price = float(position_data['markPrice'])
            unrealized_pnl = float(position_data['unrealisedPnl'])
            
            return Position(
                exchange="BYBIT",
                symbol=symbol,
                side=side,
                amount=amount,
                entry_price=entry_price,
                current_price=current_price,
                unrealized_pnl=unrealized_pnl,
                realized_pnl=float(position_data.get('cumRealisedPnl', 0)),
                open_time=datetime.now(),  # Bybit no proporciona tiempo de apertura directamente
                last_update_time=datetime.now()
            )
        except (ExchangeError, NetworkError) as e:
            self.logger.error(f"Error al obtener posición para {symbol} en Bybit: {str(e)}")
            raise
    
    async def get_balance(self) -> Dict[str, float]:
        """
        Obtiene el balance de la cuenta en Bybit.
        
        Returns:
            Dict[str, float]: Balance por moneda.
        """
        try:
            params = {'accountType': 'CONTRACT'}
            balance = await self.client.fetch_balance(params)
            return {currency: float(data['total']) for currency, data in balance['total'].items() if float(data['total']) > 0}
        except (ExchangeError, NetworkError) as e:
            self.logger.error(f"Error al obtener balance en Bybit: {str(e)}")
            raise
    
    async def estimate_slippage(self, symbol: str, side: OrderSide, amount: float) -> float:
        """
        Estima el slippage para una operación de mercado en Bybit.
        
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
            self.logger.error(f"Error al estimar slippage para {symbol} en Bybit: {str(e)}")
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

"""
Módulo para simulación y backtesting de la estrategia de arbitraje.
"""
import asyncio
import json
import logging
import os
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple

import pandas as pd
import numpy as np
from loguru import logger

from src.models.data_models import (
    ArbitrageOpportunity, ArbitragePosition, FundingRateInfo,
    Order, OrderSide, OrderStatus, OrderType, Position
)
from src.execution.arbitrage_calculator import ArbitrageCalculator
from src.risk.risk_manager import RiskManager


class BacktestExchange:
    """Simulador de exchange para backtesting."""
    
    def __init__(self, exchange_id: str, historical_data: Dict, fee_rate: float = 0.1):
        """
        Inicializa el simulador de exchange.
        
        Args:
            exchange_id: Identificador del exchange.
            historical_data: Datos históricos para simulación.
            fee_rate: Tasa de comisión en porcentaje.
        """
        self.exchange_id = exchange_id
        self.historical_data = historical_data
        self.fee_rate = fee_rate / 100  # Convertir a decimal
        self.current_time = None
        self.positions = {}
        self.orders = {}
        self.logger = logging.getLogger(f"backtest.{exchange_id}")
    
    def set_current_time(self, timestamp: datetime) -> None:
        """
        Establece el tiempo actual para la simulación.
        
        Args:
            timestamp: Timestamp a establecer.
        """
        self.current_time = timestamp
    
    async def get_funding_rate(self, symbol: str) -> FundingRateInfo:
        """
        Obtiene la tasa de financiamiento simulada para un símbolo.
        
        Args:
            symbol: Símbolo del contrato perpetuo.
            
        Returns:
            FundingRateInfo: Información de la tasa de financiamiento.
        """
        if symbol not in self.historical_data:
            raise ValueError(f"No hay datos históricos para {symbol} en {self.exchange_id}")
        
        # Buscar el registro más cercano al tiempo actual
        df = self.historical_data[symbol]
        closest_idx = df.index.get_indexer([self.current_time], method='nearest')[0]
        record = df.iloc[closest_idx]
        
        # Calcular próximo tiempo de financiamiento (cada 8 horas)
        current_hour = self.current_time.hour
        hours_to_next = (8 - (current_hour % 8)) % 8
        next_funding_time = self.current_time.replace(minute=0, second=0, microsecond=0) + timedelta(hours=hours_to_next)
        
        return FundingRateInfo(
            exchange=self.exchange_id,
            symbol=symbol,
            funding_rate=float(record['funding_rate']),
            next_funding_time=next_funding_time,
            mark_price=float(record['mark_price']),
            index_price=float(record['index_price']),
            timestamp=self.current_time
        )
    
    async def get_mark_price(self, symbol: str) -> float:
        """
        Obtiene el precio mark simulado para un símbolo.
        
        Args:
            symbol: Símbolo del contrato perpetuo.
            
        Returns:
            float: Precio mark actual.
        """
        if symbol not in self.historical_data:
            raise ValueError(f"No hay datos históricos para {symbol} en {self.exchange_id}")
        
        df = self.historical_data[symbol]
        closest_idx = df.index.get_indexer([self.current_time], method='nearest')[0]
        return float(df.iloc[closest_idx]['mark_price'])
    
    async def get_index_price(self, symbol: str) -> float:
        """
        Obtiene el precio index simulado para un símbolo.
        
        Args:
            symbol: Símbolo del contrato perpetuo.
            
        Returns:
            float: Precio index actual.
        """
        if symbol not in self.historical_data:
            raise ValueError(f"No hay datos históricos para {symbol} en {self.exchange_id}")
        
        df = self.historical_data[symbol]
        closest_idx = df.index.get_indexer([self.current_time], method='nearest')[0]
        return float(df.iloc[closest_idx]['index_price'])
    
    async def get_orderbook(self, symbol: str, limit: int = 20) -> Dict:
        """
        Obtiene un libro de órdenes simulado para un símbolo.
        
        Args:
            symbol: Símbolo del contrato perpetuo.
            limit: Número de niveles a obtener.
            
        Returns:
            Dict: Libro de órdenes con bids y asks.
        """
        mark_price = await self.get_mark_price(symbol)
        
        # Simular un libro de órdenes con spread del 0.1%
        spread = mark_price * 0.001
        best_bid = mark_price - spread / 2
        best_ask = mark_price + spread / 2
        
        # Crear niveles con profundidad decreciente
        bids = []
        asks = []
        
        for i in range(limit):
            price_offset = i * 0.0005 * mark_price
            size = 10 / (i + 1)  # Tamaño decreciente
            
            bids.append([best_bid - price_offset, size])
            asks.append([best_ask + price_offset, size])
        
        return {
            'bids': bids,
            'asks': asks,
            'timestamp': self.current_time.timestamp() * 1000,
            'datetime': self.current_time.isoformat(),
            'nonce': int(self.current_time.timestamp() * 1000)
        }
    
    async def create_order(self, symbol: str, order_type: OrderType, side: OrderSide, 
                          amount: float, price: Optional[float] = None) -> Order:
        """
        Crea una orden simulada en el exchange.
        
        Args:
            symbol: Símbolo del contrato perpetuo.
            order_type: Tipo de orden (market, limit).
            side: Lado de la orden (buy, sell).
            amount: Cantidad a comprar/vender.
            price: Precio para órdenes límite.
            
        Returns:
            Order: Información de la orden creada.
        """
        # Generar ID de orden
        order_id = f"{self.exchange_id}-{symbol}-{self.current_time.timestamp()}-{side.value}"
        
        # Obtener precio de ejecución
        execution_price = price if order_type == OrderType.LIMIT else await self.get_mark_price(symbol)
        
        # Simular slippage para órdenes de mercado (0.05%)
        if order_type == OrderType.MARKET:
            slippage = 0.0005
            if side == OrderSide.BUY:
                execution_price *= (1 + slippage)
            else:
                execution_price *= (1 - slippage)
        
        # Crear orden
        order = Order(
            exchange=self.exchange_id,
            symbol=symbol,
            order_id=order_id,
            client_order_id=None,
            side=side,
            type=order_type,
            price=execution_price,
            amount=amount,
            status=OrderStatus.FILLED,  # Simular ejecución inmediata
            filled_amount=amount,
            average_fill_price=execution_price,
            timestamp=self.current_time
        )
        
        # Guardar orden
        self.orders[order_id] = order
        
        # Actualizar posición
        self._update_position(symbol, side, amount, execution_price)
        
        return order
    
    async def get_order(self, symbol: str, order_id: str) -> Order:
        """
        Obtiene información de una orden simulada.
        
        Args:
            symbol: Símbolo del contrato perpetuo.
            order_id: ID de la orden.
            
        Returns:
            Order: Información de la orden.
        """
        if order_id not in self.orders:
            raise ValueError(f"Orden {order_id} no encontrada")
        
        return self.orders[order_id]
    
    async def cancel_order(self, symbol: str, order_id: str) -> bool:
        """
        Cancela una orden simulada.
        
        Args:
            symbol: Símbolo del contrato perpetuo.
            order_id: ID de la orden.
            
        Returns:
            bool: True si se canceló correctamente, False en caso contrario.
        """
        if order_id not in self.orders:
            return False
        
        order = self.orders[order_id]
        
        if order.status in [OrderStatus.FILLED, OrderStatus.CANCELED]:
            return False
        
        order.status = OrderStatus.CANCELED
        return True
    
    async def get_position(self, symbol: str) -> Optional[Position]:
        """
        Obtiene la posición simulada para un símbolo.
        
        Args:
            symbol: Símbolo del contrato perpetuo.
            
        Returns:
            Optional[Position]: Información de la posición o None si no hay posición.
        """
        if symbol not in self.positions:
            return None
        
        position = self.positions[symbol]
        
        # Actualizar precio actual
        current_price = await self.get_mark_price(symbol)
        
        # Calcular P&L no realizado
        if position.side == OrderSide.BUY:
            unrealized_pnl = position.amount * (current_price - position.entry_price)
        else:
            unrealized_pnl = position.amount * (position.entry_price - current_price)
        
        # Actualizar posición
        position.current_price = current_price
        position.unrealized_pnl = unrealized_pnl
        position.last_update_time = self.current_time
        
        return position
    
    async def get_balance(self) -> Dict[str, float]:
        """
        Obtiene el balance simulado de la cuenta.
        
        Returns:
            Dict[str, float]: Balance por moneda.
        """
        # Simular balance suficiente para operar
        return {
            'USDT': 100000.0,
            'USD': 100000.0,
            'BTC': 1.0,
            'ETH': 10.0
        }
    
    async def estimate_slippage(self, symbol: str, side: OrderSide, amount: float) -> float:
        """
        Estima el slippage para una operación de mercado simulada.
        
        Args:
            symbol: Símbolo del contrato perpetuo.
            side: Lado de la orden (buy, sell).
            amount: Cantidad a comprar/vender.
            
        Returns:
            float: Slippage estimado en porcentaje.
        """
        # Simular slippage basado en el tamaño de la orden
        base_slippage = 0.05  # 0.05% base
        size_factor = min(amount / 10, 1.0)  # Factor por tamaño (máximo 1.0)
        
        return base_slippage * (1 + size_factor)
    
    def _update_position(self, symbol: str, side: OrderSide, amount: float, price: float) -> None:
        """
        Actualiza la posición simulada después de una ejecución de orden.
        
        Args:
            symbol: Símbolo del contrato perpetuo.
            side: Lado de la orden (buy, sell).
            amount: Cantidad ejecutada.
            price: Precio de ejecución.
        """
        if symbol not in self.positions:
            # Crear nueva posición
            self.positions[symbol] = Position(
                exchange=self.exchange_id,
                symbol=symbol,
                side=side,
                amount=amount,
                entry_price=price,
                current_price=price,
                unrealized_pnl=0.0,
                realized_pnl=0.0,
                open_time=self.current_time,
                last_update_time=self.current_time
            )
            return
        
        position = self.positions[symbol]
        
        if position.side == side:
            # Aumentar posición existente
            new_amount = position.amount + amount
            new_entry_price = (position.entry_price * position.amount + price * amount) / new_amount
            
            position.amount = new_amount
            position.entry_price = new_entry_price
            position.current_price = price
            position.last_update_time = self.current_time
        else:
            # Reducir o cerrar posición existente
            if amount < position.amount:
                # Reducir posición
                realized_pnl = 0.0
                
                if position.side == OrderSide.BUY:
                    realized_pnl = amount * (price - position.entry_price)
                else:
                    realized_pnl = amount * (position.entry_price - price)
                
                position.amount -= amount
                position.realized_pnl += realized_pnl
                position.current_price = price
                position.last_update_time = self.current_time
            elif amount == position.amount:
                # Cerrar posición
                realized_pnl = 0.0
                
                if position.side == OrderSide.BUY:
                    realized_pnl = amount * (price - position.entry_price)
                else:
                    realized_pnl = amount * (position.entry_price - price)
                
                position.amount = 0
                position.realized_pnl += realized_pnl
                position.current_price = price
                position.last_update_time = self.current_time
                
                # Eliminar posición cerrada
                del self.positions[symbol]
            else:
                # Cerrar posición existente y abrir nueva en dirección opuesta
                realized_pnl = 0.0
                
                if position.side == OrderSide.BUY:
                    realized_pnl = position.amount * (price - position.entry_price)
                else:
                    realized_pnl = position.amount * (position.entry_price - price)
                
                # Crear nueva posición en dirección opuesta
                new_amount = amount - position.amount
                
                self.positions[symbol] = Position(
                    exchange=self.exchange_id,
                    symbol=symbol,
                    side=side,
                    amount=new_amount,
                    entry_price=price,
                    current_price=price,
                    unrealized_pnl=0.0,
                    realized_pnl=realized_pnl,
                    open_time=self.current_time,
                    last_update_time=self.current_time
                )


class BacktestEngine:
    """Motor de backtesting para la estrategia de arbitraje de funding rate."""
    
    def __init__(
        self,
        start_date: datetime,
        end_date: datetime,
        exchanges_data: Dict[str, Dict[str, pd.DataFrame]],
        min_funding_rate_diff: float,
        max_position_size: float,
        exit_funding_rate_diff: float,
        max_position_holding_time: int,
        fee_rates: Dict[str, float] = None
    ):
        """
        Inicializa el motor de backtesting.
        
        Args:
            start_date: Fecha de inicio del backtest.
            end_date: Fecha de fin del backtest.
            exchanges_data: Datos históricos por exchange y símbolo.
            min_funding_rate_diff: Umbral mínimo de diferencial de funding rate.
            max_position_size: Tamaño máximo de posición por exchange.
            exit_funding_rate_diff: Umbral de diferencial para cerrar posición.
            max_position_holding_time: Tiempo máximo de mantenimiento de posición (horas).
            fee_rates: Tasas de comisión por exchange.
        """
        self.start_date = start_date
        self.end_date = end_date
        self.exchanges_data = exchanges_data
        self.min_funding_rate_diff = min_funding_rate_diff
        self.max_position_size = max_position_size
        self.exit_funding_rate_diff = exit_funding_rate_diff
        self.max_position_holding_time = max_position_holding_time
        
        # Configurar tasas de comisión
        self.fee_rates = fee_rates or {exchange_id: 0.1 for exchange_id in exchanges_data.keys()}
        
        # Inicializar componentes
        self.exchanges = {}
        for exchange_id, data in exchanges_data.items():
            self.exchanges[exchange_id] = BacktestExchange(
                exchange_id=exchange_id,
                historical_data=data,
                fee_rate=self.fee_rates.get(exchange_id, 0.1)
            )
        
        self.calculator = ArbitrageCalculator(min_funding_rate_diff)
        
        self.risk_manager = RiskManager(
            max_position_size=max_position_size,
            max_daily_drawdown=max_position_size * 0.1,  # 10% del tamaño máximo
            max_positions=10
        )
        
        # Resultados del backtest
        self.current_time = start_date
        self.positions_history = []
        self.trades_history = []
        self.funding_rates_history = []
        self.equity_curve = []
        self.active_positions = {}
        
        self.logger = logging.getLogger("backtest")
    
    async def run(self, time_step: timedelta = timedelta(hours=1)) -> Dict:
        """
        Ejecuta el backtest.
        
        Args:
            time_step: Intervalo de tiempo entre iteraciones.
            
        Returns:
            Dict: Resultados del backtest.
        """
        self.logger.info(f"Iniciando backtest desde {self.start_date} hasta {self.end_date}")
        
        current_time = self.start_date
        initial_equity = 10000.0  # Capital inicial
        current_equity = initial_equity
        
        # Registrar punto inicial en la curva de equity
        self.equity_curve.append({
            'timestamp': current_time,
            'equity': current_equity,
            'drawdown': 0.0,
            'active_positions': 0
        })
        
        # Bucle principal del backtest
        while current_time <= self.end_date:
            self.current_time = current_time
            
            # Actualizar tiempo en todos los exchanges
            for exchange in self.exchanges.values():
                exchange.set_current_time(current_time)
            
            # Actualizar tasas de funding
            await self._update_funding_rates()
            
            # Actualizar posiciones existentes
            await self._update_positions()
            
            # Verificar condiciones de salida
            await self._check_exit_conditions()
            
            # Buscar nuevas oportunidades
            await self._find_and_execute_opportunities()
            
            # Calcular equity actual
            current_equity = initial_equity
            
            for position_id, position in self.active_positions.items():
                current_equity += position.total_pnl
            
            # Registrar punto en la curva de equity
            max_equity = max([point['equity'] for point in self.equity_curve])
            drawdown = max(0, max_equity - current_equity)
            
            self.equity_curve.append({
                'timestamp': current_time,
                'equity': current_equity,
                'drawdown': drawdown,
                'active_positions': len(self.active_positions)
            })
            
            # Avanzar al siguiente paso de tiempo
            current_time += time_step
        
        # Cerrar posiciones abiertas al final del backtest
        for position_id in list(self.active_positions.keys()):
            await self._close_position(position_id)
        
        # Calcular métricas finales
        results = self._calculate_metrics()
        
        self.logger.info(f"Backtest completado. Rendimiento total: {results['total_return']:.2f}%")
        
        return results
    
    async def _update_funding_rates(self) -> None:
        """Actualiza las tasas de funding para todos los pares."""
        funding_rates = []
        
        for exchange_id, exchange in self.exchanges.items():
            for symbol in self.exchanges_data[exchange_id].keys():
                try:
                    rate_info = await exchange.get_funding_rate(symbol)
                    funding_rates.append(rate_info)
                    
                    # Registrar en historial
                    self.funding_rates_history.append({
                        'timestamp': self.current_time,
                        'exchange': exchange_id,
                        'symbol': symbol,
                        'funding_rate': rate_info.funding_rate,
                        'mark_price': rate_info.mark_price,
                        'index_price': rate_info.index_price
                    })
                except Exception as e:
                    self.logger.error(f"Error al obtener funding rate para {symbol} en {exchange_id}: {str(e)}")
    
    async def _update_positions(self) -> None:
        """Actualiza el estado de las posiciones activas."""
        for position_id, position in list(self.active_positions.items()):
            try:
                # Obtener exchanges
                long_exchange_id = position.long_position.exchange
                short_exchange_id = position.short_position.exchange
                
                if long_exchange_id not in self.exchanges or short_exchange_id not in self.exchanges:
                    continue
                
                long_exchange = self.exchanges[long_exchange_id]
                short_exchange = self.exchanges[short_exchange_id]
                
                # Actualizar posiciones
                long_position = await long_exchange.get_position(position.long_position.symbol)
                short_position = await short_exchange.get_position(position.short_position.symbol)
                
                if not long_position or not short_position:
                    # Una de las posiciones ya se cerró
                    if position_id in self.active_positions:
                        del self.active_positions[position_id]
                    continue
                
                # Actualizar posición de arbitraje
                position.long_position = long_position
                position.short_position = short_position
                position.last_update_time = self.current_time
                
                # Actualizar diferencial de funding rate actual
                long_rate_info = await long_exchange.get_funding_rate(position.long_position.symbol)
                short_rate_info = await short_exchange.get_funding_rate(position.short_position.symbol)
                
                position.current_funding_rate_diff = short_rate_info.funding_rate - long_rate_info.funding_rate
            
            except Exception as e:
                self.logger.error(f"Error al actualizar posición {position_id}: {str(e)}")
    
    async def _check_exit_conditions(self) -> None:
        """Verifica condiciones de salida para posiciones activas."""
        for position_id in list(self.active_positions.keys()):
            try:
                position = self.active_positions[position_id]
                
                # Verificar tiempo máximo de mantenimiento
                holding_time = (self.current_time - position.open_time).total_seconds() / 3600  # en horas
                
                if holding_time >= self.max_position_holding_time:
                    self.logger.info(f"Cerrando posición {position_id} por tiempo máximo de mantenimiento")
                    await self._close_position(position_id)
                    continue
                
                # Verificar diferencial de funding rate
                if position.current_funding_rate_diff < self.exit_funding_rate_diff:
                    self.logger.info(f"Cerrando posición {position_id} por diferencial de funding rate bajo")
                    await self._close_position(position_id)
                    continue
                
            except Exception as e:
                self.logger.error(f"Error al verificar condiciones de salida para posición {position_id}: {str(e)}")
    
    async def _find_and_execute_opportunities(self) -> None:
        """Busca y ejecuta oportunidades de arbitraje."""
        try:
            # Recopilar tasas de funding actuales
            funding_rates = []
            
            for exchange_id, exchange in self.exchanges.items():
                for symbol in self.exchanges_data[exchange_id].keys():
                    try:
                        rate_info = await exchange.get_funding_rate(symbol)
                        funding_rates.append(rate_info)
                    except Exception as e:
                        self.logger.error(f"Error al obtener funding rate para {symbol} en {exchange_id}: {str(e)}")
            
            # Calcular oportunidades
            opportunities = self.calculator.calculate_opportunities(funding_rates)
            
            # Ejecutar oportunidades viables
            for opportunity in opportunities:
                # Verificar si ya tenemos una posición similar
                if self._has_similar_position(opportunity):
                    continue
                
                # Calcular P&L teórico
                position_size = self.risk_manager.calculate_position_size(opportunity)
                
                if position_size <= 0:
                    continue
                
                # Ejecutar oportunidad
                await self._execute_opportunity(opportunity, position_size)
                
        except Exception as e:
            self.logger.error(f"Error al buscar oportunidades: {str(e)}")
    
    async def _execute_opportunity(self, opportunity: ArbitrageOpportunity, position_size: float) -> Optional[str]:
        """
        Ejecuta una oportunidad de arbitraje.
        
        Args:
            opportunity: Oportunidad de arbitraje a ejecutar.
            position_size: Tamaño de posición en USD.
            
        Returns:
            Optional[str]: ID de la posición de arbitraje creada, o None si falló.
        """
        try:
            # Obtener exchanges
            long_exchange_id = opportunity.long_exchange
            short_exchange_id = opportunity.short_exchange
            
            if long_exchange_id not in self.exchanges or short_exchange_id not in self.exchanges:
                self.logger.error(f"Exchange no disponible: {long_exchange_id} o {short_exchange_id}")
                return None
            
            long_exchange = self.exchanges[long_exchange_id]
            short_exchange = self.exchanges[short_exchange_id]
            
            # Obtener precios actuales
            long_price = await long_exchange.get_mark_price(opportunity.long_symbol)
            short_price = await short_exchange.get_mark_price(opportunity.short_symbol)
            
            # Calcular cantidades
            long_amount = position_size / long_price
            short_amount = position_size / short_price
            
            # Ejecutar órdenes
            long_order = await long_exchange.create_order(
                symbol=opportunity.long_symbol,
                order_type=OrderType.MARKET,
                side=OrderSide.BUY,
                amount=long_amount
            )
            
            short_order = await short_exchange.create_order(
                symbol=opportunity.short_symbol,
                order_type=OrderType.MARKET,
                side=OrderSide.SELL,
                amount=short_amount
            )
            
            # Registrar trades
            self.trades_history.append({
                'timestamp': self.current_time,
                'exchange': long_exchange_id,
                'symbol': opportunity.long_symbol,
                'side': 'buy',
                'amount': long_amount,
                'price': long_order.average_fill_price,
                'value': long_amount * long_order.average_fill_price
            })
            
            self.trades_history.append({
                'timestamp': self.current_time,
                'exchange': short_exchange_id,
                'symbol': opportunity.short_symbol,
                'side': 'sell',
                'amount': short_amount,
                'price': short_order.average_fill_price,
                'value': short_amount * short_order.average_fill_price
            })
            
            # Obtener posiciones
            long_position = await long_exchange.get_position(opportunity.long_symbol)
            short_position = await short_exchange.get_position(opportunity.short_symbol)
            
            if not long_position or not short_position:
                self.logger.error(f"Error: no se pudieron obtener las posiciones después de la ejecución")
                return None
            
            # Crear posición de arbitraje
            position_id = f"backtest-{self.current_time.timestamp()}-{long_exchange_id}-{short_exchange_id}"
            
            arbitrage_position = ArbitragePosition(
                id=position_id,
                long_position=long_position,
                short_position=short_position,
                funding_rate_diff_at_entry=opportunity.funding_rate_diff,
                current_funding_rate_diff=opportunity.funding_rate_diff,
                open_time=self.current_time,
                last_update_time=self.current_time
            )
            
            # Registrar posición
            self.active_positions[position_id] = arbitrage_position
            
            # Registrar en historial
            self.positions_history.append({
                'position_id': position_id,
                'open_time': self.current_time,
                'long_exchange': long_exchange_id,
                'long_symbol': opportunity.long_symbol,
                'long_amount': long_amount,
                'long_price': long_order.average_fill_price,
                'short_exchange': short_exchange_id,
                'short_symbol': opportunity.short_symbol,
                'short_amount': short_amount,
                'short_price': short_order.average_fill_price,
                'funding_rate_diff': opportunity.funding_rate_diff,
                'status': 'open'
            })
            
            self.logger.info(f"Posición de arbitraje abierta: {position_id}, "
                           f"{opportunity.long_identifier} (long) vs {opportunity.short_identifier} (short), "
                           f"tamaño: ${position_size:.2f}, diferencial: {opportunity.funding_rate_diff:.4f}%")
            
            return position_id
            
        except Exception as e:
            self.logger.error(f"Error al ejecutar oportunidad: {str(e)}")
            return None
    
    async def _close_position(self, position_id: str) -> bool:
        """
        Cierra una posición de arbitraje.
        
        Args:
            position_id: ID de la posición a cerrar.
            
        Returns:
            bool: True si se cerró correctamente, False en caso contrario.
        """
        if position_id not in self.active_positions:
            self.logger.warning(f"Posición {position_id} no encontrada")
            return False
        
        position = self.active_positions[position_id]
        
        try:
            # Obtener exchanges
            long_exchange_id = position.long_position.exchange
            short_exchange_id = position.short_position.exchange
            
            if long_exchange_id not in self.exchanges or short_exchange_id not in self.exchanges:
                self.logger.error(f"Exchange no disponible: {long_exchange_id} o {short_exchange_id}")
                return False
            
            long_exchange = self.exchanges[long_exchange_id]
            short_exchange = self.exchanges[short_exchange_id]
            
            # Cerrar posición long (vender)
            long_order = await long_exchange.create_order(
                symbol=position.long_position.symbol,
                order_type=OrderType.MARKET,
                side=OrderSide.SELL,
                amount=position.long_position.amount
            )
            
            # Cerrar posición short (comprar)
            short_order = await short_exchange.create_order(
                symbol=position.short_position.symbol,
                order_type=OrderType.MARKET,
                side=OrderSide.BUY,
                amount=position.short_position.amount
            )
            
            # Registrar trades
            self.trades_history.append({
                'timestamp': self.current_time,
                'exchange': long_exchange_id,
                'symbol': position.long_position.symbol,
                'side': 'sell',
                'amount': position.long_position.amount,
                'price': long_order.average_fill_price,
                'value': position.long_position.amount * long_order.average_fill_price
            })
            
            self.trades_history.append({
                'timestamp': self.current_time,
                'exchange': short_exchange_id,
                'symbol': position.short_position.symbol,
                'side': 'buy',
                'amount': position.short_position.amount,
                'price': short_order.average_fill_price,
                'value': position.short_position.amount * short_order.average_fill_price
            })
            
            # Calcular P&L total
            total_pnl = position.total_pnl
            
            # Actualizar historial de posiciones
            for pos in self.positions_history:
                if pos['position_id'] == position_id and pos['status'] == 'open':
                    pos['close_time'] = self.current_time
                    pos['holding_time_hours'] = (self.current_time - pos['open_time']).total_seconds() / 3600
                    pos['pnl'] = total_pnl
                    pos['status'] = 'closed'
                    break
            
            # Eliminar posición de activas
            del self.active_positions[position_id]
            
            self.logger.info(f"Posición {position_id} cerrada, P&L total: ${total_pnl:.2f}")
            
            return True
            
        except Exception as e:
            self.logger.error(f"Error al cerrar posición {position_id}: {str(e)}")
            return False
    
    def _has_similar_position(self, opportunity: ArbitrageOpportunity) -> bool:
        """
        Verifica si ya existe una posición similar a la oportunidad.
        
        Args:
            opportunity: Oportunidad a verificar.
            
        Returns:
            bool: True si existe una posición similar, False en caso contrario.
        """
        for position in self.active_positions.values():
            if (position.long_position.exchange == opportunity.long_exchange and
                position.long_position.symbol == opportunity.long_symbol and
                position.short_position.exchange == opportunity.short_exchange and
                position.short_position.symbol == opportunity.short_symbol):
                return True
        
        return False
    
    def _calculate_metrics(self) -> Dict:
        """
        Calcula métricas finales del backtest.
        
        Returns:
            Dict: Métricas del backtest.
        """
        if not self.equity_curve:
            return {
                'total_return': 0.0,
                'annualized_return': 0.0,
                'max_drawdown': 0.0,
                'sharpe_ratio': 0.0,
                'total_trades': 0,
                'winning_trades': 0,
                'losing_trades': 0,
                'win_rate': 0.0,
                'avg_profit': 0.0,
                'avg_loss': 0.0,
                'profit_factor': 0.0,
                'max_consecutive_wins': 0,
                'max_consecutive_losses': 0
            }
        
        # Calcular retorno total
        initial_equity = self.equity_curve[0]['equity']
        final_equity = self.equity_curve[-1]['equity']
        total_return = (final_equity / initial_equity - 1) * 100
        
        # Calcular retorno anualizado
        days = (self.end_date - self.start_date).days
        if days > 0:
            annualized_return = ((1 + total_return / 100) ** (365 / days) - 1) * 100
        else:
            annualized_return = 0.0
        
        # Calcular máximo drawdown
        max_drawdown = max([point['drawdown'] for point in self.equity_curve], default=0.0)
        max_drawdown_pct = (max_drawdown / initial_equity) * 100
        
        # Calcular retornos diarios
        daily_returns = []
        equity_df = pd.DataFrame(self.equity_curve)
        equity_df['timestamp'] = pd.to_datetime(equity_df['timestamp'])
        equity_df.set_index('timestamp', inplace=True)
        
        daily_equity = equity_df.resample('D').last()
        daily_equity = daily_equity.dropna()
        
        if len(daily_equity) > 1:
            daily_returns = daily_equity['equity'].pct_change().dropna().tolist()
        
        # Calcular Sharpe ratio
        if daily_returns and len(daily_returns) > 1:
            avg_return = sum(daily_returns) / len(daily_returns)
            std_return = np.std(daily_returns)
            sharpe_ratio = (avg_return / std_return) * np.sqrt(252) if std_return > 0 else 0.0
        else:
            sharpe_ratio = 0.0
        
        # Calcular estadísticas de trades
        closed_positions = [pos for pos in self.positions_history if pos.get('status') == 'closed']
        total_trades = len(closed_positions)
        
        if total_trades > 0:
            winning_trades = len([pos for pos in closed_positions if pos.get('pnl', 0) > 0])
            losing_trades = len([pos for pos in closed_positions if pos.get('pnl', 0) <= 0])
            
            win_rate = (winning_trades / total_trades) * 100 if total_trades > 0 else 0.0
            
            profits = [pos.get('pnl', 0) for pos in closed_positions if pos.get('pnl', 0) > 0]
            losses = [abs(pos.get('pnl', 0)) for pos in closed_positions if pos.get('pnl', 0) <= 0]
            
            avg_profit = sum(profits) / len(profits) if profits else 0.0
            avg_loss = sum(losses) / len(losses) if losses else 0.0
            
            profit_factor = sum(profits) / sum(losses) if sum(losses) > 0 else float('inf')
            
            # Calcular rachas
            results = [1 if pos.get('pnl', 0) > 0 else -1 for pos in closed_positions]
            
            max_consecutive_wins = 0
            max_consecutive_losses = 0
            current_wins = 0
            current_losses = 0
            
            for result in results:
                if result > 0:
                    current_wins += 1
                    current_losses = 0
                    max_consecutive_wins = max(max_consecutive_wins, current_wins)
                else:
                    current_losses += 1
                    current_wins = 0
                    max_consecutive_losses = max(max_consecutive_losses, current_losses)
        else:
            winning_trades = 0
            losing_trades = 0
            win_rate = 0.0
            avg_profit = 0.0
            avg_loss = 0.0
            profit_factor = 0.0
            max_consecutive_wins = 0
            max_consecutive_losses = 0
        
        return {
            'total_return': total_return,
            'annualized_return': annualized_return,
            'max_drawdown': max_drawdown_pct,
            'sharpe_ratio': sharpe_ratio,
            'total_trades': total_trades,
            'winning_trades': winning_trades,
            'losing_trades': losing_trades,
            'win_rate': win_rate,
            'avg_profit': avg_profit,
            'avg_loss': avg_loss,
            'profit_factor': profit_factor,
            'max_consecutive_wins': max_consecutive_wins,
            'max_consecutive_losses': max_consecutive_losses
        }
    
    def save_results(self, output_dir: str) -> None:
        """
        Guarda los resultados del backtest en archivos CSV y JSON.
        
        Args:
            output_dir: Directorio de salida para los archivos.
        """
        os.makedirs(output_dir, exist_ok=True)
        
        # Guardar curva de equity
        equity_df = pd.DataFrame(self.equity_curve)
        equity_df.to_csv(os.path.join(output_dir, 'equity_curve.csv'), index=False)
        
        # Guardar historial de trades
        trades_df = pd.DataFrame(self.trades_history)
        trades_df.to_csv(os.path.join(output_dir, 'trades.csv'), index=False)
        
        # Guardar historial de posiciones
        positions_df = pd.DataFrame(self.positions_history)
        positions_df.to_csv(os.path.join(output_dir, 'positions.csv'), index=False)
        
        # Guardar historial de funding rates
        funding_df = pd.DataFrame(self.funding_rates_history)
        funding_df.to_csv(os.path.join(output_dir, 'funding_rates.csv'), index=False)
        
        # Guardar métricas
        metrics = self._calculate_metrics()
        
        with open(os.path.join(output_dir, 'metrics.json'), 'w') as f:
            json.dump(metrics, f, indent=4)
        
        self.logger.info(f"Resultados guardados en {output_dir}")


async def load_historical_data(data_dir: str) -> Dict[str, Dict[str, pd.DataFrame]]:
    """
    Carga datos históricos para backtesting.
    
    Args:
        data_dir: Directorio con archivos de datos históricos.
        
    Returns:
        Dict: Datos históricos por exchange y símbolo.
    """
    exchanges_data = {}
    
    # Buscar archivos CSV en el directorio
    for filename in os.listdir(data_dir):
        if not filename.endswith('.csv'):
            continue
        
        # Formato esperado: exchange_symbol.csv (ej. binance_btcusdt.csv)
        parts = filename.split('_')
        
        if len(parts) < 2:
            continue
        
        exchange_id = parts[0].upper()
        symbol = '_'.join(parts[1:]).split('.')[0].upper()
        
        # Cargar datos
        file_path = os.path.join(data_dir, filename)
        df = pd.read_csv(file_path)
        
        # Convertir timestamp a datetime
        if 'timestamp' in df.columns:
            df['timestamp'] = pd.to_datetime(df['timestamp'])
            df.set_index('timestamp', inplace=True)
        
        # Inicializar exchange si no existe
        if exchange_id not in exchanges_data:
            exchanges_data[exchange_id] = {}
        
        # Guardar datos
        exchanges_data[exchange_id][symbol] = df
    
    return exchanges_data


async def run_backtest(
    start_date: str,
    end_date: str,
    data_dir: str,
    min_funding_rate_diff: float = 0.01,
    max_position_size: float = 10000.0,
    exit_funding_rate_diff: float = 0.005,
    max_position_holding_time: int = 24,
    output_dir: str = None
) -> Dict:
    """
    Ejecuta un backtest de la estrategia de arbitraje.
    
    Args:
        start_date: Fecha de inicio (formato: YYYY-MM-DD).
        end_date: Fecha de fin (formato: YYYY-MM-DD).
        data_dir: Directorio con datos históricos.
        min_funding_rate_diff: Umbral mínimo de diferencial de funding rate.
        max_position_size: Tamaño máximo de posición por exchange.
        exit_funding_rate_diff: Umbral de diferencial para cerrar posición.
        max_position_holding_time: Tiempo máximo de mantenimiento de posición (horas).
        output_dir: Directorio para guardar resultados.
        
    Returns:
        Dict: Métricas del backtest.
    """
    # Convertir fechas
    start = datetime.fromisoformat(start_date)
    end = datetime.fromisoformat(end_date)
    
    # Cargar datos históricos
    exchanges_data = await load_historical_data(data_dir)
    
    if not exchanges_data:
        raise ValueError(f"No se encontraron datos históricos en {data_dir}")
    
    # Configurar motor de backtest
    engine = BacktestEngine(
        start_date=start,
        end_date=end,
        exchanges_data=exchanges_data,
        min_funding_rate_diff=min_funding_rate_diff,
        max_position_size=max_position_size,
        exit_funding_rate_diff=exit_funding_rate_diff,
        max_position_holding_time=max_position_holding_time
    )
    
    # Ejecutar backtest
    results = await engine.run()
    
    # Guardar resultados si se especificó un directorio
    if output_dir:
        engine.save_results(output_dir)
    
    return results

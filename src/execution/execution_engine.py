"""
Motor de ejecución para la estrategia de arbitraje de funding rate.
"""
import asyncio
import logging
import uuid
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Set, Tuple

from src.exchanges.base_exchange import BaseExchange
from src.execution.arbitrage_calculator import ArbitrageCalculator
from src.models.data_models import (
    ArbitrageOpportunity, ArbitragePosition, FundingRateInfo,
    Order, OrderSide, OrderStatus, OrderType, Position
)
from src.risk.risk_manager import RiskManager


class ExecutionEngine:
    """Motor de ejecución para la estrategia de arbitraje de funding rate."""
    
    def __init__(
        self,
        exchanges: Dict[str, BaseExchange],
        calculator: ArbitrageCalculator,
        risk_manager: RiskManager,
        exit_funding_rate_diff: float,
        max_position_holding_time: int
    ):
        """
        Inicializa el motor de ejecución.
        
        Args:
            exchanges: Diccionario de instancias de exchanges por nombre.
            calculator: Calculador de oportunidades de arbitraje.
            risk_manager: Gestor de riesgos.
            exit_funding_rate_diff: Umbral de diferencial para cerrar posición.
            max_position_holding_time: Tiempo máximo de mantenimiento de posición (horas).
        """
        self.exchanges = exchanges
        self.calculator = calculator
        self.risk_manager = risk_manager
        self.exit_funding_rate_diff = exit_funding_rate_diff
        self.max_position_holding_time = max_position_holding_time
        
        self.logger = logging.getLogger("arbitrage.execution")
        self.active_positions: Dict[str, ArbitragePosition] = {}
        self.funding_rates: Dict[str, FundingRateInfo] = {}
        self.running = False
        self.lock = asyncio.Lock()
    
    async def start(self) -> None:
        """Inicia el motor de ejecución."""
        self.running = True
        self.logger.info("Motor de ejecución iniciado")
    
    async def stop(self) -> None:
        """Detiene el motor de ejecución."""
        self.running = False
        self.logger.info("Motor de ejecución detenido")
    
    async def update_funding_rates(self, trading_pairs: List[Tuple[str, str]]) -> None:
        """
        Actualiza las tasas de financiamiento para los pares de trading.
        
        Args:
            trading_pairs: Lista de tuplas (exchange, symbol).
        """
        tasks = []
        
        for exchange_id, symbol in trading_pairs:
            if exchange_id in self.exchanges:
                exchange = self.exchanges[exchange_id]
                tasks.append(self._fetch_funding_rate(exchange, symbol))
        
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        for result in results:
            if isinstance(result, Exception):
                self.logger.error(f"Error al actualizar funding rate: {str(result)}")
            elif isinstance(result, FundingRateInfo):
                self.funding_rates[result.identifier] = result
    
    async def _fetch_funding_rate(self, exchange: BaseExchange, symbol: str) -> FundingRateInfo:
        """
        Obtiene la tasa de financiamiento para un par específico.
        
        Args:
            exchange: Instancia del exchange.
            symbol: Símbolo del contrato perpetuo.
            
        Returns:
            FundingRateInfo: Información de la tasa de financiamiento.
        """
        return await exchange.get_funding_rate(symbol)
    
    async def find_opportunities(self) -> List[ArbitrageOpportunity]:
        """
        Busca oportunidades de arbitraje basadas en las tasas de financiamiento actuales.
        
        Returns:
            List[ArbitrageOpportunity]: Lista de oportunidades de arbitraje.
        """
        funding_rates = list(self.funding_rates.values())
        return self.calculator.calculate_opportunities(funding_rates)
    
    async def execute_opportunity(self, opportunity: ArbitrageOpportunity) -> Optional[str]:
        """
        Ejecuta una oportunidad de arbitraje.
        
        Args:
            opportunity: Oportunidad de arbitraje a ejecutar.
            
        Returns:
            Optional[str]: ID de la posición de arbitraje creada, o None si falló.
        """
        async with self.lock:
            # Verificar si podemos abrir una nueva posición
            if not self.risk_manager.can_open_new_position(opportunity):
                self.logger.warning(f"No se puede abrir nueva posición para {opportunity.long_identifier} vs "
                                   f"{opportunity.short_identifier} debido a restricciones de riesgo")
                return None
            
            # Obtener tamaño de posición
            position_size = self.risk_manager.calculate_position_size(opportunity)
            
            if position_size <= 0:
                self.logger.warning(f"Tamaño de posición calculado es cero o negativo para {opportunity.long_identifier} vs "
                                   f"{opportunity.short_identifier}")
                return None
            
            # Obtener exchanges
            long_exchange_id = opportunity.long_exchange
            short_exchange_id = opportunity.short_exchange
            
            if long_exchange_id not in self.exchanges or short_exchange_id not in self.exchanges:
                self.logger.error(f"Exchange no disponible: {long_exchange_id} o {short_exchange_id}")
                return None
            
            long_exchange = self.exchanges[long_exchange_id]
            short_exchange = self.exchanges[short_exchange_id]
            
            # Obtener precios actuales
            try:
                long_price = await long_exchange.get_mark_price(opportunity.long_symbol)
                short_price = await short_exchange.get_mark_price(opportunity.short_symbol)
            except Exception as e:
                self.logger.error(f"Error al obtener precios: {str(e)}")
                return None
            
            # Calcular cantidades
            long_amount = position_size / long_price
            short_amount = position_size / short_price
            
            # Ejecutar órdenes
            try:
                # Abrir posición long
                long_order = await long_exchange.create_order(
                    symbol=opportunity.long_symbol,
                    order_type=OrderType.MARKET,
                    side=OrderSide.BUY,
                    amount=long_amount
                )
                
                # Abrir posición short
                short_order = await short_exchange.create_order(
                    symbol=opportunity.short_symbol,
                    order_type=OrderType.MARKET,
                    side=OrderSide.SELL,
                    amount=short_amount
                )
                
                # Esperar a que se completen las órdenes
                long_filled = await self._wait_for_order_fill(long_exchange, opportunity.long_symbol, long_order.order_id)
                short_filled = await self._wait_for_order_fill(short_exchange, opportunity.short_symbol, short_order.order_id)
                
                if not long_filled or not short_filled:
                    self.logger.error(f"Error: órdenes no se completaron correctamente")
                    # TODO: Implementar lógica para cerrar posiciones parciales si es necesario
                    return None
                
                # Obtener posiciones actualizadas
                long_position = await long_exchange.get_position(opportunity.long_symbol)
                short_position = await short_exchange.get_position(opportunity.short_symbol)
                
                if not long_position or not short_position:
                    self.logger.error(f"Error: no se pudieron obtener las posiciones después de la ejecución")
                    return None
                
                # Crear posición de arbitraje
                position_id = str(uuid.uuid4())
                arbitrage_position = ArbitragePosition(
                    id=position_id,
                    long_position=long_position,
                    short_position=short_position,
                    funding_rate_diff_at_entry=opportunity.funding_rate_diff,
                    current_funding_rate_diff=opportunity.funding_rate_diff,
                    open_time=datetime.now(),
                    last_update_time=datetime.now()
                )
                
                # Registrar posición
                self.active_positions[position_id] = arbitrage_position
                
                self.logger.info(f"Posición de arbitraje abierta: {position_id}, "
                               f"{opportunity.long_identifier} (long) vs {opportunity.short_identifier} (short), "
                               f"tamaño: ${position_size:.2f}, diferencial: {opportunity.funding_rate_diff:.4f}%")
                
                return position_id
                
            except Exception as e:
                self.logger.error(f"Error al ejecutar oportunidad: {str(e)}")
                return None
    
    async def _wait_for_order_fill(self, exchange: BaseExchange, symbol: str, order_id: str, 
                                  timeout: int = 60) -> bool:
        """
        Espera a que una orden se complete.
        
        Args:
            exchange: Instancia del exchange.
            symbol: Símbolo del contrato perpetuo.
            order_id: ID de la orden.
            timeout: Tiempo máximo de espera en segundos.
            
        Returns:
            bool: True si la orden se completó, False en caso contrario.
        """
        start_time = datetime.now()
        
        while (datetime.now() - start_time).total_seconds() < timeout:
            try:
                order = await exchange.get_order(symbol, order_id)
                
                if order.status == OrderStatus.FILLED:
                    return True
                elif order.status in [OrderStatus.CANCELED, OrderStatus.REJECTED]:
                    self.logger.error(f"Orden {order_id} cancelada o rechazada")
                    return False
                
                await asyncio.sleep(1)
            except Exception as e:
                self.logger.error(f"Error al verificar estado de orden {order_id}: {str(e)}")
                await asyncio.sleep(1)
        
        self.logger.error(f"Timeout esperando que la orden {order_id} se complete")
        return False
    
    async def update_positions(self) -> None:
        """Actualiza el estado de las posiciones activas."""
        position_ids = list(self.active_positions.keys())
        
        for position_id in position_ids:
            try:
                position = self.active_positions[position_id]
                
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
                position.last_update_time = datetime.now()
                
                # Actualizar diferencial de funding rate actual
                long_key = f"{long_exchange_id}:{position.long_position.symbol}"
                short_key = f"{short_exchange_id}:{position.short_position.symbol}"
                
                if long_key in self.funding_rates and short_key in self.funding_rates:
                    long_rate = self.funding_rates[long_key].funding_rate
                    short_rate = self.funding_rates[short_key].funding_rate
                    position.current_funding_rate_diff = short_rate - long_rate
            
            except Exception as e:
                self.logger.error(f"Error al actualizar posición {position_id}: {str(e)}")
    
    async def check_exit_conditions(self) -> None:
        """Verifica condiciones de salida para posiciones activas."""
        position_ids = list(self.active_positions.keys())
        
        for position_id in position_ids:
            try:
                position = self.active_positions[position_id]
                
                # Verificar tiempo máximo de mantenimiento
                holding_time = (datetime.now() - position.open_time).total_seconds() / 3600  # en horas
                
                if holding_time >= self.max_position_holding_time:
                    self.logger.info(f"Cerrando posición {position_id} por tiempo máximo de mantenimiento")
                    await self.close_position(position_id)
                    continue
                
                # Verificar diferencial de funding rate
                if position.current_funding_rate_diff < self.exit_funding_rate_diff:
                    self.logger.info(f"Cerrando posición {position_id} por diferencial de funding rate bajo")
                    await self.close_position(position_id)
                    continue
                
                # Verificar stop loss
                if self.risk_manager.should_stop_loss(position):
                    self.logger.info(f"Cerrando posición {position_id} por stop loss")
                    await self.close_position(position_id)
                    continue
                
            except Exception as e:
                self.logger.error(f"Error al verificar condiciones de salida para posición {position_id}: {str(e)}")
    
    async def close_position(self, position_id: str) -> bool:
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
        
        # Obtener exchanges
        long_exchange_id = position.long_position.exchange
        short_exchange_id = position.short_position.exchange
        
        if long_exchange_id not in self.exchanges or short_exchange_id not in self.exchanges:
            self.logger.error(f"Exchange no disponible: {long_exchange_id} o {short_exchange_id}")
            return False
        
        long_exchange = self.exchanges[long_exchange_id]
        short_exchange = self.exchanges[short_exchange_id]
        
        try:
            # Cerrar posición long (vender)
            await long_exchange.create_order(
                symbol=position.long_position.symbol,
                order_type=OrderType.MARKET,
                side=OrderSide.SELL,
                amount=position.long_position.amount
            )
            
            # Cerrar posición short (comprar)
            await short_exchange.create_order(
                symbol=position.short_position.symbol,
                order_type=OrderType.MARKET,
                side=OrderSide.BUY,
                amount=position.short_position.amount
            )
            
            # Calcular P&L total
            total_pnl = position.total_pnl
            
            # Eliminar posición de activas
            del self.active_positions[position_id]
            
            self.logger.info(f"Posición {position_id} cerrada, P&L total: ${total_pnl:.2f}")
            
            return True
            
        except Exception as e:
            self.logger.error(f"Error al cerrar posición {position_id}: {str(e)}")
            return False
    
    async def run_cycle(self) -> None:
        """Ejecuta un ciclo completo del motor de arbitraje."""
        if not self.running:
            return
        
        try:
            # Actualizar posiciones existentes
            await self.update_positions()
            
            # Verificar condiciones de salida
            await self.check_exit_conditions()
            
            # Buscar nuevas oportunidades
            opportunities = await self.find_opportunities()
            
            # Ejecutar oportunidades viables
            for opportunity in opportunities:
                # Verificar si ya tenemos una posición similar
                if self._has_similar_position(opportunity):
                    continue
                
                # Calcular P&L teórico
                fees = {exchange_id: 0.1 for exchange_id in self.exchanges}  # 0.1% por defecto
                slippage = {}
                
                for exchange_id, exchange in self.exchanges.items():
                    if exchange_id == opportunity.long_exchange:
                        try:
                            slippage[exchange_id] = await exchange.estimate_slippage(
                                opportunity.long_symbol, OrderSide.BUY, 1.0  # Cantidad nominal para estimación
                            )
                        except:
                            slippage[exchange_id] = 0.05  # 0.05% por defecto
                    
                    if exchange_id == opportunity.short_exchange:
                        try:
                            slippage[exchange_id] = await exchange.estimate_slippage(
                                opportunity.short_symbol, OrderSide.SELL, 1.0  # Cantidad nominal para estimación
                            )
                        except:
                            slippage[exchange_id] = 0.05  # 0.05% por defecto
                
                position_size = self.risk_manager.calculate_position_size(opportunity)
                theoretical_pnl = self.calculator.calculate_theoretical_pnl(
                    opportunity, position_size, fees, slippage
                )
                
                # Ejecutar solo si el P&L teórico es positivo
                if theoretical_pnl > 0:
                    await self.execute_opportunity(opportunity)
        
        except Exception as e:
            self.logger.error(f"Error en ciclo de ejecución: {str(e)}")
    
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

"""
Módulo de gestión de riesgos para la estrategia de arbitraje de funding rate.
"""
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional

from src.models.data_models import ArbitrageOpportunity, ArbitragePosition


class RiskManager:
    """Gestor de riesgos para la estrategia de arbitraje de funding rate."""
    
    def __init__(
        self,
        max_position_size: float,
        max_daily_drawdown: float,
        max_positions: int = 5
    ):
        """
        Inicializa el gestor de riesgos.
        
        Args:
            max_position_size: Tamaño máximo de posición por par de arbitraje (USD).
            max_daily_drawdown: Máxima pérdida diaria permitida (USD).
            max_positions: Número máximo de posiciones simultáneas.
        """
        self.max_position_size = max_position_size
        self.max_daily_drawdown = max_daily_drawdown
        self.max_positions = max_positions
        
        self.logger = logging.getLogger("arbitrage.risk")
        self.daily_pnl = 0.0
        self.last_reset = datetime.now()
        self.position_sizes: Dict[str, float] = {}  # Tamaño por exchange
    
    def reset_daily_metrics(self) -> None:
        """Reinicia las métricas diarias si es necesario."""
        now = datetime.now()
        if (now - self.last_reset).days >= 1:
            self.daily_pnl = 0.0
            self.last_reset = now
            self.logger.info("Métricas diarias reiniciadas")
    
    def update_daily_pnl(self, pnl: float) -> None:
        """
        Actualiza el P&L diario.
        
        Args:
            pnl: Ganancia o pérdida a registrar.
        """
        self.reset_daily_metrics()
        self.daily_pnl += pnl
        self.logger.info(f"P&L diario actualizado: ${self.daily_pnl:.2f}")
    
    def can_open_new_position(self, opportunity: ArbitrageOpportunity) -> bool:
        """
        Verifica si se puede abrir una nueva posición según las restricciones de riesgo.
        
        Args:
            opportunity: Oportunidad de arbitraje a evaluar.
            
        Returns:
            bool: True si se puede abrir la posición, False en caso contrario.
        """
        self.reset_daily_metrics()
        
        # Verificar drawdown diario
        if self.daily_pnl <= -self.max_daily_drawdown:
            self.logger.warning(f"No se puede abrir nueva posición: se alcanzó el drawdown diario máximo (${self.max_daily_drawdown:.2f})")
            return False
        
        # Verificar número máximo de posiciones
        if len(self.position_sizes) >= self.max_positions:
            self.logger.warning(f"No se puede abrir nueva posición: se alcanzó el número máximo de posiciones ({self.max_positions})")
            return False
        
        # Verificar exposición por exchange
        long_exchange = opportunity.long_exchange
        short_exchange = opportunity.short_exchange
        
        long_exposure = self.position_sizes.get(long_exchange, 0)
        short_exposure = self.position_sizes.get(short_exchange, 0)
        
        if long_exposure >= self.max_position_size:
            self.logger.warning(f"No se puede abrir nueva posición: exposición máxima alcanzada en {long_exchange}")
            return False
        
        if short_exposure >= self.max_position_size:
            self.logger.warning(f"No se puede abrir nueva posición: exposición máxima alcanzada en {short_exchange}")
            return False
        
        return True
    
    def calculate_position_size(self, opportunity: ArbitrageOpportunity) -> float:
        """
        Calcula el tamaño de posición óptimo para una oportunidad de arbitraje.
        
        Args:
            opportunity: Oportunidad de arbitraje.
            
        Returns:
            float: Tamaño de posición en USD.
        """
        # Obtener exposición actual por exchange
        long_exchange = opportunity.long_exchange
        short_exchange = opportunity.short_exchange
        
        long_exposure = self.position_sizes.get(long_exchange, 0)
        short_exposure = self.position_sizes.get(short_exchange, 0)
        
        # Calcular espacio disponible
        long_available = self.max_position_size - long_exposure
        short_available = self.max_position_size - short_exposure
        
        # Usar el mínimo entre ambos
        available_size = min(long_available, short_available)
        
        # Escalar según el diferencial de funding rate
        # Más diferencial = más tamaño (hasta el máximo disponible)
        funding_diff = opportunity.funding_rate_diff
        
        # Escala simple: 0.01% -> 10% del máximo, 0.1% -> 100% del máximo
        scale_factor = min(funding_diff / 0.1, 1.0)
        
        position_size = available_size * scale_factor
        
        # Aplicar un mínimo razonable (por ejemplo, $100)
        min_size = 100.0
        if position_size < min_size:
            position_size = 0.0  # No abrir posición si es muy pequeña
        
        self.logger.info(f"Tamaño de posición calculado para {opportunity.long_identifier} vs "
                        f"{opportunity.short_identifier}: ${position_size:.2f} "
                        f"(diferencial: {funding_diff:.4f}%, factor: {scale_factor:.2f})")
        
        return position_size
    
    def register_position(self, position: ArbitragePosition) -> None:
        """
        Registra una nueva posición en el gestor de riesgos.
        
        Args:
            position: Posición de arbitraje abierta.
        """
        long_exchange = position.long_position.exchange
        short_exchange = position.short_position.exchange
        
        long_value = position.long_position.position_value
        short_value = position.short_position.position_value
        
        # Actualizar exposición por exchange
        self.position_sizes[long_exchange] = self.position_sizes.get(long_exchange, 0) + long_value
        self.position_sizes[short_exchange] = self.position_sizes.get(short_exchange, 0) + short_value
        
        self.logger.info(f"Posición registrada: {position.id}, "
                        f"exposición en {long_exchange}: ${self.position_sizes[long_exchange]:.2f}, "
                        f"exposición en {short_exchange}: ${self.position_sizes[short_exchange]:.2f}")
    
    def unregister_position(self, position: ArbitragePosition) -> None:
        """
        Elimina una posición del gestor de riesgos y actualiza el P&L.
        
        Args:
            position: Posición de arbitraje cerrada.
        """
        long_exchange = position.long_position.exchange
        short_exchange = position.short_position.exchange
        
        long_value = position.long_position.position_value
        short_value = position.short_position.position_value
        
        # Actualizar exposición por exchange
        if long_exchange in self.position_sizes:
            self.position_sizes[long_exchange] = max(0, self.position_sizes[long_exchange] - long_value)
        
        if short_exchange in self.position_sizes:
            self.position_sizes[short_exchange] = max(0, self.position_sizes[short_exchange] - short_value)
        
        # Actualizar P&L diario
        self.update_daily_pnl(position.total_pnl)
        
        self.logger.info(f"Posición cerrada: {position.id}, P&L: ${position.total_pnl:.2f}, "
                        f"exposición en {long_exchange}: ${self.position_sizes.get(long_exchange, 0):.2f}, "
                        f"exposición en {short_exchange}: ${self.position_sizes.get(short_exchange, 0):.2f}")
    
    def should_stop_loss(self, position: ArbitragePosition) -> bool:
        """
        Determina si una posición debe cerrarse por stop loss.
        
        Args:
            position: Posición de arbitraje a evaluar.
            
        Returns:
            bool: True si la posición debe cerrarse, False en caso contrario.
        """
        # Ejemplo de regla de stop loss: pérdida mayor al 1% del valor de la posición
        position_value = position.long_position.position_value + position.short_position.position_value
        loss_threshold = position_value * 0.01
        
        if position.total_pnl < -loss_threshold:
            self.logger.warning(f"Stop loss activado para posición {position.id}: "
                              f"P&L actual ${position.total_pnl:.2f}, umbral -${loss_threshold:.2f}")
            return True
        
        return False
    
    def get_risk_metrics(self) -> Dict:
        """
        Obtiene métricas actuales de riesgo.
        
        Returns:
            Dict: Métricas de riesgo.
        """
        return {
            "daily_pnl": self.daily_pnl,
            "max_daily_drawdown": self.max_daily_drawdown,
            "position_sizes": self.position_sizes,
            "max_position_size": self.max_position_size,
            "max_positions": self.max_positions,
            "active_positions": len(self.position_sizes)
        }

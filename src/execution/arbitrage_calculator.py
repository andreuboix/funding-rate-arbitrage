"""
Módulo para el cálculo de oportunidades de arbitraje de funding rate.
"""
import asyncio
import logging
from datetime import datetime
from typing import Dict, List, Optional, Tuple

from src.models.data_models import ArbitrageOpportunity, FundingRateInfo


class ArbitrageCalculator:
    """Clase para calcular oportunidades de arbitraje de funding rate."""
    
    def __init__(self, min_funding_rate_diff: float):
        """
        Inicializa el calculador de arbitraje.
        
        Args:
            min_funding_rate_diff: Umbral mínimo de diferencial de funding rate para considerar una oportunidad.
        """
        self.min_funding_rate_diff = min_funding_rate_diff
        self.logger = logging.getLogger("arbitrage.calculator")
    
    def calculate_opportunities(self, funding_rates: List[FundingRateInfo]) -> List[ArbitrageOpportunity]:
        """
        Calcula oportunidades de arbitraje basadas en diferenciales de funding rate.
        
        Args:
            funding_rates: Lista de información de funding rate para diferentes contratos.
            
        Returns:
            List[ArbitrageOpportunity]: Lista de oportunidades de arbitraje identificadas.
        """
        opportunities = []
        
        # Ordenar por funding rate (de menor a mayor)
        sorted_rates = sorted(funding_rates, key=lambda x: x.funding_rate)
        
        # Comparar cada par posible para encontrar oportunidades
        for i in range(len(sorted_rates)):
            for j in range(i + 1, len(sorted_rates)):
                low_rate = sorted_rates[i]
                high_rate = sorted_rates[j]
                
                # Calcular diferencial de funding rate
                funding_rate_diff = high_rate.funding_rate - low_rate.funding_rate
                
                # Verificar si supera el umbral mínimo
                if funding_rate_diff >= self.min_funding_rate_diff:
                    # Calcular beneficio teórico (sin considerar fees y slippage)
                    theoretical_profit = funding_rate_diff
                    
                    # Crear oportunidad de arbitraje
                    opportunity = ArbitrageOpportunity(
                        long_exchange=low_rate.exchange,
                        long_symbol=low_rate.symbol,
                        short_exchange=high_rate.exchange,
                        short_symbol=high_rate.symbol,
                        funding_rate_diff=funding_rate_diff,
                        theoretical_profit=theoretical_profit,
                        timestamp=datetime.now()
                    )
                    
                    opportunities.append(opportunity)
                    self.logger.info(f"Oportunidad de arbitraje identificada: {opportunity.long_identifier} (long) vs "
                                     f"{opportunity.short_identifier} (short), diferencial: {funding_rate_diff:.4f}%")
        
        return opportunities
    
    def calculate_theoretical_pnl(self, opportunity: ArbitrageOpportunity, position_size: float,
                                 fees: Dict[str, float], slippage: Dict[str, float]) -> float:
        """
        Calcula el P&L teórico para una oportunidad de arbitraje.
        
        Args:
            opportunity: Oportunidad de arbitraje.
            position_size: Tamaño de posición en USD.
            fees: Diccionario de fees por exchange (en porcentaje).
            slippage: Diccionario de slippage estimado por exchange (en porcentaje).
            
        Returns:
            float: P&L teórico en USD.
        """
        # Calcular funding rate ganado (en el lado long)
        long_funding = position_size * (opportunity.funding_rate_diff / 100)
        
        # Calcular costos de trading
        long_exchange = opportunity.long_exchange
        short_exchange = opportunity.short_exchange
        
        long_fee = position_size * (fees.get(long_exchange, 0) / 100)
        short_fee = position_size * (fees.get(short_exchange, 0) / 100)
        
        long_slippage = position_size * (slippage.get(long_exchange, 0) / 100)
        short_slippage = position_size * (slippage.get(short_exchange, 0) / 100)
        
        total_costs = long_fee + short_fee + long_slippage + short_slippage
        
        # Calcular P&L teórico
        theoretical_pnl = long_funding - total_costs
        
        self.logger.info(f"P&L teórico para {opportunity.long_identifier} vs {opportunity.short_identifier}: "
                         f"${theoretical_pnl:.2f} (funding: ${long_funding:.2f}, costos: ${total_costs:.2f})")
        
        return theoretical_pnl
    
    def should_close_position(self, current_diff: float, exit_threshold: float) -> bool:
        """
        Determina si una posición de arbitraje debe cerrarse basado en el diferencial actual.
        
        Args:
            current_diff: Diferencial de funding rate actual.
            exit_threshold: Umbral de salida configurado.
            
        Returns:
            bool: True si la posición debe cerrarse, False en caso contrario.
        """
        return current_diff < exit_threshold

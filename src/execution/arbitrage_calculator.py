"""
Módulo para el cálculo de oportunidades de arbitraje de funding rate.
"""
import asyncio
import logging
from datetime import datetime
from typing import Dict, List, Optional, Tuple
from src.models.data_models import ArbitrageOpportunity, FundingRateInfo
import os


def _parse_env_list(key: str) -> List[str]:
    return os.getenv(key, "").split(',') if os.getenv(key) else []

class ArbitrageCalculator:
    """Clase para calcular oportunidades de arbitraje de funding rate con costos reales."""
    
    def __init__(
        self,
        position_size: float,
        min_funding_rate_diff: float,
        exit_threshold: float,
        fees: Dict[str, float],
        slippage: Dict[str, float]
    ):
        self.position_size = position_size
        self.min_funding_rate_diff = min_funding_rate_diff
        self.exit_threshold = exit_threshold
        self.fees = fees
        self.slippage = slippage
        self.logger = logging.getLogger("arbitrage.calculator")

    def _load_trading_pairs(self) -> List[Tuple[str, str]]:
        """
        Lee TRADING_PAIRS de env: formato EXCHANGE:SYMBOL.
        Devuelve lista de tuplas (exchange, symbol).
        """
        raw = _parse_env_list('TRADING_PAIRS')
        pairs = []
        for entry in raw:
            try:
                exch, sym = entry.split(':', 1)
                pairs.append((exch.strip(), sym.strip()))
            except ValueError:
                self.logger.warning(f"Formato inválido en TRADING_PAIRS: {entry}")
        return pairs

    def calculate_opportunity(
        self,
        funding_rates: List[FundingRateInfo]
    ) -> Optional[ArbitrageOpportunity]:
        """
        Calcula y devuelve la oportunidad de arbitraje más rentable.
        """
        best = None
        best_pnl = 0.0

        # Ordenar por funding rate
        rates = sorted(funding_rates, key=lambda x: x.funding_rate)
        # Comparar pares
        for i in range(len(rates)):
            for j in range(i + 1, len(rates)):
                low = rates[i]
                high = rates[j]
                diff = high.funding_rate - low.funding_rate
                if diff < self.min_funding_rate_diff:
                    continue
                # Calcular P&L teórico con costos
                arb = ArbitrageOpportunity(
                    long_exchange=low.exchange,
                    long_symbol=low.symbol,
                    short_exchange=high.exchange,
                    short_symbol=high.symbol,
                    funding_rate_diff=diff,
                    theoretical_profit=0.0,
                    timestamp=datetime.now()
                )
                pnl = self.calculate_theoretical_pnl(arb)
                if pnl > best_pnl:
                    best_pnl = pnl
                    best = arb
        if best:
            best.theoretical_profit = best_pnl
            self.logger.info(f"Mejor oportunidad: {best.long_identifier} vs {best.short_identifier}, PnL={best_pnl:.2f}")
        return best

    def calculate_theoretical_pnl(
        self,
        opportunity: ArbitrageOpportunity
    ) -> float:
        """
        Calcula el P&L teórico para una oportunidad usando costos reales.
        """
        # Funding ganado
        fund = self.position_size * (opportunity.funding_rate_diff / 100)

        # Costos trading
        long_fee = self.position_size * (self.fees.get(opportunity.long_exchange, 0) / 100)
        short_fee = self.position_size * (self.fees.get(opportunity.short_exchange, 0) / 100)
        long_slip = self.position_size * (self.slippage.get(opportunity.long_exchange, 0) / 100)
        short_slip = self.position_size * (self.slippage.get(opportunity.short_exchange, 0) / 100)
        total_costs = long_fee + short_fee + long_slip + short_slip

        pnl = fund - total_costs
        self.logger.debug(
            f"PnL teórico ({opportunity.long_exchange}/{opportunity.short_exchange}): funding={fund:.4f}, costs={total_costs:.4f}, pnl={pnl:.4f}"
        )
        return pnl

    def should_close_position(
        self,
        current_diff: float
    ) -> bool:
        """
        Determina si cerrar según umbral de salida.
        """
        return current_diff < self.exit_threshold
"""
Pruebas unitarias para el módulo de cálculo de arbitraje.
"""
import pytest
from datetime import datetime
from src.models.data_models import FundingRateInfo
from src.execution.arbitrage_calculator import ArbitrageCalculator


@pytest.fixture
def funding_rates():
    """Fixture para crear datos de prueba de funding rates."""
    return [
        FundingRateInfo(
            exchange="BINANCE",
            symbol="BTC/USDT",
            funding_rate=0.01,  # 0.01% por 8h
            next_funding_time=datetime.now(),
            mark_price=50000.0,
            index_price=50010.0,
            timestamp=datetime.now()
        ),
        FundingRateInfo(
            exchange="BYBIT",
            symbol="BTC-PERP",
            funding_rate=0.03,  # 0.03% por 8h
            next_funding_time=datetime.now(),
            mark_price=50020.0,
            index_price=50015.0,
            timestamp=datetime.now()
        ),
        FundingRateInfo(
            exchange="OKX",
            symbol="BTC-USDT-SWAP",
            funding_rate=-0.01,  # -0.01% por 8h
            next_funding_time=datetime.now(),
            mark_price=49990.0,
            index_price=50005.0,
            timestamp=datetime.now()
        )
    ]


def test_calculate_opportunities_with_threshold():
    """Prueba el cálculo de oportunidades con un umbral específico."""
    # Configurar
    calculator = ArbitrageCalculator(min_funding_rate_diff=0.02)
    rates = [
        FundingRateInfo(
            exchange="BINANCE",
            symbol="BTC/USDT",
            funding_rate=0.01,
            next_funding_time=datetime.now(),
            mark_price=50000.0,
            index_price=50010.0,
            timestamp=datetime.now()
        ),
        FundingRateInfo(
            exchange="BYBIT",
            symbol="BTC-PERP",
            funding_rate=0.03,
            next_funding_time=datetime.now(),
            mark_price=50020.0,
            index_price=50015.0,
            timestamp=datetime.now()
        )
    ]
    
    # Ejecutar
    opportunities = calculator.calculate_opportunities(rates)
    
    # Verificar
    assert len(opportunities) == 1
    assert opportunities[0].long_exchange == "BINANCE"
    assert opportunities[0].short_exchange == "BYBIT"
    assert opportunities[0].funding_rate_diff == 0.02
    assert opportunities[0].theoretical_profit == 0.02


def test_calculate_opportunities_below_threshold():
    """Prueba que no se generen oportunidades por debajo del umbral."""
    # Configurar
    calculator = ArbitrageCalculator(min_funding_rate_diff=0.03)
    rates = [
        FundingRateInfo(
            exchange="BINANCE",
            symbol="BTC/USDT",
            funding_rate=0.01,
            next_funding_time=datetime.now(),
            mark_price=50000.0,
            index_price=50010.0,
            timestamp=datetime.now()
        ),
        FundingRateInfo(
            exchange="BYBIT",
            symbol="BTC-PERP",
            funding_rate=0.03,
            next_funding_time=datetime.now(),
            mark_price=50020.0,
            index_price=50015.0,
            timestamp=datetime.now()
        )
    ]
    
    # Ejecutar
    opportunities = calculator.calculate_opportunities(rates)
    
    # Verificar
    assert len(opportunities) == 0


def test_calculate_opportunities_multiple(funding_rates):
    """Prueba el cálculo de múltiples oportunidades."""
    # Configurar
    calculator = ArbitrageCalculator(min_funding_rate_diff=0.01)
    
    # Ejecutar
    opportunities = calculator.calculate_opportunities(funding_rates)
    
    # Verificar
    assert len(opportunities) == 2
    
    # Verificar oportunidad 1: OKX vs BYBIT
    assert any(
        opp.long_exchange == "OKX" and opp.short_exchange == "BYBIT" and opp.funding_rate_diff == 0.04
        for opp in opportunities
    )
    
    # Verificar oportunidad 2: BINANCE vs BYBIT
    assert any(
        opp.long_exchange == "BINANCE" and opp.short_exchange == "BYBIT" and opp.funding_rate_diff == 0.02
        for opp in opportunities
    )


def test_calculate_theoretical_pnl():
    """Prueba el cálculo del P&L teórico."""
    # Configurar
    calculator = ArbitrageCalculator(min_funding_rate_diff=0.01)
    rates = [
        FundingRateInfo(
            exchange="BINANCE",
            symbol="BTC/USDT",
            funding_rate=0.01,
            next_funding_time=datetime.now(),
            mark_price=50000.0,
            index_price=50010.0,
            timestamp=datetime.now()
        ),
        FundingRateInfo(
            exchange="BYBIT",
            symbol="BTC-PERP",
            funding_rate=0.03,
            next_funding_time=datetime.now(),
            mark_price=50020.0,
            index_price=50015.0,
            timestamp=datetime.now()
        )
    ]
    
    opportunities = calculator.calculate_opportunities(rates)
    assert len(opportunities) == 1
    
    # Configurar parámetros para el cálculo de P&L
    position_size = 10000.0  # $10,000
    fees = {"BINANCE": 0.1, "BYBIT": 0.1}  # 0.1% por operación
    slippage = {"BINANCE": 0.05, "BYBIT": 0.05}  # 0.05% de slippage
    
    # Ejecutar
    pnl = calculator.calculate_theoretical_pnl(opportunities[0], position_size, fees, slippage)
    
    # Verificar
    # Funding ganado: $10,000 * 0.02% = $2
    # Costos: $10,000 * (0.1% + 0.1% + 0.05% + 0.05%) = $30
    # P&L teórico: $2 - $30 = -$28
    assert pnl == pytest.approx(-28.0, abs=0.1)


def test_should_close_position():
    """Prueba la lógica para determinar si una posición debe cerrarse."""
    # Configurar
    calculator = ArbitrageCalculator(min_funding_rate_diff=0.01)
    exit_threshold = 0.005
    
    # Ejecutar y verificar
    assert calculator.should_close_position(current_diff=0.004, exit_threshold=exit_threshold) is True
    assert calculator.should_close_position(current_diff=0.006, exit_threshold=exit_threshold) is False
    assert calculator.should_close_position(current_diff=0.005, exit_threshold=exit_threshold) is False

"""
Pruebas unitarias para el módulo de gestión de riesgos.
"""
import pytest
from datetime import datetime, timedelta
from src.models.data_models import (
    ArbitrageOpportunity, ArbitragePosition, Position, OrderSide
)
from src.risk.risk_manager import RiskManager


@pytest.fixture
def risk_manager():
    """Fixture para crear un gestor de riesgos para pruebas."""
    return RiskManager(
        max_position_size=10000.0,
        max_daily_drawdown=500.0,
        max_positions=5
    )


@pytest.fixture
def sample_opportunity():
    """Fixture para crear una oportunidad de arbitraje de prueba."""
    return ArbitrageOpportunity(
        long_exchange="BINANCE",
        long_symbol="BTC/USDT",
        short_exchange="BYBIT",
        short_symbol="BTC-PERP",
        funding_rate_diff=0.02,
        theoretical_profit=0.02,
        timestamp=datetime.now()
    )


@pytest.fixture
def sample_position():
    """Fixture para crear una posición de arbitraje de prueba."""
    long_position = Position(
        exchange="BINANCE",
        symbol="BTC/USDT",
        side=OrderSide.BUY,
        amount=0.2,
        entry_price=50000.0,
        current_price=50100.0,
        unrealized_pnl=20.0,
        realized_pnl=0.0,
        open_time=datetime.now() - timedelta(hours=5),
        last_update_time=datetime.now()
    )
    
    short_position = Position(
        exchange="BYBIT",
        symbol="BTC-PERP",
        side=OrderSide.SELL,
        amount=0.2,
        entry_price=50100.0,
        current_price=50000.0,
        unrealized_pnl=20.0,
        realized_pnl=0.0,
        open_time=datetime.now() - timedelta(hours=5),
        last_update_time=datetime.now()
    )
    
    return ArbitragePosition(
        id="test-position-1",
        long_position=long_position,
        short_position=short_position,
        funding_rate_diff_at_entry=0.02,
        current_funding_rate_diff=0.015,
        open_time=datetime.now() - timedelta(hours=5),
        last_update_time=datetime.now()
    )


def test_reset_daily_metrics(risk_manager):
    """Prueba el reinicio de métricas diarias."""
    # Configurar
    risk_manager.daily_pnl = -200.0
    risk_manager.last_reset = datetime.now() - timedelta(days=2)
    
    # Ejecutar
    risk_manager.reset_daily_metrics()
    
    # Verificar
    assert risk_manager.daily_pnl == 0.0
    assert (datetime.now() - risk_manager.last_reset).total_seconds() < 10  # Menos de 10 segundos


def test_update_daily_pnl(risk_manager):
    """Prueba la actualización del P&L diario."""
    # Configurar
    initial_pnl = risk_manager.daily_pnl
    
    # Ejecutar
    risk_manager.update_daily_pnl(100.0)
    
    # Verificar
    assert risk_manager.daily_pnl == initial_pnl + 100.0


def test_can_open_new_position(risk_manager, sample_opportunity):
    """Prueba la verificación para abrir nuevas posiciones."""
    # Caso 1: Sin restricciones
    assert risk_manager.can_open_new_position(sample_opportunity) is True
    
    # Caso 2: Drawdown diario alcanzado
    risk_manager.daily_pnl = -risk_manager.max_daily_drawdown
    assert risk_manager.can_open_new_position(sample_opportunity) is False
    risk_manager.daily_pnl = 0.0  # Restaurar
    
    # Caso 3: Número máximo de posiciones alcanzado
    for i in range(risk_manager.max_positions):
        risk_manager.position_sizes[f"EXCHANGE{i}"] = 1000.0
    assert risk_manager.can_open_new_position(sample_opportunity) is False
    risk_manager.position_sizes.clear()  # Restaurar
    
    # Caso 4: Exposición máxima en un exchange
    risk_manager.position_sizes["BINANCE"] = risk_manager.max_position_size
    assert risk_manager.can_open_new_position(sample_opportunity) is False
    risk_manager.position_sizes.clear()  # Restaurar


def test_calculate_position_size(risk_manager, sample_opportunity):
    """Prueba el cálculo del tamaño de posición."""
    # Caso 1: Sin posiciones existentes
    size = risk_manager.calculate_position_size(sample_opportunity)
    assert size > 0
    assert size <= risk_manager.max_position_size
    
    # Caso 2: Con posiciones existentes
    risk_manager.position_sizes["BINANCE"] = 5000.0
    risk_manager.position_sizes["BYBIT"] = 2000.0
    
    size = risk_manager.calculate_position_size(sample_opportunity)
    assert size > 0
    assert size <= risk_manager.max_position_size - 5000.0  # Espacio disponible en BINANCE
    
    # Caso 3: Diferencial de funding muy pequeño
    small_diff_opportunity = ArbitrageOpportunity(
        long_exchange="BINANCE",
        long_symbol="BTC/USDT",
        short_exchange="BYBIT",
        short_symbol="BTC-PERP",
        funding_rate_diff=0.001,  # Muy pequeño
        theoretical_profit=0.001,
        timestamp=datetime.now()
    )
    
    size = risk_manager.calculate_position_size(small_diff_opportunity)
    assert size == 0.0  # No abrir posición si el diferencial es muy pequeño


def test_register_position(risk_manager, sample_position):
    """Prueba el registro de una posición."""
    # Configurar
    initial_binance = risk_manager.position_sizes.get("BINANCE", 0)
    initial_bybit = risk_manager.position_sizes.get("BYBIT", 0)
    
    # Ejecutar
    risk_manager.register_position(sample_position)
    
    # Verificar
    assert risk_manager.position_sizes["BINANCE"] == initial_binance + sample_position.long_position.position_value
    assert risk_manager.position_sizes["BYBIT"] == initial_bybit + sample_position.short_position.position_value


def test_unregister_position(risk_manager, sample_position):
    """Prueba la eliminación del registro de una posición."""
    # Configurar
    risk_manager.position_sizes["BINANCE"] = 15000.0
    risk_manager.position_sizes["BYBIT"] = 12000.0
    initial_pnl = risk_manager.daily_pnl
    
    # Ejecutar
    risk_manager.unregister_position(sample_position)
    
    # Verificar
    assert risk_manager.position_sizes["BINANCE"] == 15000.0 - sample_position.long_position.position_value
    assert risk_manager.position_sizes["BYBIT"] == 12000.0 - sample_position.short_position.position_value
    assert risk_manager.daily_pnl == initial_pnl + sample_position.total_pnl


def test_should_stop_loss(risk_manager, sample_position):
    """Prueba la lógica de stop loss."""
    # Caso 1: Sin pérdida
    sample_position.long_position.unrealized_pnl = 20.0
    sample_position.short_position.unrealized_pnl = 20.0
    assert risk_manager.should_stop_loss(sample_position) is False
    
    # Caso 2: Con pérdida que activa stop loss
    position_value = sample_position.long_position.position_value + sample_position.short_position.position_value
    loss_threshold = position_value * 0.01
    
    sample_position.long_position.unrealized_pnl = -loss_threshold * 0.6
    sample_position.short_position.unrealized_pnl = -loss_threshold * 0.6
    assert risk_manager.should_stop_loss(sample_position) is True


def test_get_risk_metrics(risk_manager):
    """Prueba la obtención de métricas de riesgo."""
    # Configurar
    risk_manager.daily_pnl = -150.0
    risk_manager.position_sizes = {"BINANCE": 5000.0, "BYBIT": 3000.0}
    
    # Ejecutar
    metrics = risk_manager.get_risk_metrics()
    
    # Verificar
    assert metrics["daily_pnl"] == -150.0
    assert metrics["max_daily_drawdown"] == 500.0
    assert metrics["position_sizes"] == {"BINANCE": 5000.0, "BYBIT": 3000.0}
    assert metrics["max_position_size"] == 10000.0
    assert metrics["max_positions"] == 5
    assert metrics["active_positions"] == 2

"""
Módulo principal para la ejecución de la estrategia de arbitraje de funding rate.
"""
import asyncio
import logging
import os
import signal
import sys
from datetime import datetime
from typing import Dict, List, Optional, Tuple

from loguru import logger
from dotenv import load_dotenv

from src.config import load_config
from src.exchanges.binance_exchange import BinanceExchange
from src.exchanges.bybit_exchange import BybitExchange
from src.execution.arbitrage_calculator import ArbitrageCalculator
from src.execution.execution_engine import ExecutionEngine
from src.risk.risk_manager import RiskManager
from src.api.health_check import start_api_server


# Configurar logging
def setup_logging(log_level: str, log_dir: str) -> None:
    """
    Configura el sistema de logging.
    
    Args:
        log_level: Nivel de logging (DEBUG, INFO, WARNING, ERROR).
        log_dir: Directorio para archivos de log.
    """
    # Crear directorio de logs si no existe
    os.makedirs(log_dir, exist_ok=True)
    
    # Configurar loguru
    logger.remove()  # Eliminar handler por defecto
    
    # Añadir handler para consola
    logger.add(
        sys.stdout,
        format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level: <8}</level> | <cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - <level>{message}</level>",
        level=log_level
    )
    
    # Añadir handler para archivo rotativo
    logger.add(
        os.path.join(log_dir, "arbitrage_{time:YYYY-MM-DD}.log"),
        rotation="00:00",  # Rotar a medianoche
        retention="7 days",  # Mantener logs por 7 días
        format="{time:YYYY-MM-DD HH:mm:ss} | {level: <8} | {name}:{function}:{line} - {message}",
        level=log_level,
        serialize=True  # Formato JSON para facilitar análisis
    )
    
    # Configurar logging estándar para redirigir a loguru
    class InterceptHandler(logging.Handler):
        def emit(self, record):
            # Obtener el mensaje formateado
            logger_opt = logger.opt(depth=6, exception=record.exc_info)
            logger_opt.log(record.levelname, record.getMessage())
    
    # Configurar handler para bibliotecas que usan logging estándar
    logging.basicConfig(handlers=[InterceptHandler()], level=logging.INFO, force=True)


async def main() -> None:
    """Función principal para ejecutar la estrategia de arbitraje."""
    # Cargar configuración
    config = load_config()
    
    # Configurar logging
    setup_logging(config.log_level, config.log_dir)
    
    logger.info("Iniciando estrategia de arbitraje de funding rate")
    logger.info(f"Configuración cargada: {len(config.exchanges)} exchanges, {len(config.trading_pairs)} pares de trading")
    
    # Inicializar exchanges
    exchanges = {}
    
    for exchange_id, exchange_config in config.exchanges.items():
        try:
            if exchange_id == "BINANCE":
                exchange = BinanceExchange(
                    api_key=exchange_config.api_key,
                    api_secret=exchange_config.api_secret
                )
            elif exchange_id == "BYBIT":
                exchange = BybitExchange(
                    api_key=exchange_config.api_key,
                    api_secret=exchange_config.api_secret
                )
            else:
                logger.warning(f"Exchange no soportado: {exchange_id}")
                continue
            
            await exchange.initialize()
            exchanges[exchange_id] = exchange
            logger.info(f"Exchange {exchange_id} inicializado correctamente")
        except Exception as e:
            logger.error(f"Error al inicializar exchange {exchange_id}: {str(e)}")
    
    if not exchanges:
        logger.error("No se pudo inicializar ningún exchange, abortando")
        return
    
    # Inicializar componentes
    calculator = ArbitrageCalculator(config.min_funding_rate_diff)
    
    risk_manager = RiskManager(
        max_position_size=config.max_position_size,
        max_daily_drawdown=config.risk.max_daily_drawdown
    )
    
    execution_engine = ExecutionEngine(
        exchanges=exchanges,
        calculator=calculator,
        risk_manager=risk_manager,
        exit_funding_rate_diff=config.risk.exit_funding_rate_diff,
        max_position_holding_time=config.risk.max_position_holding_time
    )
    
    # Iniciar API de health check
    api_task = asyncio.create_task(
        start_api_server(
            port=config.api_port,
            execution_engine=execution_engine,
            risk_manager=risk_manager
        )
    )
    
    # Preparar pares de trading
    trading_pairs = [(pair.exchange, pair.symbol) for pair in config.trading_pairs]
    
    # Iniciar motor de ejecución
    await execution_engine.start()
    
    # Manejar señales de terminación
    loop = asyncio.get_running_loop()
    
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, lambda: asyncio.create_task(shutdown(exchanges, execution_engine, api_task)))
    
    # Bucle principal
    try:
        while True:
            # Actualizar tasas de funding
            await execution_engine.update_funding_rates(trading_pairs)
            
            # Ejecutar ciclo de arbitraje
            await execution_engine.run_cycle()
            
            # Esperar antes del siguiente ciclo
            await asyncio.sleep(10)  # Ajustar según necesidades
    
    except Exception as e:
        logger.error(f"Error en bucle principal: {str(e)}")
        await shutdown(exchanges, execution_engine, api_task)


async def shutdown(exchanges, execution_engine, api_task):
    """Cierra correctamente todos los componentes."""
    logger.info("Cerrando estrategia de arbitraje...")
    
    # Detener motor de ejecución
    await execution_engine.stop()
    
    # Cerrar conexiones con exchanges
    for exchange_id, exchange in exchanges.items():
        try:
            await exchange.close()
            logger.info(f"Exchange {exchange_id} cerrado correctamente")
        except Exception as e:
            logger.error(f"Error al cerrar exchange {exchange_id}: {str(e)}")
    
    # Cancelar tarea de API
    api_task.cancel()
    
    # Salir
    logger.info("Estrategia de arbitraje cerrada correctamente")


if __name__ == "__main__":
    asyncio.run(main())

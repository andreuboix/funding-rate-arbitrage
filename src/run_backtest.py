"""
Script para ejecutar el modo de simulación/backtesting.
"""
import argparse
import asyncio
import os
import sys
from datetime import datetime

from src.backtest import run_backtest


async def main():
    """Función principal para ejecutar el backtesting."""
    parser = argparse.ArgumentParser(description="Ejecutar backtesting de arbitraje de funding rate")
    
    parser.add_argument("--start-date", type=str, required=True,
                        help="Fecha de inicio (formato: YYYY-MM-DD)")
    
    parser.add_argument("--end-date", type=str, required=True,
                        help="Fecha de fin (formato: YYYY-MM-DD)")
    
    parser.add_argument("--data-dir", type=str, required=True,
                        help="Directorio con datos históricos")
    
    parser.add_argument("--min-funding-rate-diff", type=float, default=0.01,
                        help="Umbral mínimo de diferencial de funding rate (por defecto: 0.01)")
    
    parser.add_argument("--max-position-size", type=float, default=10000.0,
                        help="Tamaño máximo de posición por exchange en USD (por defecto: 10000.0)")
    
    parser.add_argument("--exit-funding-rate-diff", type=float, default=0.005,
                        help="Umbral de diferencial para cerrar posición (por defecto: 0.005)")
    
    parser.add_argument("--max-position-holding-time", type=int, default=24,
                        help="Tiempo máximo de mantenimiento de posición en horas (por defecto: 24)")
    
    parser.add_argument("--output-dir", type=str, default="./backtest_results",
                        help="Directorio para guardar resultados (por defecto: ./backtest_results)")
    
    args = parser.parse_args()
    
    try:
        # Validar fechas
        start_date = datetime.fromisoformat(args.start_date)
        end_date = datetime.fromisoformat(args.end_date)
        
        if end_date <= start_date:
            print("Error: La fecha de fin debe ser posterior a la fecha de inicio")
            sys.exit(1)
        
        # Validar directorio de datos
        if not os.path.isdir(args.data_dir):
            print(f"Error: El directorio de datos '{args.data_dir}' no existe")
            sys.exit(1)
        
        # Crear directorio de salida si no existe
        os.makedirs(args.output_dir, exist_ok=True)
        
        print(f"Iniciando backtesting desde {args.start_date} hasta {args.end_date}...")
        print(f"Umbral de diferencial: {args.min_funding_rate_diff}%")
        print(f"Tamaño máximo de posición: ${args.max_position_size}")
        
        # Ejecutar backtesting
        results = await run_backtest(
            start_date=args.start_date,
            end_date=args.end_date,
            data_dir=args.data_dir,
            min_funding_rate_diff=args.min_funding_rate_diff,
            max_position_size=args.max_position_size,
            exit_funding_rate_diff=args.exit_funding_rate_diff,
            max_position_holding_time=args.max_position_holding_time,
            output_dir=args.output_dir
        )
        
        # Mostrar resultados
        print("\nResultados del backtesting:")
        print(f"Rendimiento total: {results['total_return']:.2f}%")
        print(f"Rendimiento anualizado: {results['annualized_return']:.2f}%")
        print(f"Máximo drawdown: {results['max_drawdown']:.2f}%")
        print(f"Ratio de Sharpe: {results['sharpe_ratio']:.2f}")
        print(f"Total de operaciones: {results['total_trades']}")
        print(f"Operaciones ganadoras: {results['winning_trades']} ({results['win_rate']:.2f}%)")
        print(f"Operaciones perdedoras: {results['losing_trades']}")
        print(f"Beneficio promedio: ${results['avg_profit']:.2f}")
        print(f"Pérdida promedio: ${results['avg_loss']:.2f}")
        print(f"Factor de beneficio: {results['profit_factor']:.2f}")
        
        print(f"\nResultados guardados en: {args.output_dir}")
        
    except ValueError as e:
        print(f"Error: {str(e)}")
        sys.exit(1)
    except Exception as e:
        print(f"Error inesperado: {str(e)}")
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())

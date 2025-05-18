# Arbitraje de Funding Rate para Criptomonedas

Este proyecto implementa una estrategia de arbitraje de funding rate para swaps perpetuos entre múltiples exchanges de criptomonedas. La estrategia aprovecha las diferencias en las tasas de financiamiento entre distintos exchanges para generar beneficios con posiciones neutrales al mercado.

## Características

- Monitoreo en tiempo real de tasas de financiamiento en múltiples exchanges
- Cálculo automático de diferenciales de funding rate y oportunidades de arbitraje
- Ejecución asíncrona de órdenes para abrir y cerrar posiciones de arbitraje
- Gestión de riesgos y límites de posición configurables
- Registro detallado de operaciones y P&L
- Modo de simulación/backtesting para probar estrategias
- Endpoint de health-check para monitoreo

## Requisitos

- Python 3.11+
- Poetry (gestor de dependencias)
- Claves API para los exchanges que se utilizarán

## Instalación

1. Instalar dependencias con Poetry:
```bash
poetry install
```

2. Configurar variables de entorno:
```bash
cp .env.example .env
# Editar .env con tus claves API y configuraciones
```

## Uso

### Modo de producción

```bash
poetry run python -m src.main
```

### Modo de simulación/backtesting

```bash
poetry run python -m src.backtest --start-date 2023-01-01 --end-date 2023-01-31
```

## Configuración

El archivo `.env` permite configurar:

- Claves API para cada exchange
- Pares de trading (ej. BINANCE:BTC/USDT, BYBIT:BTC-PERP)
- Umbral mínimo de diferencial de funding rate
- Tamaño máximo de posición por exchange
- Risk limits y stop-loss

## Despliegue con Docker

1. Construir la imagen:
```bash
docker build -t funding-rate-arbitrage .
```

2. Ejecutar el contenedor:
```bash
docker run -d --name funding-arb --env-file .env funding-rate-arbitrage
```

## Monitoreo

El proyecto incluye un endpoint de health-check en `http://localhost:8000/health` que proporciona:
- Diferencial de funding rate actual
- Posiciones abiertas
- Tiempo de actividad
- Métricas de rendimiento

Para integración con Prometheus/Grafana, consultar la documentación en `/docs/monitoring.md`.

## Estructura del Proyecto

```
funding-rate-arbitrage/
├── src/                    # Código fuente principal
│   ├── __init__.py
│   ├── main.py             # Punto de entrada principal
│   ├── config.py           # Configuración y carga de variables
│   ├── exchanges/          # Conectores para exchanges
│   ├── models/             # Modelos de datos
│   ├── execution/          # Motor de ejecución
│   ├── risk/               # Gestión de riesgos
│   └── api/                # API para health-check
├── tests/                  # Pruebas unitarias
├── docs/                   # Documentación
├── pyproject.toml          # Configuración de Poetry
├── Dockerfile              # Configuración para Docker
└── .env.example            # Plantilla para variables de entorno
```

## Licencia

MIT

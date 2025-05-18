# Funding Rate Arbitrage

This project implements a funding rate arbitrage strategy for perpetual swaps between multiple cryptocurrency exchanges. The strategy exploits differences in funding rates across exchanges to generate profits with market-neutral positions.

## Features

- Real-time monitoring of funding rates across multiple exchanges
- Automatic calculation of funding rate spreads and arbitrage opportunities
- Asynchronous order execution to open and close arbitrage positions
- Configurable risk management and position limits
- Detailed trade logging and P&L
- Simulation/backtesting mode for strategy testing
- Health-check endpoint for monitoring

## Requirements

- Python 3.11+
- Poetry (dependency manager)
- API keys for the exchanges to be used

## Installation

1. Install dependencies with Poetry:
```bash
poetry install
```

2. Configure environment variables:
```bash
cp .env.example .env
# Edit .env with your API keys and settings
```

## Usage

### Production Mode

```bash
poetry run python -m src.main
```

### Simulation/Backtesting Mode

```bash
poetry run python -m src.backtest --start-date 2023-01-01 --end-date 2023-01-31
```

## Configuration

The `.env` file allows you to configure:

- API keys for each exchange
- Trading pairs (e.g., BINANCE:BTC/USDT, BYBIT:BTC-PERP)
- Minimum funding rate spread threshold
- Maximum position size per exchange
- Risk limits and stop-loss

## Deployment with Docker

1. Build the image:
```bash
docker build -t funding-rate-arbitrage .

``` ```

2. Run the container:
```bash
docker run -d --name funding-arb --env-file .env funding-rate-arbitrage
```

## Monitoring

The project includes a health-check endpoint at `http://localhost:8000/health` that provides:
- Current funding rate differential
- Open positions
- Uptime
- Performance metrics

For Prometheus/Grafana integration, see the documentation in `/docs/monitoring.md`.

## Project Structure

```
funding-rate-arbitrage/
├── src/ # Main source code
│ ├── __init__.py
│ ├── main.py # Main entry point
│ ├── config.py # Configuration and loading variables
│ ├── exchanges/ # Connectors for exchanges
│ ├── models/ # Data models
│ ├── execution/ # Execution engine
│ ├── risk/ # Risk management
│ └── api/ # API for health-check
├── tests/ # Unit tests
├── docs/ # Documentation
├── pyproject.toml # Poetry Configuration
├── Dockerfile # Configuration for Docker
└── .env.example # Template for environment variables
```

## License

MIT

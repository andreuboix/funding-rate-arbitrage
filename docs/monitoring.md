# Integración con Prometheus y Grafana

Este documento describe cómo integrar el sistema de arbitraje de funding rate con Prometheus y Grafana para monitoreo y visualización.

## Configuración de Prometheus

### 1. Instalación de Prometheus

Si aún no tiene Prometheus instalado, puede seguir las [instrucciones oficiales](https://prometheus.io/docs/prometheus/latest/installation/).

### 2. Configuración para scraping

Añada la siguiente configuración a su archivo `prometheus.yml`:

```yaml
scrape_configs:
  - job_name: 'funding-rate-arbitrage'
    scrape_interval: 15s
    metrics_path: /metrics
    static_configs:
      - targets: ['funding-arbitrage:8000']  # Ajustar según el nombre del host/contenedor
```

### 3. Reiniciar Prometheus

```bash
sudo systemctl restart prometheus
```

## Configuración de Grafana

### 1. Instalación de Grafana

Si aún no tiene Grafana instalado, puede seguir las [instrucciones oficiales](https://grafana.com/docs/grafana/latest/installation/).

### 2. Añadir Prometheus como fuente de datos

1. Acceda a Grafana (por defecto en http://localhost:3000)
2. Vaya a Configuración > Fuentes de datos
3. Haga clic en "Añadir fuente de datos"
4. Seleccione "Prometheus"
5. Configure la URL (por ejemplo, http://prometheus:9090)
6. Haga clic en "Guardar & Test"

### 3. Importar dashboard

Puede crear un nuevo dashboard o importar el dashboard de ejemplo proporcionado en `docs/grafana_dashboard.json`.

Para importar:
1. Vaya a Dashboards > Importar
2. Cargue el archivo JSON o pegue su contenido
3. Seleccione la fuente de datos de Prometheus
4. Haga clic en "Importar"

## Métricas disponibles

El sistema expone las siguientes métricas en el endpoint `/metrics`:

| Métrica | Tipo | Descripción |
|---------|------|-------------|
| `arbitrage_uptime_seconds` | Gauge | Tiempo de actividad en segundos |
| `arbitrage_active_positions` | Gauge | Número de posiciones de arbitraje activas |
| `arbitrage_daily_pnl` | Gauge | P&L diario acumulado |
| `arbitrage_funding_rate` | Gauge | Tasa de funding por exchange y símbolo |
| `arbitrage_funding_diff` | Gauge | Diferencial de funding rate para posiciones activas |

## Dashboard recomendado

El dashboard recomendado incluye los siguientes paneles:

1. **Estado general**
   - Tiempo de actividad
   - Número de posiciones activas
   - P&L diario

2. **Tasas de funding**
   - Gráfico de tasas de funding por exchange y símbolo
   - Tabla de tasas actuales

3. **Posiciones activas**
   - Tabla de posiciones con diferenciales
   - Gráfico de P&L por posición

4. **Alertas**
   - Configuración de alertas para:
     - Caídas en el P&L
     - Posiciones con tiempo de mantenimiento prolongado
     - Diferenciales de funding por debajo del umbral

## Configuración de alertas

### Ejemplo de alerta para P&L negativo

```yaml
groups:
- name: funding-arbitrage
  rules:
  - alert: NegativeDailyPnL
    expr: arbitrage_daily_pnl < -300
    for: 5m
    labels:
      severity: warning
    annotations:
      summary: "P&L diario negativo"
      description: "El P&L diario ha caído por debajo de -$300"
```

### Ejemplo de alerta para posiciones activas

```yaml
groups:
- name: funding-arbitrage
  rules:
  - alert: NoActivePositions
    expr: arbitrage_active_positions == 0
    for: 1h
    labels:
      severity: info
    annotations:
      summary: "Sin posiciones activas"
      description: "No hay posiciones activas durante la última hora"
```

## Despliegue conjunto

Para un despliegue conjunto con Docker Compose, puede utilizar la siguiente configuración:

```yaml
version: '3'

services:
  funding-arbitrage:
    build: .
    container_name: funding-arbitrage
    env_file: .env
    ports:
      - "8000:8000"
    volumes:
      - ./logs:/app/logs
    restart: unless-stopped

  prometheus:
    image: prom/prometheus
    container_name: prometheus
    ports:
      - "9090:9090"
    volumes:
      - ./prometheus.yml:/etc/prometheus/prometheus.yml
    restart: unless-stopped

  grafana:
    image: grafana/grafana
    container_name: grafana
    ports:
      - "3000:3000"
    volumes:
      - grafana-storage:/var/lib/grafana
    depends_on:
      - prometheus
    restart: unless-stopped

volumes:
  grafana-storage:
```

Guarde este archivo como `docker-compose.yml` y ejecute:

```bash
docker-compose up -d
```

Esto iniciará el sistema de arbitraje junto con Prometheus y Grafana para monitoreo.

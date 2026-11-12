# Pipeline ETL Resiliente con Prefect 3, SQLite y uv

Pipeline ETL modular e independiente para **múltiples fuentes de telemetría**, utilizando **Prefect 3** para orquestación con subflows, **SQLite** como almacenamiento de métricas y **uv** como gestor de dependencias.

## Estructura del proyecto

```
.
├── data/
│   ├── raw/
│   │   ├── telemetry_events.jsonl      ← Datos de telemetría web (JSONL)
│   │   └── product_events.jsonl        ← Datos de eventos de producto (JSONL)
│   ├── processed/                       ← Snapshots exportados
│   ├── reporting/
│   │   ├── business_pis.sqlite          ← Base de datos SQLite con KPIs consolidados
│   │   ├── latest_kpis.json             ← Últimos KPIs exportados
│   │   └── pipeline_runs.jsonl          ← Historial de ejecuciones
│   └── pipelines/
│       └── pipeline.py                  ← Código del pipeline ETL
├── pyproject.toml                       ← Dependencias (uv)
└── README.md
```

## Arquitectura del pipeline

```
business_performance_pipeline (flow principal)
  │
  ├── web_telemetry_subflow          ← Extrae, transforma y calcula KPIs desde
  │   └── telemetry_events.jsonl        telemetry_events.jsonl
  │
  ├── product_telemetry_subflow      ← Extrae, transforma y calcula KPIs desde
  │   └── product_events.jsonl          product_events.jsonl
  │
  └── Consolidación de KPIs
      ├── load_kpis (SQLite UPSERT)
      ├── export_eval_snapshot (JSON)
      └── _record_pipeline_run (historial)
```

## Componentes del pipeline

| Tarea / Función                  | Decorador      | Descripción                                                                 |
|----------------------------------|----------------|-----------------------------------------------------------------------------|
| `extract_events`                 | `@task`        | Lee JSONL; si no existe, genera datos de muestra. Reintentos: 3×2s.         |
| `transform_events`               | `@task`        | Limpia/filtra corruptos, inválidos y duplicados. Caché: 1 hora.             |
| `calculate_kpis`                 | `@task`        | Calcula total eventos, usuarios únicos, compras, ingresos, conversión, error. |
| `load_kpis`                      | `@task`        | UPSERT en SQLite (idempotente). Reintentos: 2×3s.                            |
| `export_eval_snapshot`           | `@task`        | Exporta snapshot JSON. Soporta `fail_intentionally=True` para pruebas.       |
| `_record_pipeline_run`           | — (función)    | Registro persistente de metadatos en `pipeline_runs.jsonl`.                  |
| `web_telemetry_subflow`          | `@flow`        | Subflow: procesa telemetry_events.jsonl → KPIs individuales.                |
| `product_telemetry_subflow`      | `@flow`        | Subflow: procesa product_events.jsonl → KPIs individuales.                  |
| `business_performance_pipeline`  | `@flow`        | Flow principal: orquesta ambos subflows y consolida KPIs.                   |

## Requisitos

- Python ≥ 3.11
- [uv](https://docs.astral.sh/uv/) (gestor de proyectos Python)

## Instalación

```bash
# 1. Sincronizar dependencias con uv
uv sync

# 2. Verificar que todo está instalado
uv run python -c "import prefect; print(prefect.__version__)"
```

## Ejecución

```bash
# Ejecutar el pipeline completo (desde la raíz del proyecto)
uv run python data/pipelines/pipeline.py
```

> El pipeline busca primero `data/raw/telemetry_events.jsonl` y `data/raw/product_events.jsonl`.
> Si alguno no existe, genera datos de muestra automáticamente.

### Probar resiliencia (fallo controlado en exportación)

Ejecuta el pipeline con `fail_export=True` para verificar que un fallo en la
exportación **no detiene** el flujo principal:

```bash
cd /workspaces/latam-aie-pt3 && source .venv/bin/activate && \
PREFECT_SERVER_ANALYTICS_ENABLED=false uv run python -c "
from data.pipelines.pipeline import business_performance_pipeline
result = business_performance_pipeline(fail_export=True)
print('Pipeline completado')
"
```

**Salida esperada**: la tarea `export_eval_snapshot` lanza `RuntimeError`,
pero el flujo muestra `Pipeline completado exitosamente` y finaliza en estado
`Completed()`. Ambos subflows se ejecutan sin problemas.

## Servidor UI de Prefect

En GitHub Codespaces, Python accede a la API por localhost, pero el navegador
debe usar la URL HTTPS del puerto reenviado. Ejecuta desde la raíz del proyecto:

```bash
# Terminal 1: guardar ambas direcciones en el perfil activo
PREFECT_PUBLIC_URL="https://${CODESPACE_NAME}-4200.${GITHUB_CODESPACES_PORT_FORWARDING_DOMAIN}"
uv run prefect config set \
	PREFECT_API_URL=http://127.0.0.1:4200/api \
	PREFECT_UI_API_URL="$PREFECT_PUBLIC_URL/api"

printf 'Abre la UI en: %s\n' "$PREFECT_PUBLIC_URL"
uv run prefect server start --host 0.0.0.0
```

Si ya hay un servidor activo en el puerto 4200, detenlo con Ctrl+C en su
terminal antes de iniciarlo con esta configuración. No borres la base de datos.
Repite la configuración si cambia el nombre del Codespace.

```bash
# Terminal 2: comprobar la API y ejecutar el pipeline
curl --fail http://127.0.0.1:4200/api/health
uv run python data/pipelines/pipeline.py
```

En el panel **Puertos**, reenvía el puerto **4200**, mantenlo **privado** y
abre su dirección HTTPS en el navegador con tu sesión de GitHub iniciada.
No configures el protocolo interno del puerto como HTTPS: Prefect sirve HTTP;
Codespaces proporciona HTTPS externamente.

Si aparece `Failed to fetch` o `Mixed Content`, comprueba:

```bash
curl --fail http://127.0.0.1:4200/ui-settings
```

El campo `api_url` debe mostrar la URL HTTPS del Codespace terminada en `/api`,
no localhost ni `0.0.0.0`. Tras reiniciar el servidor, recarga la UI con
Ctrl+Shift+R. Cambiar solo `PREFECT_API_URL` no corrige la dirección de la UI.

Para trabajar fuera de Codespaces, usa
`uv run prefect config set PREFECT_API_URL=http://127.0.0.1:4200/api PREFECT_UI_API_URL=http://127.0.0.1:4200/api`
y abre http://127.0.0.1:4200.

Desde la UI puedes:

- Ver el historial de ejecuciones del flujo
- Inspeccionar logs de cada tarea
- Visualizar el DAG del pipeline
- Monitorear reintentos y estados fallidos

## Tests

La suite de pruebas cubre todas las tareas, subflows y el flujo principal del pipeline.

### Requisito

```bash
uv add --dev pytest
```

### Ejecutar todos los tests

```bash
cd /workspaces/latam-aie-pt3 && source .venv/bin/activate && \
PREFECT_SERVER_ANALYTICS_ENABLED=false uv run pytest test/ -v --tb=short
```

### Ejecutar un test específico

```bash
# Una clase de test
PREFECT_SERVER_ANALYTICS_ENABLED=false uv run pytest test/test_pipeline.py::TestExtract -v --tb=short

# Un test concreto
PREFECT_SERVER_ANALYTICS_ENABLED=false uv run pytest test/test_pipeline.py::TestTransform::test_removes_corrupt -v --tb=short

# Un subflow
PREFECT_SERVER_ANALYTICS_ENABLED=false uv run pytest test/test_pipeline.py::TestSubflows -v --tb=short
```

### Cobertura de tests (31 tests)

| Clase de test         | Tests | ¿Qué cubre?                                      |
|-----------------------|-------|--------------------------------------------------|
| `TestExtract`         | 4     | Lectura JSONL, líneas corruptas, generación de sample (web y producto) |
| `TestTransform`       | 4     | Limpieza web: corruptos, duplicados, normalización revenue, vacío |
| `TestTransformProduct`| 4     | Aplanado estructura anidada, mapeo action→event_type, revenue desde rev |
| `TestCalculateKPIs`   | 4     | Métricas base, tasa conversión, cero eventos, action="purchase" |
| `TestLoad`            | 2     | Creación tabla SQLite, UPSERT idempotente        |
| `TestExport`          | 2     | Snapshot JSON, fail_intentionally=True           |
| `TestSubflows`        | 3     | subflow web, subflow producto, datos corruptos   |
| `TestMainFlow`        | 2     | Pipeline completo, pipeline con fail_export      |
| `TestOutput`          | 3     | SQLite con KPIs, JSON exportado, historial runs  |
| `TestUtils`           | 3     | Generación de samples, registro de ejecuciones   |

## Verificar resultados

```bash
# Ver los KPIs en SQLite
sqlite3 data/reporting/business_pis.sqlite "SELECT * FROM kpi_window;"

# Ver el último snapshot JSON
cat data/reporting/latest_kpis.json

# Ver el historial de ejecuciones
cat data/reporting/pipeline_runs.jsonl | head -5
```

## Principios de diseño

- **Modularidad**: cada fase es una `@task` independiente, reutilizable y aislada.
- **Resiliencia**: reintentos automáticos en extract y load; tarea opcional de exportación que no bloquea el flujo.
- **Idempotencia**: UPSERT en SQLite evita duplicados en cargas repetidas.
- **Caché inteligente**: transform_events usa `task_input_hash` para evitar reprocesar datos idénticos dentro de 1 hora.
- **Registro persistente**: cada ejecución queda documentada en `pipeline_runs.jsonl`.
- **Cero dependencias externas**: solo necesita `prefect` y la stdlib de Python.

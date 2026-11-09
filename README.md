# Pipeline ETL Resiliente con Prefect 3, SQLite y uv

Pipeline ETL modular e independiente para telemetría de eventos, utilizando **Prefect 3** para orquestación, **SQLite** como almacenamiento de métricas y **uv** como gestor de dependencias.

## Estructura del proyecto

```
.
├── data/
│   ├── raw/
│   │   └── telemetry_events.jsonl      ← Datos de entrada (JSONL)
│   ├── processed/                       ← Snapshots exportados
│   ├── reporting/
│   │   ├── business_pis.sqlite          ← Base de datos SQLite con KPIs
│   │   ├── latest_kpis.json             ← Últimos KPIs exportados
│   │   └── pipeline_runs.jsonl          ← Historial de ejecuciones
│   └── pipelines/
│       └── pipeline.py                  ← Código del pipeline ETL
├── pyproject.toml                       ← Dependencias (uv)
└── README.md
```

## Componentes del pipeline

| Tarea / Función         | Decorador      | Descripción                                                                 |
|-------------------------|----------------|-----------------------------------------------------------------------------|
| `extract_events`        | `@task`        | Lee JSONL; si no existe, genera datos de muestra. Reintentos: 3×2s.        |
| `transform_events`      | `@task`        | Limpia/filtra corruptos, inválidos y duplicados. Caché: 1 hora.            |
| `calculate_kpis`        | `@task`        | Calcula total eventos, usuarios únicos, compras, ingresos, conversión, error. |
| `load_kpis`             | `@task`        | UPSERT en SQLite (idempotente). Reintentos: 2×3s.                           |
| `export_eval_snapshot`  | `@task`        | Exporta snapshot JSON. Soporta `fail_intentionally=True` para pruebas.      |
| `_record_pipeline_run`  | — (función)    | Registro persistente de metadatos en `pipeline_runs.jsonl`.                 |
| `business_performance_pipeline` | `@flow` | Flujo principal que encadena todas las tareas.                              |

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

> El pipeline primero busca `data/raw/telemetry_events.jsonl`. Si no existe, genera 50 eventos de muestra automáticamente.

### Probar resiliencia (fallo controlado en exportación)

Ejecuta el pipeline con `fail_export=True` para verificar que un fallo en la
exportación **no detiene** el flujo principal. Usa el comando con `-c` (no
escribas `business_performance_pipeline(...)` directamente en la terminal bash):

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
`Completed()`. Las tareas previas (extract, transform, KPIs, load) se ejecutan
sin problemas.

## Servidor UI de Prefect

Prefect 3 incluye una interfaz web para monitorear ejecuciones, logs y el estado de cada tarea:

```bash
# 1. Iniciar el servidor local de Prefect
# En la primera ejecución te pedirá crear un perfil — selecciona la opción 1
# y ponle un nombre (ej: "prefect-server")
uv run prefect server start

# 2. En otra terminal, configurar la URL de la API y ejecutar el pipeline
uv run prefect config set PREFECT_API_URL=http://0.0.0.0:4200/api
uv run python data/pipelines/pipeline.py
```

> ⚠️ **Importante**: Sin configurar `PREFECT_API_URL`, el pipeline iniciará un servidor **temporal** efímero y no podrás ver los resultados en la UI. Sigue los pasos 1 y 2 para una experiencia completa.

Esto levanta la UI en http://localhost:4200. Desde allí puedes:

- Ver el historial de ejecuciones del flujo
- Inspeccionar logs de cada tarea
- Visualizar el DAG del pipeline
- Monitorear reintentos y estados fallidos

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

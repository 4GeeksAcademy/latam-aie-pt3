"""
pipeline.py — Pipeline ETL resiliente, modular e independiente con Prefect 3.

Cada fase (Extracción, Transformación, Cálculo de KPIs, Carga y Exportación)
se ejecuta de forma independiente a través de tareas decoradas con @task,
permitiendo reutilización, aislamiento de errores y reintentos automáticos.
"""

from __future__ import annotations

import hashlib
import json
import os
import sqlite3
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from prefect import flow, task
from prefect.tasks import task_input_hash

# ──────────────────────────────────────────────
# Constantes de rutas (relativas al directorio del proyecto)
# ──────────────────────────────────────────────
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
DATA_RAW = PROJECT_ROOT / "data" / "raw"
DATA_PROCESSED = PROJECT_ROOT / "data" / "processed"
DATA_REPORTING = PROJECT_ROOT / "data" / "reporting"
DB_PATH = DATA_REPORTING / "business_pis.sqlite"
RUNS_JSONL = DATA_REPORTING / "pipeline_runs.jsonl"
KPI_EXPORT = DATA_REPORTING / "latest_kpis.json"

# ──────────────────────────────────────────────
# 1. EXTRACT
# ──────────────────────────────────────────────


def _generate_sample_events(n: int = 50) -> list[dict[str, Any]]:
    """Genera datos de muestra reproducibles (semilla fija)."""
    import random

    rng = random.Random(42)
    users = [f"user_{i}" for i in range(1, 11)]
    events = []
    base_ts = datetime(2025, 1, 1, tzinfo=timezone.utc)

    for i in range(n):
        user = rng.choice(users)
        ts = base_ts + timedelta(hours=i)
        event_type = rng.choices(
            ["page_view", "purchase", "login", "error", "logout"],
            weights=[40, 10, 25, 5, 20],
            k=1,
        )[0]
        event = {
            "event_id": f"evt-{i:04d}",
            "user_id": user,
            "timestamp": ts.isoformat(),
            "event_type": event_type,
            "revenue": round(rng.uniform(10.0, 200.0), 2)
            if event_type == "purchase"
            else 0.0,
        }
        # Introducir un par de registros corruptos deliberadamente
        if i in (3, 17):
            event["_corrupt"] = True
        events.append(event)
    return events


@task(
    retries=3,
    retry_delay_seconds=2,
    name="extract_events",
)
def extract_events(source_path: str | None = None) -> list[dict[str, Any]]:
    """Lee eventos desde un archivo JSONL.

    Si la fuente no existe, genera datos de muestra reproducibles.
    Valida línea por línea el formato JSON.
    """
    if source_path is None:
        source_path = str(DATA_RAW / "telemetry_events.jsonl")

    path = Path(source_path)

    if not path.exists():
        print(f"[extract] Fuente no encontrada: {path}. Generando datos de muestra…")
        sample = _generate_sample_events(50)
        path.parent.mkdir(parents=True, exist_ok=True)
        with open(path, "w", encoding="utf-8") as f:
            for evt in sample:
                f.write(json.dumps(evt) + "\n")
        print(f"[extract] {len(sample)} eventos de muestra escritos en {path}")
        return sample

    raw_events: list[dict[str, Any]] = []
    with open(path, "r", encoding="utf-8") as f:
        for lineno, line in enumerate(f, start=1):
            line = line.strip()
            if not line:
                continue
            try:
                raw_events.append(json.loads(line))
            except json.JSONDecodeError:
                print(f"[extract] Línea {lineno} ignorada: JSON inválido")

    print(f"[extract] {len(raw_events)} eventos extraídos desde {path}")
    return raw_events


# ──────────────────────────────────────────────
# 2. TRANSFORM
# ──────────────────────────────────────────────


@task(
    cache_key_fn=task_input_hash,
    cache_expiration=timedelta(hours=1),
    name="transform_events",
)
def transform_events(raw_events: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Limpia y filtra registros corruptos, inválidos o duplicados.

    El resultado se cachea durante 1 hora: si los mismos datos de entrada
    se presentan de nuevo, se retorna el resultado cachead en lugar de
    reprocesar.
    """
    seen_ids: set[str] = set()
    clean: list[dict[str, Any]] = []

    for evt in raw_events:
        # 1. Descartar corruptos
        if evt.get("_corrupt"):
            print(f"[transform] Descartado registro corrupto: {evt.get('event_id')}")
            continue

        # 2. Validar campo event_id
        eid = evt.get("event_id")
        if not isinstance(eid, str) or not eid:
            print("[transform] Descartado registro sin event_id válido")
            continue

        # 3. Eliminar duplicados
        if eid in seen_ids:
            print(f"[transform] Duplicado descartado: {eid}")
            continue
        seen_ids.add(eid)

        # 4. Normalizar campos numéricos
        evt["revenue"] = float(evt.get("revenue", 0.0))
        clean.append(evt)

    print(f"[transform] {len(clean)} eventos limpios (de {len(raw_events)} brutos)")
    return clean


# ──────────────────────────────────────────────
# 3. CALCULATE KPIs
# ──────────────────────────────────────────────


@task(name="calculate_kpis")
def calculate_kpis(events: list[dict[str, Any]]) -> dict[str, Any]:
    """Calcula métricas de negocio esenciales a partir de eventos limpios."""
    total_events = len(events)
    unique_users = len({e["user_id"] for e in events})
    purchase_events = [e for e in events if e.get("event_type") == "purchase"]
    error_events = [e for e in events if e.get("event_type") == "error"]
    total_revenue = sum(e["revenue"] for e in purchase_events)
    conversion_rate = round(len(purchase_events) / total_events, 4) if total_events else 0.0
    error_rate = round(len(error_events) / total_events, 4) if total_events else 0.0

    kpis = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "total_events": total_events,
        "unique_users": unique_users,
        "purchase_events": len(purchase_events),
        "total_revenue": round(total_revenue, 2),
        "conversion_rate": conversion_rate,
        "error_rate": error_rate,
    }
    print(f"[kpis] Métricas calculadas: {json.dumps(kpis, indent=2)}")
    return kpis


# ──────────────────────────────────────────────
# 4. LOAD (SQLite UPSERT)
# ──────────────────────────────────────────────


def _ensure_schema(conn: sqlite3.Connection) -> None:
    """Crea la tabla kpi_window si no existe."""
    conn.execute("""
        CREATE TABLE IF NOT EXISTS kpi_window (
            id INTEGER PRIMARY KEY CHECK (id = 1),
            timestamp TEXT NOT NULL,
            total_events INTEGER NOT NULL,
            unique_users INTEGER NOT NULL,
            purchase_events INTEGER NOT NULL,
            total_revenue REAL NOT NULL,
            conversion_rate REAL NOT NULL,
            error_rate REAL NOT NULL
        )
    """)


@task(
    retries=2,
    retry_delay_seconds=3,
    name="load_kpis",
)
def load_kpis(kpis: dict[str, Any]) -> None:
    """Inserta o actualiza (UPSERT) la ventana de métricas en SQLite."""
    db_path = str(DB_PATH)
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)

    conn = sqlite3.connect(db_path)
    try:
        _ensure_schema(conn)
        conn.execute(
            """
            INSERT INTO kpi_window (
                id, timestamp, total_events, unique_users,
                purchase_events, total_revenue, conversion_rate, error_rate
            ) VALUES (1, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(id) DO UPDATE SET
                timestamp          = excluded.timestamp,
                total_events       = excluded.total_events,
                unique_users       = excluded.unique_users,
                purchase_events    = excluded.purchase_events,
                total_revenue      = excluded.total_revenue,
                conversion_rate    = excluded.conversion_rate,
                error_rate         = excluded.error_rate
            """,
            (
                kpis["timestamp"],
                kpis["total_events"],
                kpis["unique_users"],
                kpis["purchase_events"],
                kpis["total_revenue"],
                kpis["conversion_rate"],
                kpis["error_rate"],
            ),
        )
        conn.commit()
        print(f"[load] KPIs escritos en {db_path}")
    finally:
        conn.close()


# ──────────────────────────────────────────────
# 5. EXPORT / SNAPSHOT (opcional, con fallo simulado)
# ──────────────────────────────────────────────


@task(name="export_eval_snapshot")
def export_eval_snapshot(
    kpis: dict[str, Any],
    sample_events: list[dict[str, Any]] | None = None,
    fail_intentionally: bool = False,
) -> dict[str, Any]:
    """Exporta un snapshot JSON con KPIs y una muestra de eventos.

    Si ``fail_intentionally=True``, simula un fallo controlado para probar
    la resiliencia del flujo.
    """
    if fail_intentionally:
        msg = "[export] Fallo controlado simulado (fail_intentionally=True)"
        print(msg)
        raise RuntimeError(msg)

    snapshot = {
        "exported_at": datetime.now(timezone.utc).isoformat(),
        "kpis": kpis,
        "sample_size": len(sample_events) if sample_events else 0,
    }
    dest = DATA_PROCESSED / f"snapshot_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}.json"
    dest.parent.mkdir(parents=True, exist_ok=True)

    with open(dest, "w", encoding="utf-8") as f:
        json.dump(snapshot, f, indent=2, ensure_ascii=False)

    # También exportar latest_kpis.json
    KPI_EXPORT.parent.mkdir(parents=True, exist_ok=True)
    with open(KPI_EXPORT, "w", encoding="utf-8") as f:
        json.dump(kpis, f, indent=2, ensure_ascii=False)

    print(f"[export] Snapshot guardado en {dest}")
    print(f"[export] KPIs exportados a {KPI_EXPORT}")
    return snapshot


# ──────────────────────────────────────────────
# UTILIDAD: registro persistente de ejecuciones
# ──────────────────────────────────────────────


def _record_pipeline_run(metadata: dict[str, Any]) -> None:
    """Registra metadatos de cada ejecución en pipeline_runs.jsonl."""
    RUNS_JSONL.parent.mkdir(parents=True, exist_ok=True)
    record = {
        "run_id": hashlib.sha256(
            json.dumps(metadata, sort_keys=True).encode()
        ).hexdigest()[:12],
        "timestamp": datetime.now(timezone.utc).isoformat(),
        **metadata,
    }
    with open(RUNS_JSONL, "a", encoding="utf-8") as f:
        f.write(json.dumps(record) + "\n")
    print(f"[records] Ejecución registrada en {RUNS_JSONL}")


# ──────────────────────────────────────────────
# FLUJO PRINCIPAL
# ──────────────────────────────────────────────


@flow(
    name="business_performance_pipeline",
    log_prints=True,
    retries=0,
)
def business_performance_pipeline(
    source_path: str | None = None,
    fail_export: bool = False,
) -> dict[str, Any]:
    """Flujo principal que encadena Extracción → Transformación → KPIs → Carga → Exportación.

    Parámetros
    ----------
    source_path : str, opcional
        Ruta al archivo JSONL de entrada. Si es ``None``, se usa el valor
        por defecto dentro de ``data/raw/``.
    fail_export : bool
        Si es ``True``, la tarea de exportación lanza una excepción (para
        probar la resiliencia). El flujo principal **no** falla porque la
        exportación es opcional.

    Retorna
    -------
    dict
        Resumen con KPIs, estado de cada etapa y metadatos de la ejecución.
    """
    ts = datetime.now(timezone.utc)

    # 1. Extracción
    raw = extract_events(source_path)

    # 2. Transformación
    clean = transform_events(raw)

    # 3. Cálculo de KPIs
    kpis = calculate_kpis(clean)

    # 4. Carga a SQLite
    load_kpis(kpis)

    # 5. Exportación (opcional — el flujo continúa aunque falle)
    export_state = None
    try:
        export_eval_snapshot(kpis, clean, fail_intentionally=fail_export)
    except Exception as exc:
        print(f"[pipeline] Exportación opcional falló (ignorando): {exc}")

    # 6. Registrar metadatos de la ejecución
    metadata = {
        "source_path": source_path or str(DATA_RAW / "telemetry_events.jsonl"),
        "raw_count": len(raw),
        "clean_count": len(clean),
        "kpis": kpis,
        "fail_export_requested": fail_export,
        "status": "completed",
    }
    _record_pipeline_run(metadata)

    print("\n" + "=" * 50)
    print("   Pipeline completado exitosamente")
    print("=" * 50)
    return metadata


# ──────────────────────────────────────────────
# Entry point
# ──────────────────────────────────────────────

if __name__ == "__main__":
    # Ejecutar el flujo con fail_export=False por defecto
    business_performance_pipeline(fail_export=False)  # type: ignore[call-overload]
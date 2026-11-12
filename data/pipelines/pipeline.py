"""
pipeline.py — Pipeline ETL resiliente, modular e independiente con Prefect 3.

Arquitectura multi-fuente con subflows:
  business_performance_pipeline (flow principal)
    ├── web_telemetry_subflow       (origen: telemetry_events.jsonl)
    └── product_telemetry_subflow   (origen: product_events.jsonl)

Cada subflow extrae, transforma y calcula KPIs de su fuente.
El flow principal consolida, carga y exporta.
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
# 1. EXTRACT (genérica para cualquier fuente)
# ──────────────────────────────────────────────


def _generate_sample_telemetry(n: int = 50) -> list[dict[str, Any]]:
    """Genera datos de muestra de telemetría web (semilla fija 42)."""
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
        if i in (3, 17):
            event["_corrupt"] = True
        events.append(event)
    return events


def _generate_sample_product_events(n: int = 30) -> list[dict[str, Any]]:
    """Genera datos de muestra de eventos de producto con estructura anidada (semilla fija 99).

    Estructura diferente a telemetry_events:
      { "id": ..., "user": ..., "event": { "ts": ..., "product_id": ..., ... }, "rev": ... }
    """
    import random

    rng = random.Random(99)
    products = ["prod_A", "prod_B", "prod_C", "prod_D", "prod_E"]
    categories = ["electronics", "clothing", "home", "sports", "books"]
    users = [f"user_{i}" for i in range(1, 16)]
    events = []
    base_ts = datetime(2025, 1, 1, tzinfo=timezone.utc)

    for i in range(n):
        user = rng.choice(users)
        ts = base_ts + timedelta(hours=i * 2)
        product = rng.choice(products)
        action = rng.choices(
            ["view", "add_to_cart", "purchase", "remove", "review"],
            weights=[30, 20, 15, 5, 30],
            k=1,
        )[0]
        qty = rng.randint(1, 5) if action == "purchase" else 0
        price = round(rng.uniform(5.0, 500.0), 2)
        # Estructura anidada intencionalmente diferente
        event = {
            "id": f"pevt-{i:04d}",
            "user": user,
            "event": {
                "ts": ts.isoformat(),
                "product_id": product,
                "category": rng.choice(categories),
                "action": action,
                "qty": qty,
                "price": price,
            },
            "rev": round(qty * price, 2) if action == "purchase" else 0.0,
        }
        if i in (7, 22):
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

    Si la fuente no existe, genera datos de muestra reproducibles
    (telemetría web por defecto o producto según el nombre del archivo).
    Valida línea por línea el formato JSON.
    """
    if source_path is None:
        source_path = str(DATA_RAW / "telemetry_events.jsonl")

    path = Path(source_path)

    if not path.exists():
        is_product = "product" in path.name.lower()
        print(f"[extract] Fuente no encontrada: {path}. Generando datos de muestra…")
        if is_product:
            sample = _generate_sample_product_events(30)
        else:
            sample = _generate_sample_telemetry(50)
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
# 2. TRANSFORM (genérica)
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
        if evt.get("_corrupt"):
            print(f"[transform] Descartado registro corrupto: {evt.get('event_id')}")
            continue
        eid = evt.get("event_id")
        if not isinstance(eid, str) or not eid:
            print("[transform] Descartado registro sin event_id válido")
            continue
        if eid in seen_ids:
            print(f"[transform] Duplicado descartado: {eid}")
            continue
        seen_ids.add(eid)
        evt["revenue"] = float(evt.get("revenue", 0.0))
        clean.append(evt)

    print(f"[transform] {len(clean)} eventos limpios (de {len(raw_events)} brutos)")
    return clean


# ──────────────────────────────────────────────
# 2b. TRANSFORM (específica para producto: aplanar estructura anidada)
# ──────────────────────────────────────────────


@task(
    cache_key_fn=task_input_hash,
    cache_expiration=timedelta(hours=1),
    name="transform_product_events",
)
def transform_product_events(raw_events: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Limpia, filtra y aplana eventos de producto.

    Los eventos de producto llegan con estructura anidada:
      { "id": ..., "user": ..., "event": { "ts": ..., "product_id": ..., "action": ..., "qty": ..., "price": ... }, "rev": ... }

    Se normaliza a la misma estructura plana que transform_events:
      { "event_id": ..., "user_id": ..., "timestamp": ..., "event_type": ..., "revenue": ... }
    """
    seen_ids: set[str] = set()
    clean: list[dict[str, Any]] = []

    for evt in raw_events:
        # 1. Descartar corruptos
        if evt.get("_corrupt"):
            print(f"[transform-product] Descartado registro corrupto: {evt.get('id')}")
            continue

        # 2. Validar campo id
        eid = evt.get("id")
        if not isinstance(eid, str) or not eid:
            print("[transform-product] Descartado registro sin id válido")
            continue

        # 3. Eliminar duplicados
        if eid in seen_ids:
            print(f"[transform-product] Duplicado descartado: {eid}")
            continue
        seen_ids.add(eid)

        # 4. Aplanar: extraer evento anidado
        nested = evt.get("event", {})
        if not isinstance(nested, dict):
            print(f"[transform-product] Descartado {eid}: event no es un diccionario")
            continue

        # 5. Determinar event_type basado en action
        action = nested.get("action", "")
        if action == "purchase":
            event_type = "purchase"
        elif action in ("view", "add_to_cart", "remove", "review"):
            event_type = action
        else:
            event_type = action

        normalized = {
            "event_id": eid,
            "user_id": evt.get("user", "unknown"),
            "timestamp": nested.get("ts", ""),
            "event_type": event_type,
            "revenue": float(evt.get("rev", 0.0)),
        }
        clean.append(normalized)

    print(f"[transform-product] {len(clean)} eventos normalizados (de {len(raw_events)} brutos)")
    return clean


# ──────────────────────────────────────────────
# 3. CALCULATE KPIs (genérica)
# ──────────────────────────────────────────────


@task(name="calculate_kpis")
def calculate_kpis(events: list[dict[str, Any]]) -> dict[str, Any]:
    """Calcula métricas de negocio esenciales a partir de eventos limpios."""
    total_events = len(events)
    unique_users = len({e["user_id"] for e in events})
    purchase_events = [e for e in events if e.get("event_type") == "purchase" or e.get("action") == "purchase"]
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
# SUBFLOW 1: Web Telemetry
# ──────────────────────────────────────────────


@flow(
    name="web-telemetry-subflow",
    log_prints=True,
)
def web_telemetry_subflow(source_path: str | None = None) -> dict[str, Any]:
    """Subflow de telemetría web.

    Extrae eventos desde telemetry_events.jsonl, los transforma y
    calcula KPIs específicos de navegación web.
    """
    if source_path is None:
        source_path = str(DATA_RAW / "telemetry_events.jsonl")

    print(f"\n{'='*50}")
    print(f"   Subflow: web-telemetry-subflow")
    print(f"   Fuente: {source_path}")
    print(f"{'='*50}")

    raw = extract_events(source_path)
    clean = transform_events(raw)
    kpis = calculate_kpis(clean)

    result = {
        "source": "web_telemetry",
        "source_path": source_path,
        "raw_count": len(raw),
        "clean_count": len(clean),
        "kpis": kpis,
        "sample": clean,
    }
    print(f"[web-telemetry-subflow] Completado — {len(clean)} eventos procesados")
    return result


# ──────────────────────────────────────────────
# SUBFLOW 2: Product Telemetry
# ──────────────────────────────────────────────


@flow(
    name="product-telemetry-subflow",
    log_prints=True,
)
def product_telemetry_subflow(source_path: str | None = None) -> dict[str, Any]:
    """Subflow de telemetría de producto.

    Extrae eventos desde product_events.jsonl, los transforma y
    calcula KPIs específicos de interacción con productos.
    """
    if source_path is None:
        source_path = str(DATA_RAW / "product_events.jsonl")

    print(f"\n{'='*50}")
    print(f"   Subflow: product-telemetry-subflow")
    print(f"   Fuente: {source_path}")
    print(f"{'='*50}")

    raw = extract_events(source_path)
    clean = transform_product_events(raw)
    kpis = calculate_kpis(clean)

    result = {
        "source": "product_telemetry",
        "source_path": source_path,
        "raw_count": len(raw),
        "clean_count": len(clean),
        "kpis": kpis,
        "sample": clean,
    }
    print(f"[product-telemetry-subflow] Completado — {len(clean)} eventos procesados")
    return result


# ──────────────────────────────────────────────
# FLUJO PRINCIPAL
# ──────────────────────────────────────────────


@flow(
    name="business_performance_pipeline",
    log_prints=True,
    retries=0,
)
def business_performance_pipeline(
    web_source: str | None = None,
    product_source: str | None = None,
    fail_export: bool = False,
) -> dict[str, Any]:
    """Flujo principal que orquesta 2 subflows para el cálculo consolidado de KPIs.

    Parámetros
    ----------
    web_source : str, opcional
        Ruta al archivo JSONL de telemetría web. Si es ``None``, usa el valor
        por defecto ``data/raw/telemetry_events.jsonl``.
    product_source : str, opcional
        Ruta al archivo JSONL de telemetría de producto. Si es ``None``, usa
        el valor por defecto ``data/raw/product_events.jsonl``.
    fail_export : bool
        Si es ``True``, la tarea de exportación lanza una excepción (para
        probar la resiliencia). El flujo principal **no** falla porque la
        exportación es opcional.

    Retorna
    -------
    dict
        Resumen con KPIs consolidados, estado de cada subflow y metadatos.
    """
    ts = datetime.now(timezone.utc)

    # ── Ejecutar subflows en paralelo (cada uno extrae, transforma, calcula KPIs) ──
    print("\n" + "=" * 50)
    print("   BUSINESS PERFORMANCE PIPELINE")
    print("   Iniciando subflows en paralelo…")
    print("=" * 50)

    web_result = web_telemetry_subflow(web_source)
    product_result = product_telemetry_subflow(product_source)

    # ── Consolidar KPIs de ambas fuentes ──
    web_kpis = web_result["kpis"]
    product_kpis = product_result["kpis"]

    consolidated_kpis = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "total_events": web_kpis["total_events"] + product_kpis["total_events"],
        "unique_users": max(web_kpis["unique_users"], product_kpis["unique_users"]),
        "purchase_events": web_kpis["purchase_events"] + product_kpis["purchase_events"],
        "total_revenue": round(web_kpis["total_revenue"] + product_kpis["total_revenue"], 2),
        "conversion_rate": round(
            (web_kpis["purchase_events"] + product_kpis["purchase_events"])
            / (web_kpis["total_events"] + product_kpis["total_events"]),
            4,
        ) if (web_kpis["total_events"] + product_kpis["total_events"]) > 0 else 0.0,
        "error_rate": round(
            (web_kpis["error_rate"] * web_kpis["total_events"] + product_kpis["error_rate"] * product_kpis["total_events"])
            / (web_kpis["total_events"] + product_kpis["total_events"]),
            4,
        ) if (web_kpis["total_events"] + product_kpis["total_events"]) > 0 else 0.0,
    }

    print(f"\n{'='*50}")
    print("   KPIs CONSOLIDADOS (Web + Producto)")
    print(f"{'='*50}")
    print(json.dumps(consolidated_kpis, indent=2))

    # ── Carga a SQLite ──
    load_kpis(consolidated_kpis)

    # ── Exportación (opcional — el flujo continúa aunque falle) ──
    all_events = web_result["sample"] + product_result["sample"]
    export_state = None
    try:
        export_eval_snapshot(consolidated_kpis, all_events, fail_intentionally=fail_export)
    except Exception as exc:
        print(f"[pipeline] Exportación opcional falló (ignorando): {exc}")

    # ── Registrar metadatos de la ejecución ──
    metadata = {
        "source_paths": {
            "web": web_result["source_path"],
            "product": product_result["source_path"],
        },
        "raw_counts": {
            "web": web_result["raw_count"],
            "product": product_result["raw_count"],
        },
        "clean_counts": {
            "web": web_result["clean_count"],
            "product": product_result["clean_count"],
        },
        "kpis": consolidated_kpis,
        "fail_export_requested": fail_export,
        "status": "completed",
    }
    _record_pipeline_run(metadata)

    print("\n" + "=" * 50)
    print("   Pipeline completado exitosamente")
    print("=" * 50)
    return metadata


# ──────────────────────────────────────────────
# EJECUCIÓN DIRECTA
# ──────────────────────────────────────────────

if __name__ == "__main__":
    business_performance_pipeline()

"""
test_pipeline.py — Tests unitarios y de integración para el pipeline ETL.

Cubre:
  - Tasks individuales (extract, transform, transform_product, kpis, load)
  - Subflows (web_telemetry_subflow, product_telemetry_subflow)
  - Flow principal (business_performance_pipeline)
  - Consolidación de KPIs
  - Resiliencia (fail_export, corruptos, duplicados)
  - Salida esperada en SQLite y JSON
"""

from __future__ import annotations

import json
import os
import sqlite3
import tempfile
from pathlib import Path
from unittest.mock import patch

import pytest

# ─── Asegurar que el proyecto está en el path ───
_HERE = Path(__file__).resolve().parent
_PROJECT_ROOT = _HERE.parent
import sys

sys.path.insert(0, str(_PROJECT_ROOT))

from data.pipelines.pipeline import (
    DATA_RAW,
    DATA_REPORTING,
    DB_PATH,
    KPI_EXPORT,
    RUNS_JSONL,
    _generate_sample_telemetry,
    _generate_sample_product_events,
    _record_pipeline_run,
    business_performance_pipeline,
    calculate_kpis,
    export_eval_snapshot,
    extract_events,
    load_kpis,
    product_telemetry_subflow,
    transform_events,
    transform_product_events,
    web_telemetry_subflow,
)

# ============================================================
#  FIXTURES
# ============================================================


@pytest.fixture(autouse=True)
def _clean_outputs():
    """Limpia artefactos de ejecuciones anteriores antes de cada test."""
    for p in [DB_PATH, KPI_EXPORT, RUNS_JSONL]:
        if p.exists():
            p.unlink()
    # Limpiar snapshots previos
    for snap in (_PROJECT_ROOT / "data" / "processed").glob("snapshot_*.json"):
        snap.unlink()
    yield


@pytest.fixture
def sample_telemetry() -> list[dict]:
    """20 eventos de telemetría web para tests rápidos."""
    return _generate_sample_telemetry(20)


@pytest.fixture
def sample_product() -> list[dict]:
    """15 eventos de producto con estructura anidada."""
    return _generate_sample_product_events(15)


@pytest.fixture
def temp_db():
    """Base SQLite temporal para tests de load."""
    with tempfile.NamedTemporaryFile(suffix=".sqlite", delete=False) as f:
        db_path = f.name
    yield Path(db_path)
    if Path(db_path).exists():
        Path(db_path).unlink()


# ============================================================
#  1. EXTRACT
# ============================================================


class TestExtract:
    def test_extract_from_existing_file(self, tmp_path):
        """Extrae correctamente desde un JSONL existente."""
        jsonl = tmp_path / "test.jsonl"
        events = [{"event_id": "a", "user_id": "u1"}, {"event_id": "b", "user_id": "u2"}]
        with open(jsonl, "w") as f:
            for ev in events:
                f.write(json.dumps(ev) + "\n")

        result = extract_events(str(jsonl))
        assert len(result) == 2
        assert result[0]["event_id"] == "a"

    def test_extract_skips_corrupt_lines(self, tmp_path):
        """Líneas con JSON inválido se ignoran."""
        jsonl = tmp_path / "bad.jsonl"
        with open(jsonl, "w") as f:
            f.write('{"event_id": "ok"}\n')
            f.write("no-json\n")
            f.write('{"event_id": "ok2"}\n')

        result = extract_events(str(jsonl))
        assert len(result) == 2

    def test_extract_generates_sample_when_missing(self, tmp_path):
        """Si el archivo no existe, genera datos de muestra."""
        missing = tmp_path / "nonexistent.jsonl"
        result = extract_events(str(missing))
        assert len(result) > 0
        assert result[0]["event_id"].startswith("evt-")

    def test_extract_generates_product_sample_when_missing(self, tmp_path):
        """Si el archivo tiene 'product' en el nombre, genera muestra de producto."""
        missing = tmp_path / "product_data.jsonl"
        result = extract_events(str(missing))
        assert len(result) > 0
        # La muestra de producto usa estructura anidada: "id" no "event_id"
        assert result[0]["id"].startswith("pevt-")


# ============================================================
#  2. TRANSFORM (telemetría web)
# ============================================================


class TestTransform:
    def test_removes_corrupt(self, sample_telemetry):
        """Registros con _corrupt=True se eliminan."""
        clean = transform_events(sample_telemetry)
        # Los IDs corruptos son evt-0003 y evt-0017 (índices 3 y 17)
        ids = {e["event_id"] for e in clean}
        assert "evt-0003" not in ids
        assert "evt-0017" not in ids

    def test_removes_duplicates(self):
        """IDs duplicados se filtran."""
        raw = [
            {"event_id": "a", "user_id": "u1", "revenue": 10},
            {"event_id": "a", "user_id": "u1", "revenue": 20},  # duplicado
            {"event_id": "b", "user_id": "u2", "revenue": 30},
        ]
        clean = transform_events(raw)
        assert len(clean) == 2

    def test_normalizes_revenue(self):
        """Revenue se convierte a float."""
        raw = [{"event_id": "a", "user_id": "u1", "revenue": "42.5"}]
        clean = transform_events(raw)
        assert isinstance(clean[0]["revenue"], float)
        assert clean[0]["revenue"] == 42.5

    def test_empty_input(self):
        """Lista vacía retorna lista vacía."""
        assert transform_events([]) == []


# ============================================================
#  2b. TRANSFORM PRODUCTO (aplanamiento)
# ============================================================


class TestTransformProduct:
    def test_removes_corrupt(self, sample_product):
        """Registros corruptos de producto se eliminan."""
        clean = transform_product_events(sample_product)
        ids = {e["event_id"] for e in clean}
        assert "pevt-0007" not in ids
        assert "pevt-0022" not in ids

    def test_aplana_estructura_anidada(self, sample_product):
        """La estructura anidada se aplana a event_id/user_id/timestamp/event_type/revenue."""
        clean = transform_product_events(sample_product)
        assert len(clean) > 0
        evt = clean[0]
        # Debe tener los campos normalizados
        assert "event_id" in evt
        assert "user_id" in evt
        assert "timestamp" in evt
        assert "event_type" in evt
        assert "revenue" in evt
        # No debe tener los campos originales anidados
        assert "event" not in evt
        assert "rev" not in evt

    def test_action_se_mapa_a_event_type(self):
        """La acción 'purchase' se mapea a event_type='purchase'."""
        raw = [
            {
                "id": "pevt-test",
                "user": "user_1",
                "event": {"ts": "2025-01-01T00:00:00", "action": "purchase", "qty": 1, "price": 100},
                "rev": 100.0,
            }
        ]
        clean = transform_product_events(raw)
        assert clean[0]["event_type"] == "purchase"

    def test_revenue_desde_rev(self):
        """El revenue se extrae del campo 'rev'."""
        raw = [
            {
                "id": "pevt-test",
                "user": "user_1",
                "event": {"ts": "2025-01-01T00:00:00", "action": "view", "qty": 0, "price": 50},
                "rev": 999.99,
            }
        ]
        clean = transform_product_events(raw)
        assert clean[0]["revenue"] == 999.99


# ============================================================
#  3. CALCULATE KPIs
# ============================================================


class TestCalculateKPIs:
    def test_calculates_basic_metrics(self):
        """Métricas básicas se calculan correctamente."""
        events = [
            {"user_id": "u1", "event_type": "purchase", "revenue": 100},
            {"user_id": "u2", "event_type": "page_view", "revenue": 0},
            {"user_id": "u1", "event_type": "error", "revenue": 0},
            {"user_id": "u3", "event_type": "purchase", "revenue": 50},
        ]
        kpis = calculate_kpis(events)
        assert kpis["total_events"] == 4
        assert kpis["unique_users"] == 3
        assert kpis["purchase_events"] == 2
        assert kpis["total_revenue"] == 150.0

    def test_conversion_rate(self):
        """Tasa de conversión = purchase_events / total_events."""
        events = [
            {"user_id": "u1", "event_type": "purchase", "revenue": 10},
            {"user_id": "u1", "event_type": "purchase", "revenue": 20},
            {"user_id": "u2", "event_type": "page_view", "revenue": 0},
            {"user_id": "u2", "event_type": "page_view", "revenue": 0},
        ]
        kpis = calculate_kpis(events)
        assert kpis["conversion_rate"] == 0.5  # 2/4

    def test_zero_events(self):
        """Con 0 eventos, todas las métricas son 0."""
        kpis = calculate_kpis([])
        assert kpis["total_events"] == 0
        assert kpis["conversion_rate"] == 0.0
        assert kpis["error_rate"] == 0.0

    def test_supports_product_action(self):
        """Soporta event_type='purchase' o action='purchase' (productos normalizados)."""
        events = [
            {"user_id": "u1", "event_type": "purchase", "revenue": 100},
            {"user_id": "u2", "action": "purchase", "revenue": 50},
        ]
        kpis = calculate_kpis(events)
        assert kpis["purchase_events"] == 2
        assert kpis["total_revenue"] == 150.0


# ============================================================
#  4. LOAD (SQLite)
# ============================================================


class TestLoad:
    def test_load_creates_table_and_inserts(self, tmp_path):
        """load_kpis crea la tabla e inserta datos."""
        test_db = tmp_path / "test.sqlite"
        with patch("data.pipelines.pipeline.DB_PATH", test_db):
            kpis = {
                "timestamp": "2025-01-01T00:00:00",
                "total_events": 10,
                "unique_users": 5,
                "purchase_events": 3,
                "total_revenue": 500.0,
                "conversion_rate": 0.3,
                "error_rate": 0.1,
            }
            load_kpis(kpis)

            conn = sqlite3.connect(str(test_db))
            row = conn.execute("SELECT * FROM kpi_window WHERE id = 1").fetchone()
            conn.close()

            assert row is not None
            assert row[2] == 10  # total_events

    def test_load_upsert_idempotente(self, tmp_path):
        """Ejecutar load dos veces con distintos datos hace UPSERT (no duplica)."""
        test_db = tmp_path / "test.sqlite"
        with patch("data.pipelines.pipeline.DB_PATH", test_db):
            kpis_a = {
                "timestamp": "2025-01-01T00:00:00",
                "total_events": 10,
                "unique_users": 5,
                "purchase_events": 3,
                "total_revenue": 500.0,
                "conversion_rate": 0.3,
                "error_rate": 0.1,
            }
            kpis_b = {
                "timestamp": "2025-01-02T00:00:00",
                "total_events": 20,
                "unique_users": 8,
                "purchase_events": 5,
                "total_revenue": 800.0,
                "conversion_rate": 0.25,
                "error_rate": 0.05,
            }
            load_kpis(kpis_a)
            load_kpis(kpis_b)

            conn = sqlite3.connect(str(test_db))
            rows = conn.execute("SELECT * FROM kpi_window").fetchall()
            conn.close()

            assert len(rows) == 1  # solo 1 fila (UPSERT)
            # ids: id=0, timestamp=1, total_events=2, unique_users=3, purchase_events=4, ...
            assert rows[0][2] == 20  # total_events del último


# ============================================================
#  5. EXPORT
# ============================================================


class TestExport:
    def test_export_creates_files(self, tmp_path):
        """export_eval_snapshot crea snapshot y latest_kpis.json."""
        kpis = {"total_events": 5, "total_revenue": 100.0}
        with (
            patch("data.pipelines.pipeline.DATA_PROCESSED", tmp_path),
            patch("data.pipelines.pipeline.KPI_EXPORT", tmp_path / "latest_kpis.json"),
        ):
            result = export_eval_snapshot(kpis, sample_events=[])
            assert result["kpis"]["total_events"] == 5
            assert (tmp_path / "latest_kpis.json").exists()

    def test_export_fail_intentionally(self):
        """Con fail_intentionally=True, lanza RuntimeError."""
        with pytest.raises(RuntimeError, match="Fallo controlado"):
            export_eval_snapshot({}, fail_intentionally=True)


# ============================================================
#  6. SUBFLOWS
# ============================================================


class TestSubflows:
    def test_web_telemetry_subflow(self, tmp_path):
        """Subflow web procesa telemetry_events.jsonl y retorna KPIs."""
        jsonl = tmp_path / "telemetry_events.jsonl"
        with open(jsonl, "w") as f:
            f.write(json.dumps({"event_id": "e1", "user_id": "u1", "event_type": "page_view", "revenue": 0}) + "\n")
            f.write(json.dumps({"event_id": "e2", "user_id": "u2", "event_type": "purchase", "revenue": 50}) + "\n")

        result = web_telemetry_subflow(str(jsonl))
        assert result["source"] == "web_telemetry"
        assert result["clean_count"] == 2
        assert result["kpis"]["total_events"] == 2
        assert result["kpis"]["total_revenue"] >= 50.0

    def test_product_telemetry_subflow(self, tmp_path):
        """Subflow producto procesa product_events.jsonl (estructura anidada) y retorna KPIs."""
        jsonl = tmp_path / "product_events.jsonl"
        # Estructura anidada
        with open(jsonl, "w") as f:
            evt = {
                "id": "pevt-test1",
                "user": "u1",
                "event": {"ts": "2025-01-01T00:00:00", "product_id": "p1", "action": "purchase", "qty": 2, "price": 100},
                "rev": 200.0,
            }
            f.write(json.dumps(evt) + "\n")
            evt2 = {
                "id": "pevt-test2",
                "user": "u2",
                "event": {"ts": "2025-01-01T01:00:00", "product_id": "p2", "action": "view", "qty": 0, "price": 50},
                "rev": 0.0,
            }
            f.write(json.dumps(evt2) + "\n")

        result = product_telemetry_subflow(str(jsonl))
        assert result["source"] == "product_telemetry"
        assert result["clean_count"] == 2
        assert result["kpis"]["total_events"] == 2
        # transform_product_events aplana rev → revenue
        assert result["kpis"]["total_revenue"] == 200.0

    def test_subflow_handles_corrupt_gracefully(self, tmp_path):
        """Subflows no fallan ante datos corruptos."""
        jsonl = tmp_path / "test.jsonl"
        with open(jsonl, "w") as f:
            f.write(json.dumps({"event_id": "e1", "user_id": "u1", "event_type": "page_view", "revenue": 0, "_corrupt": True}) + "\n")
            f.write(json.dumps({"event_id": "e2", "user_id": "u2", "event_type": "purchase", "revenue": 30}) + "\n")

        result = web_telemetry_subflow(str(jsonl))
        assert result["clean_count"] == 1  # Solo el no corrupto


# ============================================================
#  7. FLUJO PRINCIPAL
# ============================================================


class TestMainFlow:
    def test_pipeline_completo(self, tmp_path):
        """El flow principal ejecuta ambos subflows y consolida KPIs."""
        # Crear fuente web
        web_jsonl = tmp_path / "telemetry_events.jsonl"
        with open(web_jsonl, "w") as f:
            for i in range(3):
                f.write(json.dumps({"event_id": f"w{i}", "user_id": f"u{i}", "event_type": "purchase" if i == 0 else "page_view", "revenue": 100.0 if i == 0 else 0}) + "\n")

        # Crear fuente producto (estructura anidada)
        prod_jsonl = tmp_path / "product_events.jsonl"
        with open(prod_jsonl, "w") as f:
            for i in range(2):
                evt = {
                    "id": f"p{i}",
                    "user": f"u{i}",
                    "event": {"ts": "2025-01-01T00:00:00", "product_id": f"prod{i}", "action": "purchase" if i == 0 else "view", "qty": 1, "price": 200},
                    "rev": 200.0 if i == 0 else 0.0,
                }
                f.write(json.dumps(evt) + "\n")

        with (
            patch("data.pipelines.pipeline.DATA_REPORTING", tmp_path),
            patch("data.pipelines.pipeline.DB_PATH", tmp_path / "business_pis.sqlite"),
            patch("data.pipelines.pipeline.KPI_EXPORT", tmp_path / "latest_kpis.json"),
            patch("data.pipelines.pipeline.RUNS_JSONL", tmp_path / "pipeline_runs.jsonl"),
            patch("data.pipelines.pipeline.DATA_PROCESSED", tmp_path / "processed"),
        ):
            result = business_performance_pipeline(web_source=str(web_jsonl), product_source=str(prod_jsonl))

        assert result["status"] == "completed"
        assert result["kpis"]["total_events"] == 5  # 3 web + 2 producto
        # 1 web purchase (100) + 1 product purchase (200) = 300
        assert result["kpis"]["total_revenue"] == 300.0
        assert result["kpis"]["purchase_events"] == 2  # 1 web + 1 producto
        assert isinstance(result["clean_counts"]["web"], int)
        assert isinstance(result["clean_counts"]["product"], int)

    def test_pipeline_resiliente_con_fail_export(self, tmp_path):
        """Con fail_export=True, el pipeline completa sin error."""
        jsonl = tmp_path / "telemetry_events.jsonl"
        with open(jsonl, "w") as f:
            f.write(json.dumps({"event_id": "e1", "user_id": "u1", "event_type": "page_view", "revenue": 0}) + "\n")

        prod_jsonl = tmp_path / "product_events.jsonl"
        with open(prod_jsonl, "w") as f:
            f.write(json.dumps({"id": "p1", "user": "u1", "event": {"ts": "2025-01-01T00:00:00", "product_id": "p1", "action": "view", "qty": 0, "price": 10}, "rev": 0}) + "\n")

        with (
            patch("data.pipelines.pipeline.DATA_REPORTING", tmp_path),
            patch("data.pipelines.pipeline.DB_PATH", tmp_path / "business_pis.sqlite"),
            patch("data.pipelines.pipeline.KPI_EXPORT", tmp_path / "latest_kpis.json"),
            patch("data.pipelines.pipeline.RUNS_JSONL", tmp_path / "pipeline_runs.jsonl"),
            patch("data.pipelines.pipeline.DATA_PROCESSED", tmp_path / "processed"),
        ):
            result = business_performance_pipeline(
                web_source=str(jsonl), product_source=str(prod_jsonl), fail_export=True
            )

        assert result["status"] == "completed"
        assert result["fail_export_requested"] is True


# ============================================================
#  8. SALIDA (SQLite + JSON + JSONL)
# ============================================================


class TestOutput:
    def test_sqlite_contiene_kpis_consolidados(self, tmp_path):
        """Después de ejecutar, SQLite tiene los KPIs consolidados."""
        jsonl = tmp_path / "telemetry_events.jsonl"
        with open(jsonl, "w") as f:
            f.write(json.dumps({"event_id": "e1", "user_id": "u1", "event_type": "purchase", "revenue": 100}) + "\n")

        prod_jsonl = tmp_path / "product_events.jsonl"
        with open(prod_jsonl, "w") as f:
            f.write(json.dumps({"id": "p1", "user": "u1", "event": {"ts": "2025-01-01T00:00:00", "product_id": "p1", "action": "purchase", "qty": 1, "price": 200}, "rev": 200}) + "\n")

        test_db = tmp_path / "business_pis.sqlite"
        test_kpi = tmp_path / "latest_kpis.json"
        test_runs = tmp_path / "pipeline_runs.jsonl"

        with (
            patch("data.pipelines.pipeline.DB_PATH", test_db),
            patch("data.pipelines.pipeline.KPI_EXPORT", test_kpi),
            patch("data.pipelines.pipeline.RUNS_JSONL", test_runs),
            patch("data.pipelines.pipeline.DATA_PROCESSED", tmp_path / "processed"),
        ):
            business_performance_pipeline(web_source=str(jsonl), product_source=str(prod_jsonl))

        # Verificar SQLite
        conn = sqlite3.connect(str(test_db))
        row = conn.execute("SELECT total_events, total_revenue, purchase_events FROM kpi_window WHERE id = 1").fetchone()
        conn.close()
        assert row is not None
        assert row[0] == 2  # total_events: 1 web + 1 producto
        assert row[1] == 300.0  # revenue: 100 + 200

    def test_latest_kpis_json_exportado(self, tmp_path):
        """latest_kpis.json se crea con los KPIs consolidados."""
        jsonl = tmp_path / "telemetry_events.jsonl"
        with open(jsonl, "w") as f:
            f.write(json.dumps({"event_id": "e1", "user_id": "u1", "event_type": "page_view", "revenue": 0}) + "\n")

        prod_jsonl = tmp_path / "product_events.jsonl"
        with open(prod_jsonl, "w") as f:
            f.write(json.dumps({"id": "p1", "user": "u1", "event": {"ts": "2025-01-01T00:00:00", "product_id": "p1", "action": "view", "qty": 0, "price": 0}, "rev": 0}) + "\n")

        test_kpi = tmp_path / "latest_kpis.json"
        with (
            patch("data.pipelines.pipeline.DB_PATH", tmp_path / "business_pis.sqlite"),
            patch("data.pipelines.pipeline.KPI_EXPORT", test_kpi),
            patch("data.pipelines.pipeline.RUNS_JSONL", tmp_path / "pipeline_runs.jsonl"),
            patch("data.pipelines.pipeline.DATA_PROCESSED", tmp_path / "processed"),
        ):
            business_performance_pipeline(web_source=str(jsonl), product_source=str(prod_jsonl))

        assert test_kpi.exists()
        with open(test_kpi) as f:
            data = json.load(f)
        assert "total_events" in data
        assert "total_revenue" in data

    def test_pipeline_runs_jsonl_registra_ejecucion(self, tmp_path):
        """pipeline_runs.jsonl registra cada ejecución."""
        jsonl = tmp_path / "telemetry_events.jsonl"
        with open(jsonl, "w") as f:
            f.write(json.dumps({"event_id": "e1", "user_id": "u1", "event_type": "page_view", "revenue": 0}) + "\n")

        prod_jsonl = tmp_path / "product_events.jsonl"
        with open(prod_jsonl, "w") as f:
            f.write(json.dumps({"id": "p1", "user": "u1", "event": {"ts": "2025-01-01T00:00:00", "product_id": "p1", "action": "view", "qty": 0, "price": 0}, "rev": 0}) + "\n")

        test_runs = tmp_path / "pipeline_runs.jsonl"
        with (
            patch("data.pipelines.pipeline.DB_PATH", tmp_path / "business_pis.sqlite"),
            patch("data.pipelines.pipeline.KPI_EXPORT", tmp_path / "latest_kpis.json"),
            patch("data.pipelines.pipeline.RUNS_JSONL", test_runs),
            patch("data.pipelines.pipeline.DATA_PROCESSED", tmp_path / "processed"),
        ):
            business_performance_pipeline(web_source=str(jsonl), product_source=str(prod_jsonl))

        assert test_runs.exists()
        lines = test_runs.read_text().strip().split("\n")
        assert len(lines) >= 1
        record = json.loads(lines[0])
        assert record["status"] == "completed"
        assert "run_id" in record


# ============================================================
#  9. UTILIDADES
# ============================================================


class TestUtils:
    def test_generate_sample_telemetry(self):
        """Genera eventos con semilla fija (reproducible)."""
        events = _generate_sample_telemetry(10)
        assert len(events) == 10
        # Reproducibilidad: misma semilla = mismos resultados
        events_2 = _generate_sample_telemetry(10)
        assert events == events_2

    def test_generate_sample_product_events(self):
        """Genera eventos de producto con estructura anidada reproducible."""
        events = _generate_sample_product_events(10)
        assert len(events) == 10
        assert "id" in events[0]
        assert "event" in events[0]  # estructura anidada
        assert "rev" in events[0]
        # Reproducibilidad
        events_2 = _generate_sample_product_events(10)
        assert events == events_2

    def test_record_pipeline_run(self, tmp_path):
        """_record_pipeline_run escribe en JSONL."""
        runs_file = tmp_path / "runs.jsonl"
        with patch("data.pipelines.pipeline.RUNS_JSONL", runs_file):
            _record_pipeline_run({"status": "test", "kpis": {"total": 5}})
            assert runs_file.exists()
            record = json.loads(runs_file.read_text().strip())
            assert record["status"] == "test"


# ============================================================
#  10. EJECUCIÓN DEL TEST RUNNER
# ============================================================

if __name__ == "__main__":
    pytest.main(["-v", "--tb=short", __file__])

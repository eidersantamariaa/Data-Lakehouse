"""
timetraveling.py
────────────────
Módulo de Time Travel para Apache Iceberg usando PyIceberg.
Se integra con ui.py vía FastAPI.

Uso directo:
    from timetraveling import TimeTraveler
    tt = TimeTraveler(catalog)
    tt.read_at_snapshot("db.tabla", snapshot_id=123456)
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any
import math

log = logging.getLogger("timetraveling")


# ─────────────────────────────────────────────────────────────────────────────
# Helpers internos
# ─────────────────────────────────────────────────────────────────────────────

def _format_value(v: Any) -> Any:
    """Convierte tipos complejos de PyArrow/Python a tipos JSON-serializables."""
    if v is None:
        return None
    if isinstance(v, float) and (math.isnan(v) or math.isinf(v)):
        return None
    if isinstance(v, (str, int, float, bool)):
        return v
    if isinstance(v, datetime):
        return v.isoformat()
    return str(v)


def _arrow_to_rows(arrow_table) -> list[dict]:
    """Convierte una PyArrow Table a lista de dicts JSON-serializables."""
    rows = []
    for row in arrow_table.to_pylist():
        rows.append({k: _format_value(v) for k, v in row.items()})
    return rows


def _snap_summary(snap) -> dict:
    """Extrae los campos relevantes de un snapshot Iceberg."""
    sm = snap.summary or {}
    ts = datetime.fromtimestamp(snap.timestamp_ms / 1000, tz=timezone.utc)
    return {
        "id":        snap.snapshot_id,
        "parent_id": snap.parent_snapshot_id,
        "ts_ms":     snap.timestamp_ms,
        "ts":        ts.strftime("%Y-%m-%d %H:%M:%S UTC"),
        "op":        sm.get("operation", "—"),
        "add":       sm.get("added-data-files", "0"),
        "rem":       sm.get("deleted-data-files", "0"),
        "rec":       sm.get("total-records", "—"),
        "size":      sm.get("total-files-size", "—"),
    }


# ─────────────────────────────────────────────────────────────────────────────
# Clase principal
# ─────────────────────────────────────────────────────────────────────────────

class TimeTraveler:
    """
    Operaciones de Time Travel sobre tablas Apache Iceberg.

    Parámetros
    ----------
    catalog : instancia de PyIceberg Catalog ya conectada.
    """

    def __init__(self, catalog):
        self.catalog = catalog

    # ── Utilidad ─────────────────────────────────────────────────────────────

    def _load(self, table: str):
        """Carga una tabla Iceberg a partir de 'namespace.tabla'."""
        parts = table.split(".", 1)
        if len(parts) != 2:
            raise ValueError(f"Formato invalido: usa 'namespace.tabla', recibido '{table}'")
        return self.catalog.load_table(tuple(parts))

    # ── 1. Historial ─────────────────────────────────────────────────────────

    def get_history(self, table: str) -> list[dict]:
        """
        Devuelve la lista completa de snapshots, del mas reciente al mas antiguo.
        Campos: id, parent_id, ts_ms, ts, op, add, rem, rec, size, current
        """
        t = self._load(table)
        current_id = t.current_snapshot().snapshot_id if t.current_snapshot() else None
        snaps = []
        for s in reversed(list(t.metadata.snapshots)):
            d = _snap_summary(s)
            d["current"] = (s.snapshot_id == current_id)
            snaps.append(d)
        log.info("[history] %s -> %d snapshots", table, len(snaps))
        return snaps

    # ── 2. Leer en snapshot concreto ─────────────────────────────────────────

    def read_at_snapshot(self, table: str, snapshot_id: int, limit: int = 50) -> dict:
        """
        Lee los datos de la tabla tal como estaban en el snapshot indicado.

        Devuelve: rows (lista de dicts), columns (lista de nombres), snapshot_id
        """
        t = self._load(table)
        ids = [s.snapshot_id for s in t.metadata.snapshots]
        if snapshot_id not in ids:
            available = ", ".join(str(id) for id in ids[-5:]) if ids else "ninguno"
            raise ValueError(
                f"Snapshot {snapshot_id} no existe en la tabla '{table}'. "
                f"Snapshots disponibles (últimos 5): {available}"
            )

        arrow = t.scan(snapshot_id=snapshot_id, limit=limit).to_arrow()
        rows = _arrow_to_rows(arrow)
        log.info("[read_at_snapshot] %s @ %d -> %d filas", table, snapshot_id, len(rows))
        return {
            "snapshot_id": snapshot_id,
            "columns":     arrow.column_names,
            "rows":        rows,
            "total":       len(rows),
        }

    # ── 3. Leer en timestamp ──────────────────────────────────────────────────

    def read_at_timestamp(self, table: str, timestamp_ms: int, limit: int = 50) -> dict:
        """
        Lee los datos tal como estaban en el momento indicado (ms Unix).

        Devuelve: rows, columns, resolved_snapshot_id, timestamp_ms
        """
        t = self._load(table)

        # Snapshot mas reciente <= timestamp
        target = None
        for s in sorted(t.metadata.snapshots, key=lambda x: x.timestamp_ms):
            if s.timestamp_ms <= timestamp_ms:
                target = s

        if target is None:
            ts_str = datetime.fromtimestamp(timestamp_ms / 1000).isoformat()
            raise ValueError(
                f"No existe ningun snapshot anterior a {ts_str} en '{table}'"
            )

        arrow = t.scan(snapshot_id=target.snapshot_id, limit=limit).to_arrow()
        rows = _arrow_to_rows(arrow)
        log.info("[read_at_timestamp] %s -> snapshot %d, %d filas", table, target.snapshot_id, len(rows))
        return {
            "timestamp_ms":         timestamp_ms,
            "resolved_snapshot_id": target.snapshot_id,
            "resolved_ts":          datetime.fromtimestamp(target.timestamp_ms / 1000).isoformat(),
            "columns":              arrow.column_names,
            "rows":                 rows,
            "total":                len(rows),
        }

    # ── 4. Rollback ───────────────────────────────────────────────────────────

    def rollback_to_snapshot(self, table: str, snapshot_id: int) -> dict:
        """
        Hace rollback al snapshot indicado.
        ATENCION: cambia el snapshot actual de la tabla.

        Devuelve: previous_snapshot_id, new_snapshot_id
        """
        t = self._load(table)
        previous_id = t.current_snapshot().snapshot_id if t.current_snapshot() else None

        ids = [s.snapshot_id for s in t.metadata.snapshots]
        if snapshot_id not in ids:
            available = ", ".join(str(id) for id in ids[-5:]) if ids else "ninguno"
            raise ValueError(
                f"Snapshot {snapshot_id} no encontrado en '{table}'. "
                f"Snapshots disponibles (últimos 5): {available}"
            )

        with t.manage_snapshots() as ms:
            ms.rollback_to_snapshot(snapshot_id)

        log.info("[rollback] %s: %s -> %d", table, previous_id, snapshot_id)
        return {
            "table":                table,
            "previous_snapshot_id": previous_id,
            "new_snapshot_id":      snapshot_id,
            "status":               "ok",
        }

    def rollback_to_timestamp(self, table: str, timestamp_ms: int) -> dict:
        """
        Hace rollback al snapshot mas reciente anterior al timestamp dado.

        Devuelve: previous_snapshot_id, new_snapshot_id, resolved_ts
        """
        t = self._load(table)
        previous_id = t.current_snapshot().snapshot_id if t.current_snapshot() else None

        target = None
        for s in sorted(t.metadata.snapshots, key=lambda x: x.timestamp_ms):
            if s.timestamp_ms <= timestamp_ms:
                target = s

        if target is None:
            raise ValueError(f"No hay snapshots anteriores al timestamp {timestamp_ms} en '{table}'")

        with t.manage_snapshots() as ms:
            ms.rollback_to_snapshot(target.snapshot_id)

        resolved_ts = datetime.fromtimestamp(target.timestamp_ms / 1000).isoformat()
        log.info("[rollback_ts] %s: %s -> %d (%s)", table, previous_id, target.snapshot_id, resolved_ts)
        return {
            "table":                table,
            "previous_snapshot_id": previous_id,
            "new_snapshot_id":      target.snapshot_id,
            "resolved_ts":          resolved_ts,
            "status":               "ok",
        }

    # ── 5. Lectura incremental ────────────────────────────────────────────────

    def get_incremental_changes(
        self,
        table: str,
        start_snapshot_id: int,
        end_snapshot_id: int,
        limit: int = 200,
    ) -> dict:
        """
        Devuelve los registros anadidos y eliminados entre dos snapshots.
        Lee ambos snapshots y devuelve:
        - Registros agregados (que existen en final pero no en inicial)
        - Registros eliminados (que existen en inicial pero no en final)

        Devuelve: rows (con __change_type), columns, added_records, removed_records, truncated
        """
        t = self._load(table)
        ids = [s.snapshot_id for s in t.metadata.snapshots]
        
        # Validate both snapshots exist
        for snap_id in [start_snapshot_id, end_snapshot_id]:
            if snap_id not in ids:
                available = ", ".join(str(id) for id in ids[-5:]) if ids else "ninguno"
                raise ValueError(
                    f"Snapshot {snap_id} no encontrado en '{table}'. "
                    f"Snapshots disponibles (últimos 5): {available}"
                )

        # Read both snapshots to compare
        start_arrow = t.scan(snapshot_id=start_snapshot_id).to_arrow()
        end_arrow = t.scan(snapshot_id=end_snapshot_id).to_arrow()
        
        start_rows = start_arrow.to_pylist()
        end_rows = end_arrow.to_pylist()
        
        # Convert to sets for comparison (using JSON string representation)
        import json
        start_set = {json.dumps(row, sort_keys=True, default=str) for row in start_rows}
        end_set = {json.dumps(row, sort_keys=True, default=str) for row in end_rows}
        
        # Find added rows (in end but not in start)
        added_rows_json = end_set - start_set
        added_rows = [json.loads(row_json) for row_json in sorted(added_rows_json)]
        
        # Find removed rows (in start but not in end)
        removed_rows_json = start_set - end_set
        removed_rows = [json.loads(row_json) for row_json in sorted(removed_rows_json)]
        
        # Combine and tag with change type
        all_changes = []
        for row in added_rows:
            formatted = {k: _format_value(v) for k, v in row.items()}
            formatted["__change_type"] = "added"
            all_changes.append(formatted)
        
        for row in removed_rows:
            formatted = {k: _format_value(v) for k, v in row.items()}
            formatted["__change_type"] = "removed"
            all_changes.append(formatted)
        
        # Apply limit and track if truncated
        truncated = len(all_changes) > limit
        if truncated:
            all_changes = all_changes[:limit]
        
        log.info("[incremental] %s: %d -> %d, +%d agregados -%d eliminados",
                 table, start_snapshot_id, end_snapshot_id, len(added_rows), len(removed_rows))
        return {
            "from_snapshot_id": start_snapshot_id,
            "to_snapshot_id":   end_snapshot_id,
            "columns":          end_arrow.column_names,
            "rows":             all_changes,
            "added_records":    len(added_rows),
            "removed_records":  len(removed_rows),
            "truncated":        truncated,
        }

    # ── 6. Expirar snapshots ──────────────────────────────────────────────────

    def expire_snapshots(self, table: str, older_than_ms: int, retain_last: int = 1) -> dict:
        """
        Elimina snapshots anteriores a older_than_ms, conservando siempre retain_last.
        ATENCION: los snapshots eliminados no pueden usarse para time travel.

        Devuelve: deleted_snapshots, remaining_snapshots
        """
        t = self._load(table)
        before = len(list(t.metadata.snapshots))

        t.expire_snapshots() \
         .expire_older_than(older_than_ms) \
         .retain_last(retain_last) \
         .commit()

        t = self._load(table)  # recargar para contar
        after = len(list(t.metadata.snapshots))
        deleted = before - after
        older_than_str = datetime.fromtimestamp(older_than_ms / 1000).isoformat()

        log.info("[expire] %s: %d eliminados, %d restantes", table, deleted, after)
        return {
            "table":               table,
            "older_than":          older_than_str,
            "retain_last":         retain_last,
            "deleted_snapshots":   deleted,
            "remaining_snapshots": after,
            "status":              "ok",
        }

    # ── 7. Archivos huerfanos ─────────────────────────────────────────────────

    def remove_orphan_files(self, table: str) -> dict:
        """
        Elimina archivos no referenciados por ningun snapshot.
        Suelen quedar tras operaciones fallidas o commits parciales.

        Devuelve: deleted_files (lista de paths), deleted_count
        """
        t = self._load(table)
        try:
            result = t.remove_dangling_deletes()
            deleted = list(result) if result else []
        except AttributeError:
            deleted = []
            log.warning("[orphans] remove_dangling_deletes no disponible en esta version de PyIceberg")

        log.info("[orphans] %s: %d archivos eliminados", table, len(deleted))
        return {
            "table":         table,
            "deleted_files": deleted,
            "deleted_count": len(deleted),
            "status":        "ok",
        }

    # ── 8. Estadisticas ───────────────────────────────────────────────────────

    def snapshot_stats(self, table: str) -> dict:
        """
        Resumen estadistico del historial de snapshots:
        total, rango temporal, operaciones, tamanyo acumulado.
        """
        t = self._load(table)
        snaps = list(t.metadata.snapshots)
        if not snaps:
            return {"table": table, "total": 0}

        ops: dict[str, int] = {}
        total_size = 0
        for s in snaps:
            sm = s.summary or {}
            op = sm.get("operation", "unknown")
            ops[op] = ops.get(op, 0) + 1
            try:
                total_size += int(sm.get("total-files-size", 0))
            except (ValueError, TypeError):
                pass

        oldest = min(snaps, key=lambda x: x.timestamp_ms)
        newest = max(snaps, key=lambda x: x.timestamp_ms)
        span_hours = (newest.timestamp_ms - oldest.timestamp_ms) / 3_600_000

        return {
            "table":           table,
            "total_snapshots": len(snaps),
            "oldest_ts":       datetime.fromtimestamp(oldest.timestamp_ms / 1000).isoformat(),
            "newest_ts":       datetime.fromtimestamp(newest.timestamp_ms / 1000).isoformat(),
            "span_hours":      round(span_hours, 2),
            "operations":      ops,
            "total_size_mb":   round(total_size / 1_048_576, 2),
        }
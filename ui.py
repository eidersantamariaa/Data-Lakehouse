from datetime import datetime, timezone

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, HTMLResponse
from pyiceberg.catalog import load_catalog
from pyiceberg.io.fsspec import FsspecFileIO
import pyarrow as pa
import boto3
from timetraveling import TimeTraveler

app = FastAPI()
catalog = None
s3_config = {}
tt: TimeTraveler | None = None

"""
pip install pyarrow pyiceberg boto3 fastapi uvicorn

endpoint=http://172.16.58.11:32688
access_key=GK5f421d5f440758f74b0e0312
secret_key=409baa63477885db12cd1db0a518748c5e83e971b5e8cf2129fe6c7498de125d
bucket=warehouse

"""

@app.post("/connect")
async def connect_catalog(req: Request):
    global catalog, s3_config, tt
    data = await req.json()
    try:
        catalog = load_catalog(
            "garage",
            **{
                "type": "rest",
                "uri": data["ep"],
                "s3.endpoint": data["ep_s3"],
                "s3.access-key-id": data["ak"],
                "s3.secret-access-key": data["sk"],
                "s3.region": "us-east-1",
                "s3.path-style-access": "true",
                "py-io-impl": "pyiceberg.io.pyarrow.PyArrowFileIO",
            }
        )
        # Guarda los datos de conexión para usarlos en preview
        s3_config = data
        namespaces = catalog.list_namespaces()
        print("✅ namespaces:", namespaces)
    except Exception as e:
        print("❌ error:", e)
        return JSONResponse({"status": "error", "msg": str(e)}, status_code=400)
    
    try:
        tt = TimeTraveler(catalog)
        print("✅ tt ok:", tt)
    except Exception as e:
        print("❌ tt error:", e)
        return JSONResponse({"status": "error", "msg": f"TimeTraveler init failed: {e}"}, status_code=400)

    return JSONResponse({"status": "ok"})

@app.get("/status")
def status():
    return {
        "catalog": catalog is not None,
        "tt": tt is not None,
        "catalog_type": str(type(catalog)) if catalog else None,
    }

@app.get("/tables")
def list_tables():
    if catalog is None:
        return []
    try:
        namespaces = catalog.list_namespaces()
        tables = []
        for ns in namespaces:
            ns_str = ns[0]
            for tbl in catalog.list_tables(namespace=ns_str):
                tables.append(f"{ns_str}.{tbl[-1]}")
        return tables
    except Exception as e:
        print("❌ error:", e)
        return []

@app.get("/schema/{table}")
def schema(table: str):
    if catalog is None:
        return {}
    ns, tbl_name = table.split(".", 1)
    t = catalog.load_table((ns, tbl_name))
    snap = t.current_snapshot()
    summary = snap.summary if snap else {}
    size_bytes = int(summary.get("total-files-size", 0))
    size_str = f"{size_bytes/1024/1024:.1f} MB" if size_bytes > 1024*1024 else f"{size_bytes/1024:.1f} KB"
    return {
        "schema": [
            {"id": f.field_id, "n": f.name, "t": str(f.field_type).lower(), "req": f.required}
            for f in t.schema().fields
        ],
        "rows": int(t.scan().to_arrow().num_rows),
        "files": int(summary.get("total-data-files", 0)),
        "size": size_str,
        "part": str(t.spec()) or "unpartitioned",
        "loc": t.location(),
    }

@app.get("/preview/{table}")
def preview(table: str):
    if catalog is None:
        return {"error": "Catalog not connected"}
    
    ns, tbl_name = table.split(".", 1)
    t = catalog.load_table((ns, tbl_name))
    snap = t.current_snapshot()
    summary = snap.summary if snap else {}

    rows = []
    try:
        # 1. Escaneamos la tabla y pedimos solo las primeras 5 filas
        # PyIceberg usa PyArrow internamente para esto.
        # .limit(5) es eficiente: no descarga toda la tabla.
        table_scan = t.scan(limit=5).to_arrow()
        
        # 2. Convertimos a lista de diccionarios
        raw_rows = table_scan.to_pylist()

        # 3. Formateamos tipos complejos (igual que tenías antes)
        for row in raw_rows:
            formatted_row = {}
            for k, v in row.items():
                # Convertimos a string lo que no sea un tipo básico
                if v is not None and not isinstance(v, (str, int, float, bool)):
                    formatted_row[k] = str(v)
                else:
                    formatted_row[k] = v
            rows.append(formatted_row)

    except Exception as e:
        print(f"❌ preview error: {e}")
        rows = []

    return {
        "schema": [
            {"id": f.field_id, "n": f.name, "t": str(f.field_type).lower(), "req": f.required}
            for f in t.schema().fields
        ],
        "rows": int(summary.get("total-records", 0)),
        "preview": rows,
        "snap": snap.snapshot_id if snap else "—",
    }

@app.get("/snapshots/{table}")
def snapshots(table: str):
    if catalog is None:
        return {}
    try:
        ns, tbl_name = table.split(".", 1)
        t = catalog.load_table((ns, tbl_name))
        # Refresh metadata to ensure we have the latest snapshots
        # (especially after expire_snapshots was called)
        t.refresh()
        current_id = t.current_snapshot().snapshot_id if t.current_snapshot() else None
        snaps = []
        for s in reversed(list(t.metadata.snapshots)):
            sm = s.summary or {}
            from datetime import datetime
            ts = datetime.fromtimestamp(s.timestamp_ms / 1000).strftime("%Y-%m-%d %H:%M")
            snaps.append({
                "id": str(s.snapshot_id),  # Convert to string to preserve precision
                "ts": ts,
                "op": sm.get("operation", "—"),
                "add": sm.get("added-data-files", "0"),
                "rem": sm.get("deleted-data-files", "0"),
                "rec": sm.get("total-records", "—"),
                "current": s.snapshot_id == current_id,
            })
        return {"snaps": snaps}
    except Exception as e:
        print(f"❌ snapshots error: {e}")
        return {"snaps": [], "error": str(e)}

def _tt():
    if tt is None:
        raise RuntimeError("Catalog no conectado")
    return tt


def _write_audit_log(namespace: str, table_name: str, accion: str, num_registros: int = 0):
    """Best-effort audit log writer using PyIceberg table append if available."""
    if catalog is None:
        return False

    try:
        audit_table = catalog.load_table((namespace, "audit_log"))
    except Exception:
        # If audit table does not exist, skip silently (best-effort behavior)
        return False

    row = {
        "tabla": table_name,
        "accion": accion,
        "timestamp": datetime.now(timezone.utc),
        "fuente": namespace,
        "num_registros": int(num_registros),
    }

    try:
        arrow_tbl = pa.Table.from_pylist([row])

        # PyIceberg versions differ in write APIs; try common entry points.
        if hasattr(audit_table, "append"):
            audit_table.append(arrow_tbl)
        elif hasattr(audit_table, "add_files"):
            # Not ideal for row writes, but keep fallback for API variance.
            return False
        else:
            return False
        return True
    except Exception as e:
        print(f"⚠ audit_log write failed: {e}")
        return False


@app.post("/table/delete/{table}")
def delete_table(table: str):
    """Elimina una tabla, registra la accion en audit_log y devuelve el comando ejecutado."""
    if catalog is None:
        return JSONResponse({"error": "Catalog not connected"}, status_code=400)

    try:
        ns, tbl_name = table.split(".", 1)
    except ValueError:
        return JSONResponse({"error": "Formato invalido, usa namespace.tabla"}, status_code=400)

    # Command string shown in server logs and returned to UI
    cmd = f"DROP TABLE players.{ns}.{tbl_name} PURGE"
    print(f"🗑 executing server command: {cmd}")

    try:
        # Try signatures compatible across pyiceberg versions
        try:
            catalog.drop_table((ns, tbl_name), purge_requested=True)
        except TypeError:
            try:
                catalog.drop_table((ns, tbl_name), purge=True)
            except TypeError:
                catalog.drop_table((ns, tbl_name))

        audit_written = _write_audit_log(
            namespace=ns,
            table_name=tbl_name,
            accion= "DROP",
            num_registros=0,
        )

        return {
            "status": "ok",
            "table": table,
            "server_command": cmd,
            "audit_logged": audit_written,
        }
    except Exception as e:
        return JSONResponse({"error": str(e), "server_command": cmd}, status_code=400)

@app.get("/tt/read-snapshot/{table}")
def tt_read_snapshot(table: str, snapshot_id: str, limit: int = 50):
    """Lee datos de la tabla en un snapshot concreto."""
    try:
        return _tt().read_at_snapshot(table, int(snapshot_id), limit)
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=400)
 
 
@app.get("/tt/read-timestamp/{table}")
def tt_read_timestamp(table: str, timestamp_ms: int, limit: int = 50):
    """Lee datos de la tabla en un momento concreto (ms Unix)."""
    try:
        return _tt().read_at_timestamp(table, timestamp_ms, limit)
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=400)
 
 
@app.post("/tt/rollback-snapshot/{table}")
async def tt_rollback_snapshot(table: str, req: Request):
    """Rollback al snapshot indicado."""
    try:
        body = await req.json()
        return _tt().rollback_to_snapshot(table, int(body["snapshot_id"]))
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=400)
 
 
@app.post("/tt/rollback-timestamp/{table}")
async def tt_rollback_timestamp(table: str, req: Request):
    """Rollback al snapshot mas cercano anterior al timestamp indicado."""
    try:
        body = await req.json()
        return _tt().rollback_to_timestamp(table, int(body["timestamp_ms"]))
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=400)
 
 
@app.get("/tt/changes/{table}")
def tt_changes(table: str, start_snapshot_id: str, end_snapshot_id: str, limit: int = 200):
    """Devuelve registros anadidos entre dos snapshots."""
    try:
        return _tt().get_incremental_changes(table, int(start_snapshot_id), int(end_snapshot_id), limit)
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=400)
 
 
@app.post("/tt/expire/{table}")
async def tt_expire(table: str, req: Request):
    """Expira snapshots anteriores a la fecha indicada."""
    try:
        body = await req.json()
        return _tt().expire_snapshots(
            table,
            int(body["older_than_ms"]),
            int(body.get("retain_last", 1)),
        )
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=400)
 
 
@app.post("/tt/orphans/{table}")
async def tt_orphans(table: str):
    """Elimina archivos huerfanos de la tabla."""
    try:
        return _tt().remove_orphan_files(table)
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=400)
 
 
@app.get("/tt/stats/{table}")
def tt_stats(table: str):
    """Estadisticas del historial de snapshots."""
    try:
        return _tt().snapshot_stats(table)
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=400)
    


@app.get("/", response_class=HTMLResponse)
async def root():
    with open("ui.html") as f:
        return f.read()
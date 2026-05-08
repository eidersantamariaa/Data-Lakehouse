from datetime import datetime, timezone

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, HTMLResponse
from pyiceberg.catalog import load_catalog
from pyiceberg.io.fsspec import FsspecFileIO
import pyarrow as pa
import boto3
from timetraveling import TimeTraveler
import math


app = FastAPI()
catalog = None
s3_config = {}
tt: TimeTraveler | None = None

@app.post("/connect")
async def connect_catalog(req: Request):
    global catalog, s3_config, tt
    data = await req.json()
    try:
        warehouse_prefix = data.get("wp", "").strip().strip("/")
        warehouse = f"s3://{data['bk'].strip()}"
        if warehouse_prefix:
            warehouse = f"{warehouse}/{warehouse_prefix}"

        catalog = load_catalog(
            "garage",
            **{
                "type": "rest",
                "uri": data["ep"],
                "warehouse": warehouse,
                "s3.endpoint": data["ep_s3"],
                "s3.access-key-id": data["ak"],
                "s3.secret-access-key": data["sk"],
                "s3.region": "us-east-1",
                "s3.path-style-access": "true",
                "py-io-impl": "pyiceberg.io.pyarrow.PyArrowFileIO",
            }
        )
        s3_config = data
        s3_config["warehouse"] = warehouse
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

# ── Preview ahora acepta limit como query param ─────────────────────────────
@app.get("/preview/{table}")
def preview(table: str, limit: int = 50):
    if catalog is None:
        return {"error": "Catalog not connected"}

    ns, tbl_name = table.split(".", 1)
    t = catalog.load_table((ns, tbl_name))
    snap = t.current_snapshot()
    summary = snap.summary if snap else {}

    rows = []
    try:
        table_scan = t.scan(limit=limit).to_arrow()
        raw_rows = table_scan.to_pylist()

        for row in raw_rows:
            formatted_row = {}
            for k, v in row.items():
                if isinstance(v, float) and (math.isnan(v) or math.isinf(v)):
                    formatted_row[k] = None
                elif v is not None and not isinstance(v, (str, int, float, bool)):
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
        t.refresh()
        current_id = t.current_snapshot().snapshot_id if t.current_snapshot() else None
        snaps = []
        for s in reversed(list(t.metadata.snapshots)):
            sm = s.summary or {}
            from datetime import datetime
            ts = datetime.fromtimestamp(s.timestamp_ms / 1000).strftime("%Y-%m-%d %H:%M")
            snaps.append({
                "id": str(s.snapshot_id),
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
    if catalog is None:
        return False
    try:
        audit_table = catalog.load_table((namespace, "audit_log"))
    except Exception:
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
        if hasattr(audit_table, "append"):
            audit_table.append(arrow_tbl)
        elif hasattr(audit_table, "add_files"):
            return False
        else:
            return False
        return True
    except Exception as e:
        print(f"⚠ audit_log write failed: {e}")
        return False


@app.post("/table/delete/{table}")
def delete_table(table: str):
    if catalog is None:
        return JSONResponse({"error": "Catalog not connected"}, status_code=400)
    try:
        ns, tbl_name = table.split(".", 1)
    except ValueError:
        return JSONResponse({"error": "Formato invalido, usa namespace.tabla"}, status_code=400)

    cmd = f"DROP TABLE players.{ns}.{tbl_name} PURGE"
    row_count = 0
    try:
        table_obj = catalog.load_table((ns, tbl_name))
        row_count = int(table_obj.scan().to_arrow().num_rows)
    except Exception as e:
        print(f"⚠ could not count rows before delete for {table}: {e}")
    print(f"🗑 executing server command: {cmd}")

    try:
        try:
            catalog.drop_table((ns, tbl_name), purge_requested=True)
        except TypeError:
            try:
                catalog.drop_table((ns, tbl_name), purge=True)
            except TypeError:
                catalog.drop_table((ns, tbl_name))

        audit_written = False
        if tbl_name != "audit_log":
            audit_written = _write_audit_log(
                namespace=ns,
                table_name=tbl_name,
                accion="DROP",
                num_registros=row_count,
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
    try:
        return _tt().read_at_snapshot(table, int(snapshot_id), limit)
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=400)

@app.get("/tt/read-timestamp/{table}")
def tt_read_timestamp(table: str, timestamp_ms: int, limit: int = 50):
    try:
        return _tt().read_at_timestamp(table, timestamp_ms, limit)
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=400)

@app.post("/tt/rollback-snapshot/{table}")
async def tt_rollback_snapshot(table: str, req: Request):
    try:
        body = await req.json()
        return _tt().rollback_to_snapshot(table, int(body["snapshot_id"]))
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=400)

@app.post("/tt/rollback-timestamp/{table}")
async def tt_rollback_timestamp(table: str, req: Request):
    try:
        body = await req.json()
        return _tt().rollback_to_timestamp(table, int(body["timestamp_ms"]))
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=400)

@app.get("/tt/changes/{table}")
def tt_changes(table: str, start_snapshot_id: str, end_snapshot_id: str, limit: int = 200):
    try:
        return _tt().get_incremental_changes(table, int(start_snapshot_id), int(end_snapshot_id), limit)
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=400)

@app.post("/ingesta")
async def ingesta(req: Request):
    if catalog is None:
        return JSONResponse({"error": "Catalog not connected"}, status_code=400)
    
    try:
        body = await req.json()
        apis = body.get("apis", [])
        namespace = body.get("namespace", "raw").strip()
        
        if not apis:
            return JSONResponse({"error": "No APIs selected"}, status_code=400)
        
        if not namespace:
            namespace = "raw"
        
        # Create namespace if not exists
        try:
            catalog.create_namespace(namespace)
        except Exception:
            pass  # Namespace might already exist
        
        results = {}
        
        # Process each API
        for api in apis:
            try:
                print(f"\n🚀 Starting ingestion for {api}...")
                
                if api == "fbref":
                    # fbref bronce módulo hace la ingesta directamente
                    print("📝 Importing fbref_bronce module (ejecuta ingesta directamente)...")
                    import fbref_bronce
                    print(f"✅ API fbref ingested successfully")
                    results[api] = {
                        "status": "ok",
                        "tables": ["teams", "players"],
                        "count": 2
                    }
                    
                elif api in ["transfermarkt", "thesportsdb"]:
                    # transfermarkt y thesportsdb usan run_ingesta
                    print(f"📝 Loading {api} with run_ingesta...")
                    
                    if api == "transfermarkt":
                        import transfermarkt_bronce as module
                    else:  # thesportsdb
                        import thesportsdb_bronce as module
                    
                    # Create config object for run_ingesta
                    class Config:
                        NAMESPACE = namespace
                        get_data = staticmethod(module.get_data)
                    
                    # Import and run ingesta
                    from ingesta import run_ingesta
                    ingest_result = run_ingesta(Config)
                    tables_created = [entry["table"] for entry in ingest_result.get("tables", [])]
                    
                    results[api] = {
                        "status": "ok",
                        "tables": tables_created,
                        "count": len(tables_created)
                    }
                    print(f"✅ API {api} ingested successfully ({len(tables_created)} tables)")
                    
                else:
                    results[api] = {"status": "error", "msg": f"Unknown API: {api}"}
                    
            except Exception as e:
                print(f"❌ Error ingesting {api}: {e}")
                import traceback
                traceback.print_exc()
                results[api] = {"status": "error", "msg": str(e)}
        
        return {"status": "ok", "results": results}
    
    except Exception as e:
        print(f"❌ Ingesta error: {e}")
        import traceback
        traceback.print_exc()
        return JSONResponse({"error": str(e)}, status_code=400)


@app.get("/", response_class=HTMLResponse)
async def root():
    with open("ui.html") as f:
        return f.read()

@app.get("/merge/detect/{table}")
def detect_merges(table: str, umbral: float = 0.5):
    if catalog is None:
        return JSONResponse({"error": "Catalog not connected"}, status_code=400)
    try:
        ns, tbl_name = table.split(".", 1)
        t = catalog.load_table((ns, tbl_name))
        df = t.scan().to_arrow().to_pandas()

        from plata import detectar_solapamientos_agrupados
        grupos = detectar_solapamientos_agrupados(df, umbral)

        return {
            "grupos": grupos,
            "columnas": df.columns.tolist()
        }
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=400)


@app.post("/merge/apply/{table}")
async def apply_merges(table: str, req: Request):
    if catalog is None:
        return JSONResponse({"error": "Catalog not connected"}, status_code=400)
    try:
        body = await req.json()
        ns, tbl_name = table.split(".", 1)
        t = catalog.load_table((ns, tbl_name))
        df = t.scan().to_arrow().to_pandas()

        from plata import limpiar_tabla

        config = [
            {
                "col_final":  e["col_final"],
                "fuentes":    e["fuentes"],
                "tiebreaker": e["tiebreaker"],
                "normalizar": lambda x: x,
            }
            for e in body["config"]
        ]

        tabla_limpia = limpiar_tabla(df, config=config, config_norm=[], usar_fuzzy=False)

        target_table = body.get("target_table", f"{ns}.{tbl_name}_merged")
        target_ns, target_tbl = target_table.split(".", 1)

        arrow_table = pa.Table.from_pandas(tabla_limpia)

        try:
            catalog.create_table(
                (target_ns, target_tbl),
                schema=arrow_table.schema,
            )
            print(f"✅ tabla creada: {target_table}")
        except Exception as create_err:
            print(f"⚠ tabla ya existe o error: {create_err}, sobrescribiendo...")

        target_t = catalog.load_table((target_ns, target_tbl))
        target_t.overwrite(arrow_table)

        audit_written = _write_audit_log(
            namespace=target_ns,
            table_name=target_tbl,
            accion="MERGE",
            num_registros=len(tabla_limpia),
        )

        return {
            "status": "ok",
            "table": target_table,
            "rows": len(tabla_limpia),
            "cols": tabla_limpia.columns.tolist(),
            "audit_logged": audit_written,
        }
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=400)
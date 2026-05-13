from datetime import datetime, timezone

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, HTMLResponse
from pyiceberg.catalog import load_catalog
from pyiceberg.io.fsspec import FsspecFileIO
import pyarrow as pa
import pandas as pd
import boto3
from timetraveling import TimeTraveler
import math

from IDMatching import generar_mapeo_df, unir_fuentes_df, clave_fecha_completa, clave_solo_anio
from plata import validar_tabla


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


@app.get("/merge/detect/{table}")
def merge_detect(table: str, umbral: float = 0.5):
    if catalog is None:
        return JSONResponse({"error": "Catalog not connected"}, status_code=400)
    try:
        import plata as _plata
        df = _load_table_df(table)
        grupos = _plata.detectar_solapamientos_agrupados(df, umbral_similitud=umbral)
        columnas = list(df.columns)
        return {"columnas": columnas, "grupos": grupos}
    except Exception as e:
        print(f"❌ merge detect error: {e}")
        import traceback
        traceback.print_exc()
        return JSONResponse({"error": str(e)}, status_code=400)

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


def _load_table_df(table: str):
    ns, tbl_name = table.split(".", 1)
    t = catalog.load_table((ns, tbl_name))
    return t.scan().to_arrow().to_pandas()


def _save_table_df(table_name: str, df):
    target_ns, target_tbl = table_name.split(".", 1)
    # Work on a copy and ensure columns that are entirely null get a concrete type
    df_copy = df.copy()
    for c in df_copy.columns:
        try:
            if df_copy[c].isna().all():
                df_copy[c] = pd.Series([None] * len(df_copy), dtype="string")
        except Exception:
            # In case isna() fails for exotic dtypes, coerce to string
            df_copy[c] = df_copy[c].astype("string")
    # Build Arrow table from pandas (preserve column order)
    arrow_table = pa.Table.from_pandas(df_copy, preserve_index=False)

    try:
        catalog.create_namespace(target_ns)
    except Exception:
        pass

    # Try to create table if it doesn't exist
    try:
        catalog.create_table(
            (target_ns, target_tbl),
            schema=arrow_table.schema,
        )
    except Exception as create_err:
        # table probably exists
        print(f"⚠ table create skipped for {table_name}: {create_err}")

    target_t = catalog.load_table((target_ns, target_tbl))

    # If the existing table has a different schema, merge by name (union_by_name)
    try:
        existing_schema = target_t.schema()
        existing_names = [f.name for f in existing_schema.fields]
        new_names = list(arrow_table.column_names)

        if set(existing_names) != set(new_names):
            # preserve existing order, append new columns from incoming df
            combined = list(dict.fromkeys(existing_names + new_names))
            # Build a pandas DataFrame aligned to the combined columns so pyarrow will fill missing with nulls
            aligned = pd.DataFrame()
            for col in combined:
                if col in df.columns:
                    aligned[col] = df[col]
                else:
                    aligned[col] = pd.NA
            # Convert any all-null columns to string to avoid pa.null() fields
            for c in aligned.columns:
                try:
                    if aligned[c].isna().all():
                        aligned[c] = pd.Series([None] * len(aligned), dtype="string")
                except Exception:
                    aligned[c] = aligned[c].astype("string")
            arrow_table = pa.Table.from_pandas(aligned, preserve_index=False)

    except Exception as merge_err:
        print(f"⚠ schema union attempted but failed for {table_name}: {merge_err}")

    # Try to update table schema by name first so Iceberg knows about new columns
    try:
        try:
            upd = target_t.update_schema()
            upd.union_by_name(arrow_table.schema)
            upd.commit()
            print(f"✓ schema updated by union_by_name for {table_name}")
        except Exception as upd_err:
            # If update failed due to format-version constraints, try recreate
            msg = str(upd_err).lower()
            print(f"⚠ schema union failed for {table_name}: {upd_err}")
            if 'format' in msg or 'format-version' in msg or 'requires' in msg:
                try:
                    print(f"⚠ attempting to recreate table {table_name} with new schema")
                    catalog.drop_table((target_ns, target_tbl))
                except Exception as drop_err:
                    print(f"⚠ drop table failed: {drop_err}")
                try:
                    catalog.create_table((target_ns, target_tbl), schema=arrow_table.schema)
                    target_t = catalog.load_table((target_ns, target_tbl))
                    print(f"✓ table recreated: {table_name}")
                except Exception as create_err:
                    print(f"✗ recreate failed for {table_name}: {create_err}")
    except Exception as final_err:
        print(f"⚠ schema update/recreate unexpected error for {table_name}: {final_err}")

    # Overwrite table with arrow_table (now schema-matched)
    target_t.overwrite(arrow_table)
    return {
        "table": table_name,
        "rows": len(df),
        "cols": df.columns.tolist(),
    }


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
        namespace = (body.get("namespace", "raw") or "raw").strip() or "raw"
        test_mode = bool(body.get("test_mode", False))

        if not apis:
            return JSONResponse({"error": "No APIs selected"}, status_code=400)

        try:
            catalog.create_namespace(namespace)
        except Exception:
            pass

        results = {}

        for api in apis:
            try:
                print(f"\n🚀 Starting ingestion for {api}...")
                print(f"   {'TEST MODE' if test_mode else 'FULL MODE'} - {'20%' if test_mode else '100%'} de datos")

                if api == "fbref":
                    print("📝 Importing fbref_bronce module (ejecuta ingesta directamente)...")
                    import fbref_bronce
                    fbref_bronce.get_data(namespace, test_mode=test_mode)
                    results[api] = {
                        "status": "ok",
                        "tables": ["teams", "players"],
                        "count": 2,
                    }
                    print("✅ API fbref ingested successfully")

                elif api in ["transfermarkt", "thesportsdb"]:
                    print(f"📝 Loading {api} with run_ingesta...")
                    if api == "transfermarkt":
                        import transfermarkt_bronce as module
                    else:
                        import thesportsdb_bronce as module

                    class Config:
                        NAMESPACE = namespace
                        API = api
                        get_data = staticmethod(module.get_data)

                    from ingesta import run_ingesta

                    ingest_result = run_ingesta(Config, test_mode=test_mode)
                    tables_created = [entry["table"] for entry in ingest_result.get("tables", [])]
                    results[api] = {
                        "status": "ok",
                        "tables": tables_created,
                        "count": len(tables_created),
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
        return HTMLResponse(f.read())


@app.post("/matching/run")
async def matching_run(req: Request):
    if catalog is None:
        return JSONResponse({"error": "Catalog not connected"}, status_code=400)

    try:
        body = await req.json()
        sources = body.get("sources", [])
        mapping_table = (body.get("mapping_table") or "").strip()
        unified_table = (body.get("unified_table") or "").strip()
        umbral = float(body.get("umbral", 85))

        if len(sources) < 2:
            return JSONResponse({"error": "Selecciona al menos dos fuentes"}, status_code=400)

        source_frames = []
        source_data = []
        for src in sources:
            table_name = (src.get("table") or "").strip()
            name_col = (src.get("name_col") or "").strip()
            date_col = (src.get("date_col") or "").strip()
            id_col = (src.get("id_col") or "").strip() or None
            prefix = (src.get("prefix") or "").strip()
            key_mode = (src.get("key_mode") or "full_date").strip()
            source_umbral = float(src.get("umbral", umbral))

            if not table_name or not name_col or not prefix:
                return JSONResponse({"error": "Cada fuente necesita tabla, columna de nombre y prefijo"}, status_code=400)

            if key_mode == "year":
                key_fn = clave_solo_anio
            else:
                key_fn = clave_fecha_completa

            df = _load_table_df(table_name)
            source_frames.append((df, name_col, date_col, id_col, prefix, key_fn, source_umbral))
            source_data.append({
                "table": table_name,
                "prefix": prefix,
                "rows": len(df),
                "name_col": name_col,
                "date_col": date_col,
                "id_col": id_col,
                "key_mode": key_mode,
                "umbral": source_umbral,
            })

        mapeo = generar_mapeo_df(*source_frames, umbral=umbral)
        mapping_saved = None
        if mapping_table:
            mapping_saved = _save_table_df(mapping_table, mapeo)

        source_join_frames = []
        for src, frame in zip(sources, source_frames):
            df = frame[0].copy()
            name_col = (src.get("name_col") or "").strip()
            date_col = (src.get("date_col") or "").strip()
            id_col = (src.get("id_col") or "").strip() or None
            prefix = (src.get("prefix") or "").strip()
            key_mode = (src.get("key_mode") or "full_date").strip()
            key_fn = clave_solo_anio if key_mode == "year" else clave_fecha_completa

            if not id_col:
                df = df.copy()
                df["_clave"] = df.apply(
                    lambda r, fn=key_fn, cn=name_col, cf=date_col: fn(r.get(cn, None), r.get(cf, None)),
                    axis=1,
                )
                id_col = "_clave"

            source_join_frames.append((df, id_col, prefix))

        tabla_final = unir_fuentes_df(mapeo, *source_join_frames)
        unified_saved = None
        if unified_table:
            unified_saved = _save_table_df(unified_table, tabla_final)

        def _clean_preview(df):
            cleaned = []
            for _, row in df.head(10).iterrows():
                cleaned_row = {}
                for k, v in row.items():
                    if v is None or (isinstance(v, float) and math.isnan(v)):
                        cleaned_row[k] = None
                    elif isinstance(v, (int, float, bool, str)):
                        # Convert numpy types to native Python types
                        try:
                            cleaned_row[k] = v.item() if hasattr(v, 'item') else v
                        except (ValueError, TypeError):
                            cleaned_row[k] = str(v)
                    else:
                        cleaned_row[k] = str(v)
                cleaned.append(cleaned_row)
            return cleaned

        return {
            "status": "ok",
            "mapping_rows": len(mapeo),
            "mapping_columns": mapeo.columns.tolist(),
            "unified_rows": len(tabla_final),
            "unified_columns": tabla_final.columns.tolist(),
            "sources": source_data,
            "mapping_saved": mapping_saved,
            "unified_saved": unified_saved,
            "mapping_preview": _clean_preview(mapeo),
            "unified_preview": _clean_preview(tabla_final),
        }
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=400)
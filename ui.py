from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, HTMLResponse
from pyiceberg.catalog import load_catalog
from pyiceberg.io.fsspec import FsspecFileIO
import pyarrow as pa
import boto3

app = FastAPI()
catalog = None
s3_config = {}

"""
endpoint=http://172.16.58.11:32688
access_key=GK5f421d5f440758f74b0e0312
secret_key=409baa63477885db12cd1db0a518748c5e83e971b5e8cf2129fe6c7498de125d
bucket=warehouse

"""

@app.post("/connect")
async def connect_catalog(req: Request):
    global catalog, s3_config
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
        return JSONResponse({"status": "ok"})
    except Exception as e:
        print("❌ error:", e)
        return JSONResponse({"status": "error", "msg": str(e)}, status_code=400)

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
        "rows": int(summary.get("total-records", 0)),
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
    ns, tbl_name = table.split(".", 1)
    t = catalog.load_table((ns, tbl_name))
    current_id = t.current_snapshot().snapshot_id if t.current_snapshot() else None
    snaps = []
    for s in reversed(list(t.metadata.snapshots)):
        sm = s.summary or {}
        from datetime import datetime
        ts = datetime.fromtimestamp(s.timestamp_ms / 1000).strftime("%Y-%m-%d %H:%M")
        snaps.append({
            "id": s.snapshot_id,
            "ts": ts,
            "op": sm.get("operation", "—"),
            "add": sm.get("added-data-files", "0"),
            "rem": sm.get("deleted-data-files", "0"),
            "rec": sm.get("total-records", "—"),
            "current": s.snapshot_id == current_id,
        })
    return {"snaps": snaps}

@app.get("/", response_class=HTMLResponse)
async def root():
    with open("ui.html") as f:
        return f.read()